package agent

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

const downloadPutTimeout = 90 * time.Second

type UploadTransferSession struct {
	SessionID       string
	DestinationPath string
	PartialPath     string
	Filename        string
	TotalSize       int64
	ChunkSize       int64
	CommittedOffset int64
	File            *os.File
}

func parsePayloadString(data map[string]string, key string) (string, error) {
	val, ok := data[key]
	if !ok {
		return "", fmt.Errorf("missing %s", key)
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return "", fmt.Errorf("missing %s", key)
	}
	return val, nil
}

func parsePayloadInt64(data map[string]string, key string) (int64, error) {
	val, ok := data[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, fmt.Errorf("missing %s", key)
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return n, nil
}

func validateUploadFilename(filename string) error {
	if filename == "." || filename == ".." {
		return fmt.Errorf("invalid filename")
	}
	if len(filename) > 255 {
		return fmt.Errorf("filename is too long")
	}
	if strings.ContainsAny(filename, "<>:\"/\\|?*\x00") {
		return fmt.Errorf("invalid filename")
	}
	return nil
}

func pathHasTraversalComponent(path string) bool {
	vol := filepath.VolumeName(path)
	rest := strings.TrimPrefix(path, vol)
	rest = strings.TrimPrefix(rest, string(filepath.Separator))
	rest = strings.TrimPrefix(rest, "/")

	if rest == "" {
		return false
	}

	for _, part := range strings.FieldsFunc(rest, func(r rune) bool {
		return r == filepath.Separator || r == '/'
	}) {
		if part == ".." {
			return true
		}
	}
	return false
}

func validateUploadDestinationPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("missing destination_path")
	}

	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("destination_path must be an absolute path")
	}

	if pathHasTraversalComponent(path) {
		return "", fmt.Errorf("invalid destination_path")
	}

	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("destination_path must be an absolute path")
	}

	if pathHasTraversalComponent(cleaned) {
		return "", fmt.Errorf("invalid destination_path")
	}

	return cleaned, nil
}

func (a *Agent) HandleUploadChunkAvailable(p *NatsMsg) (map[string]interface{}, error) {
	sessionID, err := parsePayloadString(p.Data, "session_id")
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("/api/internal/file-transfers/%s/next-chunk/", sessionID)

	fetchStart := time.Now()
	resp, err := a.rClient.R().
		SetDebug(false). // binary response body — suppress resty debug logging
		Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to pull upload chunk: %w", err)
	}

	if resp.StatusCode() == 404 {
		time.Sleep(100 * time.Millisecond)
		resp, err = a.rClient.R().
			SetDebug(false).
			Get(url)
		if err != nil {
			return nil, fmt.Errorf("failed to pull upload chunk: %w", err)
		}
		if resp.StatusCode() == 404 {
			a.Logger.Debugf(
				"file_transfer chunk session=%s: no chunk available (handled concurrently)",
				sessionID,
			)
			return map[string]interface{}{"status": "no_chunk"}, nil
		}
	}
	fetchMs := time.Since(fetchStart).Milliseconds()

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf(
			"pull upload chunk failed: %d %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
	}

	if err := a.applyUploadChunk(sessionID, resp); err != nil {
		return nil, err
	}

	a.FileTransferSessionsMu.Lock()
	session, ok := a.FileTransferSessions[sessionID]
	committedOffset := int64(0)
	totalSize := int64(0)
	if ok && session != nil {
		committedOffset = session.CommittedOffset
		totalSize = session.TotalSize
	}
	a.FileTransferSessionsMu.Unlock()

	a.Logger.Infof(
		"file_transfer chunk session=%s chunk_fetch_ms=%d committed_offset=%d",
		sessionID,
		fetchMs,
		committedOffset,
	)

	if committedOffset < totalSize {
		prefetchResp, prefetchErr := a.rClient.R().SetDebug(false).Get(url)
		if prefetchErr == nil && prefetchResp.StatusCode() == 200 {
			if err := a.applyUploadChunk(sessionID, prefetchResp); err != nil {
				a.Logger.Warnf(
					"file_transfer prefetch chunk session=%s err=%v",
					sessionID, err,
				)
			} else {
				a.FileTransferSessionsMu.Lock()
				if s, ok2 := a.FileTransferSessions[sessionID]; ok2 && s != nil {
					committedOffset = s.CommittedOffset
				}
				a.FileTransferSessionsMu.Unlock()
				a.Logger.Infof(
					"file_transfer prefetch chunk session=%s committed_offset=%d",
					sessionID, committedOffset,
				)
			}
		}
	}

	return map[string]interface{}{
		"status":           "acked",
		"committed_offset": committedOffset,
	}, nil
}

func (a *Agent) applyUploadChunk(sessionID string, resp *resty.Response) error {
	start, end, err := parseUploadChunkHeaders(resp)
	if err != nil {
		return err
	}

	data := resp.Body()
	expectedLen := end - start + 1
	if int64(len(data)) != expectedLen {
		return fmt.Errorf("chunk body size does not match Content-Range")
	}

	a.FileTransferSessionsMu.Lock()
	session, ok := a.FileTransferSessions[sessionID]
	if !ok || session == nil {
		a.FileTransferSessionsMu.Unlock()
		return fmt.Errorf("upload session not found")
	}
	if session.File == nil {
		a.FileTransferSessionsMu.Unlock()
		return fmt.Errorf("upload session file handle is missing")
	}
	if end >= session.TotalSize {
		a.FileTransferSessionsMu.Unlock()
		return fmt.Errorf("chunk end exceeds total_size")
	}

	committedOffset := end + 1
	if committedOffset <= session.CommittedOffset {
		a.FileTransferSessionsMu.Unlock()
		ackStart := time.Now()
		if err := a.ackUploadChunk(sessionID, committedOffset); err != nil {
			return err
		}
		a.Logger.Infof(
			"file_transfer chunk session=%s chunk_reack_ms=%d offset=%d",
			sessionID,
			time.Since(ackStart).Milliseconds(),
			committedOffset,
		)
		return nil
	}

	if start != session.CommittedOffset {
		a.FileTransferSessionsMu.Unlock()
		return fmt.Errorf("chunk start offset does not match committed_offset")
	}
	file := session.File
	a.FileTransferSessionsMu.Unlock()

	writeStart := time.Now()
	n, err := file.WriteAt(data, start)
	if err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}
	if int64(n) != expectedLen {
		return fmt.Errorf("short write for upload chunk")
	}
	writeMs := time.Since(writeStart).Milliseconds()

	a.FileTransferSessionsMu.Lock()
	session, ok = a.FileTransferSessions[sessionID]
	if !ok || session == nil {
		a.FileTransferSessionsMu.Unlock()
		return fmt.Errorf("upload session not found after write")
	}
	if start != session.CommittedOffset {
		a.FileTransferSessionsMu.Unlock()
		return fmt.Errorf("chunk start offset changed during write")
	}
	session.CommittedOffset = committedOffset
	a.FileTransferSessionsMu.Unlock()

	ackStart := time.Now()
	if err := a.ackUploadChunk(sessionID, committedOffset); err != nil {
		return err
	}
	ackMs := time.Since(ackStart).Milliseconds()

	a.Logger.Infof(
		"file_transfer chunk session=%s chunk_write_ms=%d chunk_ack_ms=%d bytes=%d offset=%d",
		sessionID,
		writeMs,
		ackMs,
		expectedLen,
		committedOffset,
	)
	return nil
}

func (a *Agent) ackUploadChunk(sessionID string, committedOffset int64) error {
	url := fmt.Sprintf("/api/internal/file-transfers/%s/ack/", sessionID)
	payload := map[string]int64{"committed_offset": committedOffset}

	resp, err := a.rClient.R().SetBody(payload).Post(url)
	if err != nil {
		return fmt.Errorf("failed to ack upload chunk: %w", err)
	}

	switch resp.StatusCode() {
	case 200:
		return nil
	case 401, 403, 410:
		return fmt.Errorf(
			"upload chunk ack auth/session error: %d %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
	default:
		return fmt.Errorf(
			"upload chunk ack failed: %d %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
	}
}

func parseUploadChunkHeaders(resp *resty.Response) (int64, int64, error) {
	startStr := strings.TrimSpace(resp.Header().Get("X-Chunk-Start"))
	endStr := strings.TrimSpace(resp.Header().Get("X-Chunk-End"))
	if startStr == "" || endStr == "" {
		return 0, 0, fmt.Errorf("missing chunk range headers")
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid X-Chunk-Start")
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid X-Chunk-End")
	}
	if start < 0 || end < start {
		return 0, 0, fmt.Errorf("invalid chunk byte range")
	}

	return start, end, nil
}

func (a *Agent) FinalizeFilesUpload(p *NatsMsg) (map[string]interface{}, error) {
	sessionID, err := parsePayloadString(p.Data, "session_id")
	if err != nil {
		return nil, err
	}

	rawDestinationPath, err := parsePayloadString(p.Data, "destination_path")
	if err != nil {
		return nil, err
	}

	destinationPath, err := validateUploadDestinationPath(rawDestinationPath)
	if err != nil {
		return nil, err
	}

	totalSize, err := parsePayloadInt64(p.Data, "total_size")
	if err != nil {
		return nil, err
	}
	if totalSize <= 0 {
		return nil, fmt.Errorf("total_size must be greater than 0")
	}

	a.FileTransferSessionsMu.Lock()
	session, ok := a.FileTransferSessions[sessionID]
	if !ok || session == nil {
		a.FileTransferSessionsMu.Unlock()
		return nil, fmt.Errorf("upload session not found")
	}
	if destinationPath != session.DestinationPath {
		a.FileTransferSessionsMu.Unlock()
		return nil, fmt.Errorf("destination_path does not match session")
	}

	partialPath := session.PartialPath
	if partialPath == "" {
		partialPath = destinationPath + ".partial"
	}
	file := session.File
	a.FileTransferSessionsMu.Unlock()

	if file != nil {
		if err := file.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync partial file: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("failed to close partial file: %w", err)
		}

		a.FileTransferSessionsMu.Lock()
		if session, ok := a.FileTransferSessions[sessionID]; ok && session != nil {
			session.File = nil
		}
		a.FileTransferSessionsMu.Unlock()
	}

	info, err := os.Stat(partialPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat partial file: %w", err)
	}
	if info.Size() != totalSize {
		return nil, fmt.Errorf("partial file size does not match total_size")
	}

	if _, err := os.Stat(destinationPath); err == nil {
		return nil, fmt.Errorf("destination file already exists")
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to check destination file: %w", err)
	}

	if err := os.Rename(partialPath, destinationPath); err != nil {
		return nil, fmt.Errorf("failed to rename partial file: %w", err)
	}

	a.FileTransferSessionsMu.Lock()
	delete(a.FileTransferSessions, sessionID)
	a.FileTransferSessionsMu.Unlock()

	return map[string]interface{}{
		"status":           "completed",
		"destination_path": destinationPath,
	}, nil
}

type DownloadTransferSession struct {
	SessionID  string
	SourcePath string
	TotalSize  int64
	ChunkSize  int64
	File       *os.File
	StopStream chan struct{}
}

func (a *Agent) PrepareFilesDownload(p *NatsMsg) (map[string]interface{}, error) {
	sessionID, err := parsePayloadString(p.Data, "session_id")
	if err != nil {
		return nil, err
	}

	rawSourcePath, err := parsePayloadString(p.Data, "source_path")
	if err != nil {
		return nil, err
	}

	sourcePath, err := validateUploadDestinationPath(rawSourcePath)
	if err != nil {
		return nil, err
	}

	chunkSize, err := parsePayloadInt64(p.Data, "chunk_size")
	if err != nil {
		return nil, err
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk_size must be greater than 0")
	}

	info, err := os.Stat(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("source file not found: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("source path is a directory")
	}
	totalSize := info.Size()
	if totalSize == 0 {
		return nil, fmt.Errorf("source file is empty")
	}

	file, err := os.Open(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file: %w", err)
	}

	stopStream := make(chan struct{})

	a.DownloadTransferSessionsMu.Lock()
	if existing, ok := a.DownloadTransferSessions[sessionID]; ok {
		if existing.StopStream != nil {
			close(existing.StopStream)
		}
		if existing.File != nil {
			_ = existing.File.Close()
		}
	}
	a.DownloadTransferSessions[sessionID] = &DownloadTransferSession{
		SessionID:  sessionID,
		SourcePath: sourcePath,
		TotalSize:  totalSize,
		ChunkSize:  chunkSize,
		File:       file,
		StopStream: stopStream,
	}
	a.DownloadTransferSessionsMu.Unlock()

	go a.streamDownloadChunks(sessionID, stopStream)

	return map[string]interface{}{
		"status":     "ready",
		"total_size": totalSize,
		"chunk_size": chunkSize,
	}, nil
}

func (a *Agent) streamDownloadChunks(sessionID string, stop <-chan struct{}) {
	offset := int64(0)
	for {
		select {
		case <-stop:
			a.Logger.Debugf("file_transfer download stream session=%s stopped", sessionID)
			return
		default:
		}

		a.DownloadTransferSessionsMu.Lock()
		session, ok := a.DownloadTransferSessions[sessionID]
		if !ok || session == nil {
			a.DownloadTransferSessionsMu.Unlock()
			return
		}
		totalSize := session.TotalSize
		a.DownloadTransferSessionsMu.Unlock()

		if offset >= totalSize {
			a.Logger.Infof(
				"file_transfer download stream session=%s complete offset=%d",
				sessionID, offset,
			)
			return
		}

		offeredOffset, err := a.pushDownloadChunk(sessionID, offset)
		if err != nil {
			a.Logger.Errorf(
				"file_transfer download stream session=%s offset=%d err=%v",
				sessionID, offset, err,
			)
			return
		}
		offset = offeredOffset
	}
}

func (a *Agent) pushDownloadChunk(sessionID string, offset int64) (int64, error) {
	a.DownloadTransferSessionsMu.Lock()
	session, ok := a.DownloadTransferSessions[sessionID]
	if !ok || session == nil {
		a.DownloadTransferSessionsMu.Unlock()
		return 0, fmt.Errorf("download session not found")
	}
	if session.File == nil {
		a.DownloadTransferSessionsMu.Unlock()
		return 0, fmt.Errorf("download session file handle is missing")
	}
	totalSize := session.TotalSize
	chunkSize := session.ChunkSize
	file := session.File
	a.DownloadTransferSessionsMu.Unlock()

	if offset >= totalSize {
		return offset, nil
	}

	remaining := totalSize - offset
	readSize := chunkSize
	if remaining < readSize {
		readSize = remaining
	}

	buf := make([]byte, readSize)
	readStart := time.Now()
	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read chunk at offset %d: %w", offset, err)
	}
	if int64(n) != readSize {
		return 0, fmt.Errorf("short read at offset %d: got %d want %d", offset, n, readSize)
	}
	readMs := time.Since(readStart).Milliseconds()

	end := offset + int64(n) - 1
	contentRange := fmt.Sprintf("bytes %d-%d/%d", offset, end, totalSize)

	url := fmt.Sprintf("/api/internal/file-transfers/%s/download-chunk/", sessionID)
	ctx, cancel := context.WithTimeout(context.Background(), downloadPutTimeout)
	defer cancel()

	putStart := time.Now()
	resp, err := a.rClient.R().
		SetDebug(false).
		SetContext(ctx).
		SetHeader("Content-Range", contentRange).
		SetBody(buf[:n]).
		Put(url)
	if err != nil {
		return 0, fmt.Errorf("failed to push download chunk: %w", err)
	}
	putMs := time.Since(putStart).Milliseconds()

	if resp.StatusCode() != 200 {
		return 0, fmt.Errorf(
			"push download chunk failed: %d %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
	}

	offeredOffset := end + 1
	a.Logger.Infof(
		"file_transfer download chunk pushed session=%s offset=%d end=%d read_ms=%d put_ms=%d",
		sessionID, offset, end, readMs, putMs,
	)
	return offeredOffset, nil
}

func (a *Agent) FinalizeFilesDownload(p *NatsMsg) (map[string]interface{}, error) {
	sessionID, err := parsePayloadString(p.Data, "session_id")
	if err != nil {
		return nil, err
	}

	a.DownloadTransferSessionsMu.Lock()
	session, ok := a.DownloadTransferSessions[sessionID]
	if !ok || session == nil {
		a.DownloadTransferSessionsMu.Unlock()
		return map[string]interface{}{"status": "completed"}, nil
	}
	stopStream := session.StopStream
	file := session.File
	delete(a.DownloadTransferSessions, sessionID)
	a.DownloadTransferSessionsMu.Unlock()

	if stopStream != nil {
		close(stopStream)
	}
	if file != nil {
		_ = file.Close()
	}

	return map[string]interface{}{"status": "completed"}, nil
}
