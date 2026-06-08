package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

const (
	uploadChunkPollInterval  = time.Second
	uploadSessionPullTimeout = 6 * time.Hour
)

type UploadTransferSession struct {
	SessionID       string
	DestinationPath string
	PartialPath     string
	Filename        string
	TotalSize       int64
	ChunkSize       int64
	CommittedOffset int64
	CreatedAt       time.Time
	PullerStarted   bool
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

func startUploadChunkPuller(a *Agent, sessionID string) {
	a.FileTransferSessionsMu.Lock()
	session, ok := a.FileTransferSessions[sessionID]
	if !ok || session == nil || session.PullerStarted {
		a.FileTransferSessionsMu.Unlock()
		return
	}
	session.PullerStarted = true
	a.FileTransferSessionsMu.Unlock()

	go a.PullAndWriteUploadChunks(sessionID)
}

func (a *Agent) PullAndWriteUploadChunks(sessionID string) {
	url := fmt.Sprintf("/api/internal/file-transfers/%s/next-chunk/", sessionID)

	for {
		a.FileTransferSessionsMu.Lock()
		session, ok := a.FileTransferSessions[sessionID]
		if !ok || session == nil {
			a.FileTransferSessionsMu.Unlock()
			return
		}
		if session.CommittedOffset >= session.TotalSize {
			a.FileTransferSessionsMu.Unlock()
			return
		}
		if time.Since(session.CreatedAt) > uploadSessionPullTimeout {
			a.FileTransferSessionsMu.Unlock()
			a.Logger.Errorln("PullAndWriteUploadChunks session timed out:", sessionID)
			return
		}
		a.FileTransferSessionsMu.Unlock()

		resp, err := a.rClient.R().Get(url)
		if err != nil {
			a.Logger.Errorln("PullAndWriteUploadChunks GET:", err)
			time.Sleep(uploadChunkPollInterval)
			continue
		}

		switch resp.StatusCode() {
		case 404:
			time.Sleep(uploadChunkPollInterval)
			continue
		case 200:
			if err := a.applyUploadChunk(sessionID, resp); err != nil {
				a.Logger.Errorln("PullAndWriteUploadChunks write:", err)
				time.Sleep(uploadChunkPollInterval)
			}
		case 401, 403, 410:
			a.Logger.Errorln(
				"PullAndWriteUploadChunks auth/session error:",
				resp.StatusCode(),
				string(resp.Body()),
			)
			return
		default:
			a.Logger.Errorln(
				"PullAndWriteUploadChunks status:",
				resp.StatusCode(),
				string(resp.Body()),
			)
			time.Sleep(uploadChunkPollInterval)
		}
	}
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
	if start != session.CommittedOffset {
		a.FileTransferSessionsMu.Unlock()
		return fmt.Errorf("chunk start offset does not match committed_offset")
	}
	if end >= session.TotalSize {
		a.FileTransferSessionsMu.Unlock()
		return fmt.Errorf("chunk end exceeds total_size")
	}
	file := session.File
	a.FileTransferSessionsMu.Unlock()

	n, err := file.WriteAt(data, start)
	if err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}
	if int64(n) != expectedLen {
		return fmt.Errorf("short write for upload chunk")
	}

	committedOffset := end + 1
	if err := a.ackUploadChunk(sessionID, committedOffset); err != nil {
		return err
	}

	a.FileTransferSessionsMu.Lock()
	defer a.FileTransferSessionsMu.Unlock()

	session, ok = a.FileTransferSessions[sessionID]
	if !ok || session == nil {
		return fmt.Errorf("upload session not found after write")
	}
	if start != session.CommittedOffset {
		return fmt.Errorf("chunk start offset changed during write")
	}

	session.CommittedOffset = committedOffset
	a.Logger.Debugf(
		"PullAndWriteUploadChunks wrote session=%s bytes=%d offset=%d",
		sessionID,
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
