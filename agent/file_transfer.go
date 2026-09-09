package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

const (
	downloadPutTimeout             = 120 * time.Second
	downloadReadyGetTimeout        = 15 * time.Second
	uploadChunkGetTimeout          = 120 * time.Second
	uploadChunkAckTimeout          = 60 * time.Second
	fileTransferFailTimeout        = 30 * time.Second
	downloadPushMaxAttempts        = 12
	downloadPushRetryMinBackoff    = 500 * time.Millisecond
	downloadPushRetryMaxBackoff    = 8 * time.Second
	downloadPushAckFallbackPoll    = 5 * time.Second
	downloadPushAckWaitMax         = 120 * time.Second
	downloadPushAckWaitLogEvery    = 5 * time.Second
	fileTransferSessionIdleTimeout = 10 * time.Minute
	fileTransferReaperInterval     = 2 * time.Minute
	fileTransferPartialRetention   = 1 * time.Hour
	fileTransferDrainMinBackoff    = 50 * time.Millisecond
	fileTransferDrainMaxBackoff    = 500 * time.Millisecond
	fileTransferDrainIdleTimeout   = 15 * time.Second
)

var downloadChunkExpectedOffsetRe = regexp.MustCompile(
	`does not match expected (\d+)`,
)

func (a *Agent) fileTransferHTTP() *resty.Client {
	if a.fileTransferClient != nil {
		return a.fileTransferClient
	}
	return a.rClient
}

type UploadTransferSession struct {
	SessionID       string
	DestinationPath string
	PartialPath     string
	Filename        string
	TotalSize       int64
	ChunkSize       int64
	CommittedOffset int64
	ConflictPolicy  string
	File            *os.File
	LastActivity    time.Time
	Hasher          hash.Hash
	HashedOffset    int64
	DormantSince    time.Time
	Draining        bool
}

const (
	uploadConflictPolicyReplace = "replace"
	uploadConflictPolicySkip    = "skip"
	uploadConflictPolicySkipMsg = "destination already exists (conflict_policy=skip)"
)

func parseUploadConflictPolicy(data map[string]string) string {
	val := strings.ToLower(strings.TrimSpace(data["conflict_policy"]))
	if val == uploadConflictPolicySkip {
		return uploadConflictPolicySkip
	}
	return uploadConflictPolicyReplace
}

func uploadDestinationExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func enforceUploadConflictPolicySkip(destinationPath, conflictPolicy string) error {
	if conflictPolicy != uploadConflictPolicySkip {
		return nil
	}
	exists, err := uploadDestinationExists(destinationPath)
	if err != nil {
		return fmt.Errorf("failed to check destination file: %w", err)
	}
	if exists {
		return fmt.Errorf("%s", uploadConflictPolicySkipMsg)
	}
	return nil
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

func parsePayloadResume(data map[string]string) (bool, int64) {
	resume := strings.EqualFold(strings.TrimSpace(data["resume"]), "true")
	offset, err := strconv.ParseInt(strings.TrimSpace(data["committed_offset"]), 10, 64)
	if err != nil || offset < 0 {
		offset = 0
	}
	return resume, offset
}

func prepareUploadPartialFile(
	partialPath string, resume bool, resumeOffset, totalSize int64,
) (*os.File, int64, error) {
	if !resume {
		file, err := os.OpenFile(partialPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to create partial file: %w", err)
		}
		return file, 0, nil
	}

	if resumeOffset < 0 || resumeOffset > totalSize {
		return nil, 0, fmt.Errorf("invalid resume offset")
	}
	info, err := os.Stat(partialPath)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot resume upload, partial file missing: %w", err)
	}
	if info.Size() < resumeOffset {
		return nil, 0, fmt.Errorf("partial file is shorter than resume offset")
	}

	file, err := os.OpenFile(partialPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open partial file: %w", err)
	}
	if err := file.Truncate(resumeOffset); err != nil {
		_ = file.Close()
		return nil, 0, fmt.Errorf("failed to truncate partial file: %w", err)
	}
	return file, resumeOffset, nil
}

func replaceUploadPartialWithDestination(partialPath, destinationPath string) error {
	if _, err := os.Stat(destinationPath); err == nil {
		if err := os.Remove(destinationPath); err != nil {
			return fmt.Errorf("failed to remove existing destination file: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to check destination file: %w", err)
	}

	if err := os.Rename(partialPath, destinationPath); err != nil {
		return fmt.Errorf("failed to rename partial file: %w", err)
	}
	return nil
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

	a.FileTransferSessionsMu.Lock()
	session, ok := a.FileTransferSessions[sessionID]
	if !ok || session == nil {
		a.FileTransferSessionsMu.Unlock()
		return nil, fmt.Errorf("upload session not found")
	}
	if session.Draining {
		a.FileTransferSessionsMu.Unlock()
		return map[string]interface{}{"status": "draining"}, nil
	}
	session.Draining = true
	a.FileTransferSessionsMu.Unlock()

	defer func() {
		a.FileTransferSessionsMu.Lock()
		if s, ok := a.FileTransferSessions[sessionID]; ok && s != nil {
			s.Draining = false
		}
		a.FileTransferSessionsMu.Unlock()
	}()

	committedOffset, err := a.drainUploadChunks(sessionID)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"status":           "acked",
		"committed_offset": committedOffset,
	}, nil
}

func (a *Agent) drainUploadChunks(sessionID string) (int64, error) {
	url := fmt.Sprintf("/api/v3/file-transfers/%s/next-chunk/", sessionID)

	readState := func() (int64, int64, bool) {
		a.FileTransferSessionsMu.Lock()
		defer a.FileTransferSessionsMu.Unlock()
		s, ok := a.FileTransferSessions[sessionID]
		if !ok || s == nil {
			return 0, 0, false
		}
		return s.CommittedOffset, s.TotalSize, true
	}

	committedOffset, totalSize, ok := readState()
	if !ok {
		return 0, fmt.Errorf("upload session not found")
	}

	backoff := fileTransferDrainMinBackoff
	idleDeadline := time.Now().Add(fileTransferDrainIdleTimeout)

	for committedOffset < totalSize {
		fetchStart := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), uploadChunkGetTimeout)
		resp, err := a.fileTransferHTTP().R().
			SetDebug(false).
			SetContext(ctx).
			Get(url)
		cancel()
		if err != nil {
			return committedOffset, fmt.Errorf("failed to pull upload chunk: %w", err)
		}

		switch resp.StatusCode() {
		case 200:
			if err := a.applyUploadChunk(sessionID, resp); err != nil {
				return committedOffset, err
			}
			committedOffset, totalSize, ok = readState()
			if !ok {
				return committedOffset, fmt.Errorf("upload session not found")
			}
			a.Logger.Infof(
				"file_transfer chunk session=%s chunk_fetch_ms=%d committed_offset=%d",
				sessionID,
				time.Since(fetchStart).Milliseconds(),
				committedOffset,
			)
			backoff = fileTransferDrainMinBackoff
			idleDeadline = time.Now().Add(fileTransferDrainIdleTimeout)
		case 404:
			if time.Now().After(idleDeadline) {
				a.Logger.Debugf(
					"file_transfer chunk session=%s: drain idle committed_offset=%d/%d",
					sessionID, committedOffset, totalSize,
				)
				return committedOffset, nil
			}
			time.Sleep(backoff)
			backoff *= 2
			if backoff > fileTransferDrainMaxBackoff {
				backoff = fileTransferDrainMaxBackoff
			}
		default:
			return committedOffset, fmt.Errorf(
				"pull upload chunk failed: %d %s",
				resp.StatusCode(),
				string(resp.Body()),
			)
		}
	}

	return committedOffset, nil
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
	session.LastActivity = time.Now()
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
	if session.Hasher != nil && start == session.HashedOffset {
		session.Hasher.Write(data)
		session.HashedOffset = committedOffset
	}
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
	url := fmt.Sprintf("/api/v3/file-transfers/%s/ack/", sessionID)
	payload := map[string]int64{"committed_offset": committedOffset}

	ctx, cancel := context.WithTimeout(context.Background(), uploadChunkAckTimeout)
	defer cancel()
	resp, err := a.fileTransferHTTP().R().
		SetContext(ctx).
		SetBody(payload).
		Post(url)
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
	hasher := session.Hasher
	hashedOffset := session.HashedOffset
	conflictPolicy := session.ConflictPolicy
	if conflictPolicy == "" {
		conflictPolicy = parseUploadConflictPolicy(p.Data)
	}
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

	expectedSHA := strings.ToLower(strings.TrimSpace(p.Data["sha256"]))
	computedSHA := ""
	if hasher != nil && hashedOffset == totalSize {
		computedSHA = hex.EncodeToString(hasher.Sum(nil))
	} else if expectedSHA != "" {
		computedSHA, err = hashFileSHA256(partialPath)
		if err != nil {
			return nil, fmt.Errorf("failed to hash partial file: %w", err)
		}
	}
	if expectedSHA != "" && computedSHA != "" && expectedSHA != computedSHA {
		_ = os.Remove(partialPath)
		a.FileTransferSessionsMu.Lock()
		delete(a.FileTransferSessions, sessionID)
		a.FileTransferSessionsMu.Unlock()
		return nil, fmt.Errorf(
			"integrity check failed: expected sha256 %s but received %s",
			expectedSHA, computedSHA,
		)
	}

	if err := enforceUploadConflictPolicySkip(destinationPath, conflictPolicy); err != nil {
		_ = os.Remove(partialPath)
		a.FileTransferSessionsMu.Lock()
		delete(a.FileTransferSessions, sessionID)
		a.FileTransferSessionsMu.Unlock()
		return nil, err
	}

	if err := replaceUploadPartialWithDestination(partialPath, destinationPath); err != nil {
		return nil, err
	}

	a.FileTransferSessionsMu.Lock()
	delete(a.FileTransferSessions, sessionID)
	a.FileTransferSessionsMu.Unlock()

	return map[string]interface{}{
		"status":           "completed",
		"destination_path": destinationPath,
		"sha256":           computedSHA,
	}, nil
}

func (a *Agent) AbortFilesUpload(p *NatsMsg) (map[string]interface{}, error) {
	sessionID, err := parsePayloadString(p.Data, "session_id")
	if err != nil {
		return nil, err
	}

	a.FileTransferSessionsMu.Lock()
	session, ok := a.FileTransferSessions[sessionID]
	if !ok || session == nil {
		a.FileTransferSessionsMu.Unlock()
		return map[string]interface{}{"status": "aborted"}, nil
	}
	file := session.File
	partialPath := session.PartialPath
	session.File = nil
	delete(a.FileTransferSessions, sessionID)
	a.FileTransferSessionsMu.Unlock()

	if file != nil {
		_ = file.Close()
	}
	if partialPath != "" {
		if err := os.Remove(partialPath); err != nil && !os.IsNotExist(err) {
			a.Logger.Warnf(
				"file_transfer upload abort session=%s: failed to remove partial %s: %v",
				sessionID, partialPath, err,
			)
		}
	}

	a.Logger.Infof("file_transfer upload aborted session=%s", sessionID)
	return map[string]interface{}{"status": "aborted"}, nil
}

type DownloadTransferSession struct {
	SessionID     string
	SourcePath    string
	TotalSize     int64
	ChunkSize     int64
	File          *os.File
	StopStream    chan struct{}
	AckCh         chan struct{}
	LastActivity  time.Time
	Hasher        hash.Hash
	HashedOffset  int64
	RemoveOnClose bool
}

func (a *Agent) HandleDownloadAck(p *NatsMsg) {
	sessionID, err := parsePayloadString(p.Data, "session_id")
	if err != nil {
		return
	}

	a.DownloadTransferSessionsMu.Lock()
	session, ok := a.DownloadTransferSessions[sessionID]
	var ackCh chan struct{}
	if ok && session != nil {
		ackCh = session.AckCh
		session.LastActivity = time.Now()
	}
	a.DownloadTransferSessionsMu.Unlock()
	if ackCh == nil {
		return
	}

	select {
	case ackCh <- struct{}{}:
	default:
	}
}

func (a *Agent) waitForDownloadAckSlot(sessionID string, stop <-chan struct{}) bool {
	var ackCh chan struct{}
	a.DownloadTransferSessionsMu.Lock()
	if s, ok := a.DownloadTransferSessions[sessionID]; ok && s != nil {
		ackCh = s.AckCh
	}
	a.DownloadTransferSessionsMu.Unlock()

	timer := time.NewTimer(downloadPushAckFallbackPoll)
	defer timer.Stop()

	if ackCh == nil {
		select {
		case <-stop:
			return false
		case <-timer.C:
			return true
		}
	}

	select {
	case <-stop:
		return false
	case <-ackCh:
		return true
	case <-timer.C:
		return true
	}
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

	resume, resumeOffset := parsePayloadResume(p.Data)
	startOffset := int64(0)
	if resume {
		if resumeOffset < 0 || resumeOffset > totalSize {
			return nil, fmt.Errorf("invalid resume offset")
		}
		startOffset = resumeOffset
	}

	removeOnClose := strings.EqualFold(strings.TrimSpace(p.Data["remove_on_close"]), "true") &&
		isArchiveTempPath(sourcePath)

	file, err := os.Open(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file: %w", err)
	}

	var hasher hash.Hash
	if startOffset == 0 {
		hasher = sha256.New()
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
		SessionID:     sessionID,
		SourcePath:    sourcePath,
		TotalSize:     totalSize,
		ChunkSize:     chunkSize,
		File:          file,
		StopStream:    stopStream,
		AckCh:         make(chan struct{}, 1),
		LastActivity:  time.Now(),
		Hasher:        hasher,
		HashedOffset:  startOffset,
		RemoveOnClose: removeOnClose,
	}
	a.DownloadTransferSessionsMu.Unlock()

	go a.streamDownloadChunks(sessionID, stopStream, startOffset)

	return map[string]interface{}{
		"status":     "ready",
		"total_size": totalSize,
		"chunk_size": chunkSize,
	}, nil
}

func (a *Agent) streamDownloadChunks(sessionID string, stop <-chan struct{}, startOffset int64) {
	offset := startOffset
	backoff := downloadPushRetryMinBackoff
	attempt := 0
	var ackWaitStarted time.Time
	var lastAckWaitLog time.Time

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

		ready, err := a.getDownloadChunkReady(sessionID)
		if err != nil {
			if !isRetryableDownloadPushError(err) {
				a.Logger.Errorf(
					"file_transfer download stream session=%s offset=%d fatal ready err=%v",
					sessionID, offset, err,
				)
				a.failDownloadStream(sessionID, err)
				return
			}
			ackWaitStarted = time.Time{}
			attempt++
			if attempt >= downloadPushMaxAttempts {
				a.Logger.Errorf(
					"file_transfer download stream session=%s offset=%d exhausted ready retries err=%v",
					sessionID, offset, err,
				)
				a.failDownloadStream(sessionID, err)
				return
			}
			a.Logger.Warnf(
				"file_transfer download stream session=%s offset=%d ready retry %d/%d backoff=%s err=%v",
				sessionID, offset, attempt, downloadPushMaxAttempts, backoff, err,
			)
			timer := time.NewTimer(backoff)
			select {
			case <-stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff *= 2
			if backoff > downloadPushRetryMaxBackoff {
				backoff = downloadPushRetryMaxBackoff
			}
			continue
		}

		attempt = 0
		backoff = downloadPushRetryMinBackoff

		switch ready.Status {
		case "cancelled", "completed", "failed", "expired":
			a.Logger.Debugf(
				"file_transfer download stream session=%s stopped status=%s",
				sessionID, ready.Status,
			)
			return
		}

		if ready.OfferedOffset > offset {
			a.Logger.Warnf(
				"file_transfer download stream session=%s re-sync offset %d → %d",
				sessionID, offset, ready.OfferedOffset,
			)
			offset = ready.OfferedOffset
			attempt = 0
			backoff = downloadPushRetryMinBackoff
			ackWaitStarted = time.Time{}
			continue
		}

		if !ready.CanPut {
			if ackWaitStarted.IsZero() {
				ackWaitStarted = time.Now()
			}
			if time.Since(ackWaitStarted) >= downloadPushAckWaitMax {
				a.Logger.Errorf(
					"file_transfer download stream session=%s offset=%d exhausted waiting for client ACK",
					sessionID, offset,
				)
				a.failDownloadStream(
					sessionID,
					fmt.Errorf("timed out waiting for client ACK at offset %d", offset),
				)
				return
			}
			if lastAckWaitLog.IsZero() || time.Since(lastAckWaitLog) >= downloadPushAckWaitLogEvery {
				a.Logger.Debugf(
					"file_transfer download stream session=%s offset=%d waiting for client ACK (%s)",
					sessionID, offset, time.Since(ackWaitStarted).Round(time.Millisecond),
				)
				lastAckWaitLog = time.Now()
			}
			if !a.waitForDownloadAckSlot(sessionID, stop) {
				return
			}
			continue
		}

		offset = ready.OfferedOffset
		offeredOffset, err := a.pushDownloadChunk(sessionID, offset)
		if err == nil {
			offset = offeredOffset
			attempt = 0
			backoff = downloadPushRetryMinBackoff
			ackWaitStarted = time.Time{}
			continue
		}

		if synced, ok := parseExpectedDownloadOffset(err); ok && synced > offset {
			a.Logger.Warnf(
				"file_transfer download stream session=%s re-sync offset %d → %d (%v)",
				sessionID, offset, synced, err,
			)
			offset = synced
			attempt = 0
			backoff = downloadPushRetryMinBackoff
			ackWaitStarted = time.Time{}
			continue
		}

		if !isRetryableDownloadPushError(err) {
			a.Logger.Errorf(
				"file_transfer download stream session=%s offset=%d fatal err=%v",
				sessionID, offset, err,
			)
			a.failDownloadStream(sessionID, err)
			return
		}

		if isDownloadPushAckWaitError(err) {
			if ackWaitStarted.IsZero() {
				ackWaitStarted = time.Now()
			}
			if time.Since(ackWaitStarted) >= downloadPushAckWaitMax {
				a.Logger.Errorf(
					"file_transfer download stream session=%s offset=%d exhausted waiting for client ACK: %v",
					sessionID, offset, err,
				)
				a.failDownloadStream(sessionID, err)
				return
			}
			if lastAckWaitLog.IsZero() || time.Since(lastAckWaitLog) >= downloadPushAckWaitLogEvery {
				a.Logger.Debugf(
					"file_transfer download stream session=%s offset=%d waiting for client ACK (%s)",
					sessionID, offset, time.Since(ackWaitStarted).Round(time.Millisecond),
				)
				lastAckWaitLog = time.Now()
			}
			if !a.waitForDownloadAckSlot(sessionID, stop) {
				return
			}
			continue
		}

		ackWaitStarted = time.Time{}
		attempt++
		if attempt >= downloadPushMaxAttempts {
			a.Logger.Errorf(
				"file_transfer download stream session=%s offset=%d exhausted retries err=%v",
				sessionID, offset, err,
			)
			a.failDownloadStream(sessionID, err)
			return
		}

		a.Logger.Warnf(
			"file_transfer download stream session=%s offset=%d retry %d/%d after %v: %v",
			sessionID, offset, attempt, downloadPushMaxAttempts, backoff, err,
		)

		timer := time.NewTimer(backoff)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
		if backoff > downloadPushRetryMaxBackoff {
			backoff = downloadPushRetryMaxBackoff
		}
	}
}

func parseExpectedDownloadOffset(err error) (int64, bool) {
	if err == nil {
		return 0, false
	}
	m := downloadChunkExpectedOffsetRe.FindStringSubmatch(err.Error())
	if len(m) != 2 {
		return 0, false
	}
	n, perr := strconv.ParseInt(m[1], 10, 64)
	if perr != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func isRetryableDownloadPushError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "download session not found") ||
		strings.Contains(msg, "file handle is missing") ||
		strings.Contains(msg, "failed to read chunk") ||
		strings.Contains(msg, "short read") {
		return false
	}

	if strings.Contains(msg, "push download chunk failed: 4") ||
		strings.Contains(msg, "download chunk ready failed: 4") {
		if strings.Contains(msg, "failed: 408") ||
			strings.Contains(msg, "failed: 425") ||
			strings.Contains(msg, "failed: 429") {
			return true
		}
		return false
	}
	// Network / timeout / 5xx / context deadline.
	return true
}

func isDownloadPushAckWaitError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "failed: 408") &&
		strings.Contains(msg, "Timed out waiting for client to ACK")
}

func (a *Agent) failDownloadStream(sessionID string, cause error) {
	a.DownloadTransferSessionsMu.Lock()
	session, ok := a.DownloadTransferSessions[sessionID]
	if ok && session != nil {
		delete(a.DownloadTransferSessions, sessionID)
	} else {
		session = nil
	}
	a.DownloadTransferSessionsMu.Unlock()

	if session != nil {
		if session.StopStream != nil {
			select {
			case <-session.StopStream:
			default:
				close(session.StopStream)
			}
		}
		if session.File != nil {
			_ = session.File.Close()
			session.File = nil
		}
		if session.RemoveOnClose && session.SourcePath != "" {
			if err := os.Remove(session.SourcePath); err != nil && !os.IsNotExist(err) {
				a.Logger.Warnf(
					"file_transfer download fail cleanup archive=%s session=%s err=%v",
					session.SourcePath, sessionID, err,
				)
			}
		}
	}

	msg := "download stream failed"
	if cause != nil {
		msg = cause.Error()
	}
	a.reportFileTransferFailure(sessionID, msg)
}

func (a *Agent) reportFileTransferFailure(sessionID, message string) {
	url := fmt.Sprintf("/api/v3/file-transfers/%s/fail/", sessionID)
	payload := map[string]string{"error": message}
	ctx, cancel := context.WithTimeout(context.Background(), fileTransferFailTimeout)
	defer cancel()
	resp, err := a.fileTransferHTTP().R().
		SetContext(ctx).
		SetBody(payload).
		Post(url)
	if err != nil {
		a.Logger.Errorf(
			"file_transfer fail callback session=%s err=%v", sessionID, err,
		)
		return
	}
	if resp.StatusCode() != 200 && resp.StatusCode() != 409 {
		a.Logger.Warnf(
			"file_transfer fail callback session=%s status=%d body=%s",
			sessionID, resp.StatusCode(), string(resp.Body()),
		)
	}
}

type downloadChunkReady struct {
	Status          string `json:"status"`
	CommittedOffset int64  `json:"committed_offset"`
	OfferedOffset   int64  `json:"offered_offset"`
	ChunkSize       int64  `json:"chunk_size"`
	TotalSize       int64  `json:"total_size"`
	CanPut          bool   `json:"can_put"`
}

func (a *Agent) getDownloadChunkReady(sessionID string) (downloadChunkReady, error) {
	var ready downloadChunkReady
	url := fmt.Sprintf("/api/v3/file-transfers/%s/download-chunk/", sessionID)
	ctx, cancel := context.WithTimeout(context.Background(), downloadReadyGetTimeout)
	defer cancel()

	resp, err := a.fileTransferHTTP().R().
		SetDebug(false).
		SetContext(ctx).
		Get(url)
	if err != nil {
		return ready, fmt.Errorf("failed to get download chunk ready: %w", err)
	}
	if resp.StatusCode() != 200 {
		return ready, fmt.Errorf(
			"download chunk ready failed: %d %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
	}
	if err := json.Unmarshal(resp.Body(), &ready); err != nil {
		return ready, fmt.Errorf("download chunk ready: invalid response: %w", err)
	}
	return ready, nil
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
	session.LastActivity = time.Now()
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

	url := fmt.Sprintf("/api/v3/file-transfers/%s/download-chunk/", sessionID)
	ctx, cancel := context.WithTimeout(context.Background(), downloadPutTimeout)
	defer cancel()

	putStart := time.Now()
	resp, err := a.fileTransferHTTP().R().
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
	var result struct {
		OfferedOffset int64 `json:"offered_offset"`
	}
	if err := json.Unmarshal(resp.Body(), &result); err == nil && result.OfferedOffset > 0 {
		offeredOffset = result.OfferedOffset
	}

	a.DownloadTransferSessionsMu.Lock()
	if s, ok := a.DownloadTransferSessions[sessionID]; ok && s != nil {
		if s.Hasher != nil && offset == s.HashedOffset {
			s.Hasher.Write(buf[:n])
			s.HashedOffset = offeredOffset
		}
	}
	a.DownloadTransferSessionsMu.Unlock()

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
	sourcePath := session.SourcePath
	totalSize := session.TotalSize
	hasher := session.Hasher
	hashedOffset := session.HashedOffset
	removeOnClose := session.RemoveOnClose
	delete(a.DownloadTransferSessions, sessionID)
	a.DownloadTransferSessionsMu.Unlock()

	computedSHA := ""
	if hasher != nil && hashedOffset == totalSize {
		computedSHA = hex.EncodeToString(hasher.Sum(nil))
	} else if sourcePath != "" {
		if sha, herr := hashFileSHA256(sourcePath); herr == nil {
			computedSHA = sha
		} else {
			a.Logger.Warnf(
				"file_transfer download finalize session=%s: failed to hash source: %v",
				sessionID, herr,
			)
		}
	}

	if stopStream != nil {
		close(stopStream)
	}
	if file != nil {
		_ = file.Close()
	}
	if removeOnClose && sourcePath != "" {
		if err := os.Remove(sourcePath); err != nil && !os.IsNotExist(err) {
			a.Logger.Warnf(
				"file_transfer download finalize session=%s: failed to remove archive %s: %v",
				sessionID, sourcePath, err,
			)
		}
	}

	return map[string]interface{}{
		"status": "completed",
		"sha256": computedSHA,
	}, nil
}

func hashFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (a *Agent) ReapStaleFileTransferSessions() {
	now := time.Now()

	type uploadCloseJob struct {
		sessionID string
		file      *os.File
	}
	var toRelease []uploadCloseJob
	var toRemove []*UploadTransferSession

	a.FileTransferSessionsMu.Lock()
	for id, session := range a.FileTransferSessions {
		if session == nil {
			delete(a.FileTransferSessions, id)
			continue
		}
		if session.File != nil {
			if now.Sub(session.LastActivity) > fileTransferSessionIdleTimeout {
				toRelease = append(toRelease, uploadCloseJob{id, session.File})
				session.File = nil
				session.DormantSince = now
			}
			continue
		}
		if !session.DormantSince.IsZero() &&
			now.Sub(session.DormantSince) > fileTransferPartialRetention {
			toRemove = append(toRemove, session)
			delete(a.FileTransferSessions, id)
		}
	}
	a.FileTransferSessionsMu.Unlock()

	for _, job := range toRelease {
		_ = job.file.Close()
		a.Logger.Infof(
			"file_transfer reaper: upload session=%s idle, handle released; "+
				".partial kept for resume",
			job.sessionID,
		)
	}
	for _, session := range toRemove {
		if session.PartialPath != "" {
			if err := os.Remove(session.PartialPath); err != nil && !os.IsNotExist(err) {
				a.Logger.Warnf(
					"file_transfer reaper: failed to remove partial file %s for session=%s: %v",
					session.PartialPath, session.SessionID, err,
				)
			}
		}
		a.Logger.Infof(
			"file_transfer reaper: removed dormant upload session=%s "+
				"(.partial retention elapsed)",
			session.SessionID,
		)
	}

	var staleDownloads []*DownloadTransferSession
	a.DownloadTransferSessionsMu.Lock()
	for id, session := range a.DownloadTransferSessions {
		if session == nil {
			delete(a.DownloadTransferSessions, id)
			continue
		}
		if now.Sub(session.LastActivity) > fileTransferSessionIdleTimeout {
			staleDownloads = append(staleDownloads, session)
			delete(a.DownloadTransferSessions, id)
		}
	}
	a.DownloadTransferSessionsMu.Unlock()

	for _, session := range staleDownloads {
		if session.StopStream != nil {
			close(session.StopStream)
		}
		if session.File != nil {
			_ = session.File.Close()
		}
		if session.RemoveOnClose && session.SourcePath != "" {
			if err := os.Remove(session.SourcePath); err != nil && !os.IsNotExist(err) {
				a.Logger.Warnf(
					"file_transfer reaper: failed to remove archive %s session=%s: %v",
					session.SourcePath, session.SessionID, err,
				)
			}
		}
		a.Logger.Infof(
			"file_transfer reaper: reaped idle download session=%s idle=%s",
			session.SessionID, now.Sub(session.LastActivity).Round(time.Second),
		)
	}
}
