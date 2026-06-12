//go:build windows

package agent

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"time"
)

func PrepareFilesUploadWindows(a *Agent, p *NatsMsg) (map[string]interface{}, error) {
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

	chunkSize, err := parsePayloadInt64(p.Data, "chunk_size")
	if err != nil {
		return nil, err
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk_size must be greater than 0")
	}

	filename, err := parsePayloadString(p.Data, "filename")
	if err != nil {
		return nil, err
	}
	if err := validateUploadFilename(filename); err != nil {
		return nil, err
	}

	parentDir := filepath.Dir(destinationPath)
	parentInfo, err := os.Stat(parentDir)
	if err != nil {
		return nil, fmt.Errorf("parent directory does not exist")
	}
	if !parentInfo.IsDir() {
		return nil, fmt.Errorf("parent directory does not exist")
	}

	resume, resumeOffset := parsePayloadResume(p.Data)
	partialPath := destinationPath + ".partial"
	file, committedOffset, err := prepareUploadPartialFile(
		partialPath, resume, resumeOffset, totalSize,
	)
	if err != nil {
		return nil, err
	}

	var hasher hash.Hash
	if !resume {
		hasher = sha256.New()
	}

	a.FileTransferSessionsMu.Lock()

	if existing, ok := a.FileTransferSessions[sessionID]; ok {
		if existing.File != nil {
			_ = existing.File.Close()
		}
	}

	a.FileTransferSessions[sessionID] = &UploadTransferSession{
		SessionID:       sessionID,
		DestinationPath: destinationPath,
		PartialPath:     partialPath,
		Filename:        filename,
		TotalSize:       totalSize,
		ChunkSize:       chunkSize,
		CommittedOffset: committedOffset,
		File:            file,
		LastActivity:    time.Now(),
		Hasher:          hasher,
		HashedOffset:    committedOffset,
	}
	a.FileTransferSessionsMu.Unlock()

	return map[string]interface{}{
		"status":           "ready",
		"committed_offset": committedOffset,
	}, nil
}
