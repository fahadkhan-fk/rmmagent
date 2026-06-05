package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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
