package agent

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
)

const (
	defaultArchiveMaxPaths     = 100
	defaultArchiveMaxFiles     = 10_000
	defaultArchiveMaxSizeBytes = int64(4 * 1024 * 1024 * 1024) // 4 GiB; ZIP64 when exceeded
	defaultArchiveMaxDepth     = 32
	archiveDiskSpaceMargin     = int64(64 * 1024 * 1024)
	// Prefix used for all temp archive files so a startup sweep can reclaim orphans.
	archiveTempPrefix = "trmm-archive-"
)

func ensureArchiveDiskSpace(dir string, needed int64) error {
	if needed <= 0 {
		return nil
	}
	usage, err := disk.Usage(dir)
	if err != nil || usage == nil {
		return nil
	}
	if int64(usage.Free) < needed+archiveDiskSpaceMargin {
		return fmt.Errorf(
			"insufficient disk space to build archive: need ~%d bytes, %d free in %s",
			needed+archiveDiskSpaceMargin, usage.Free, dir,
		)
	}
	return nil
}

type archiveLimits struct {
	maxFiles     int
	maxSizeBytes int64
	maxDepth     int
}

type archiveFileEntry struct {
	absPath string
	zipName string
	size    int64
}

func parsePayloadPathsJSON(data map[string]string) ([]string, error) {
	raw, err := parsePayloadString(data, "paths_json")
	if err != nil {
		return nil, err
	}
	var paths []string
	if err := json.Unmarshal([]byte(raw), &paths); err != nil {
		return nil, fmt.Errorf("invalid paths_json")
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("paths_json must contain at least one path")
	}
	cleaned := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		validated, err := validateUploadDestinationPath(p)
		if err != nil {
			return nil, fmt.Errorf("invalid path %q: %w", p, err)
		}
		key := strings.ToLower(validated)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		cleaned = append(cleaned, validated)
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("paths_json must contain at least one valid path")
	}
	if len(cleaned) > defaultArchiveMaxPaths {
		return nil, fmt.Errorf("too many paths (max %d)", defaultArchiveMaxPaths)
	}
	return cleaned, nil
}

func parseArchiveLimits(data map[string]string) archiveLimits {
	limits := archiveLimits{
		maxFiles:     defaultArchiveMaxFiles,
		maxSizeBytes: defaultArchiveMaxSizeBytes,
		maxDepth:     defaultArchiveMaxDepth,
	}
	if v, err := parsePayloadInt64(data, "max_files"); err == nil && v > 0 {
		limits.maxFiles = int(v)
	}
	if v, err := parsePayloadInt64(data, "max_size_bytes"); err == nil && v > 0 {
		limits.maxSizeBytes = v
	}
	if v, err := parsePayloadInt64(data, "max_depth"); err == nil && v > 0 {
		limits.maxDepth = int(v)
	}
	return limits
}

func archiveTempPath(sessionID string) string {
	safeID := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			return r
		default:
			return '_'
		}
	}, sessionID)
	return filepath.Join(os.TempDir(), fmt.Sprintf("%s%s.zip", archiveTempPrefix, safeID))
}

func isArchiveTempPath(p string) bool {
	if p == "" {
		return false
	}
	base := filepath.Base(p)
	if !strings.HasPrefix(base, archiveTempPrefix) || !strings.HasSuffix(base, ".zip") {
		return false
	}
	return filepath.Clean(filepath.Dir(p)) == filepath.Clean(os.TempDir())
}

func dedupeZipEntryName(name string, used map[string]struct{}) string {
	if _, ok := used[name]; !ok {
		used[name] = struct{}{}
		return name
	}
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(filepath.Base(name), ext)
	dir := filepath.Dir(name)
	for i := 2; i < 10_000; i++ {
		candidateBase := fmt.Sprintf("%s_%d%s", base, i, ext)
		candidate := candidateBase
		if dir != "." && dir != "" {
			candidate = filepath.ToSlash(filepath.Join(dir, candidateBase))
		}
		if _, ok := used[candidate]; !ok {
			used[candidate] = struct{}{}
			return candidate
		}
	}
	fallback := fmt.Sprintf("%s_%d%s", base, time.Now().UnixNano(), ext)
	used[fallback] = struct{}{}
	return fallback
}

func collectArchiveEntries(
	roots []string,
	limits archiveLimits,
) (files []archiveFileEntry, dirNames []string, warnings []string, totalBytes int64, err error) {
	usedNames := make(map[string]struct{})
	fileCount := 0
	depthTruncated := false

	for _, root := range roots {
		info, statErr := os.Lstat(root)
		if statErr != nil {
			return nil, nil, warnings, 0, fmt.Errorf("path not accessible: %s: %w", root, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, nil, warnings, 0, fmt.Errorf("symbolic links cannot be archived: %s", root)
		}

		rootBase := filepath.Base(root)
		if rootBase == "" || rootBase == "." || rootBase == string(filepath.Separator) {
			rootBase = "archive"
		}

		if info.IsDir() {
			rootZipPrefix := dedupeZipEntryName(filepath.ToSlash(rootBase), usedNames)
			dirNames = append(dirNames, rootZipPrefix+"/")

			walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
				if walkErr != nil {
					warnings = append(warnings, fmt.Sprintf("skipped %s: %v", path, walkErr))
					return nil
				}

				rel, relErr := filepath.Rel(root, path)
				if relErr != nil {
					return relErr
				}
				if rel == "." {
					return nil
				}

				depth := len(strings.Split(filepath.ToSlash(rel), "/"))
				if depth > limits.maxDepth {
					if !depthTruncated {
						depthTruncated = true
						warnings = append(
							warnings,
							fmt.Sprintf(
								"Some items were omitted because they exceed the maximum archive depth (%d).",
								limits.maxDepth,
							),
						)
					}
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}

				entryInfo, entryErr := d.Info()
				if entryErr != nil {
					warnings = append(warnings, fmt.Sprintf("skipped %s: %v", path, entryErr))
					return nil
				}
				if entryInfo.Mode()&os.ModeSymlink != 0 {
					warnings = append(warnings, fmt.Sprintf("skipped symlink %s", path))
					return nil
				}

				zipRel := filepath.ToSlash(rel)
				zipName := dedupeZipEntryName(rootZipPrefix+"/"+zipRel, usedNames)

				if d.IsDir() {
					dirNames = append(dirNames, zipName+"/")
					return nil
				}

				size := entryInfo.Size()
				if size < 0 {
					return fmt.Errorf("invalid file size for %s", path)
				}
				totalBytes += size
				if totalBytes > limits.maxSizeBytes {
					return fmt.Errorf(
						"archive exceeds maximum size (%d bytes)",
						limits.maxSizeBytes,
					)
				}
				fileCount++
				if fileCount > limits.maxFiles {
					return fmt.Errorf("archive exceeds maximum file count (%d)", limits.maxFiles)
				}

				files = append(files, archiveFileEntry{
					absPath: path,
					zipName: zipName,
					size:    size,
				})
				return nil
			})
			if walkErr != nil {
				return nil, nil, warnings, totalBytes, walkErr
			}
			continue
		}

		size := info.Size()
		if size < 0 {
			return nil, nil, warnings, 0, fmt.Errorf("invalid file size for %s", root)
		}
		totalBytes += size
		if totalBytes > limits.maxSizeBytes {
			return nil, nil, warnings, totalBytes, fmt.Errorf(
				"archive exceeds maximum size (%d bytes)",
				limits.maxSizeBytes,
			)
		}
		fileCount++
		if fileCount > limits.maxFiles {
			return nil, nil, warnings, totalBytes, fmt.Errorf(
				"archive exceeds maximum file count (%d)",
				limits.maxFiles,
			)
		}

		zipName := dedupeZipEntryName(filepath.ToSlash(rootBase), usedNames)
		files = append(files, archiveFileEntry{
			absPath: root,
			zipName: zipName,
			size:    size,
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].zipName < files[j].zipName
	})
	sort.Strings(dirNames)

	return files, dirNames, warnings, totalBytes, nil
}

func writeArchiveZip(
	tempPath string,
	roots []string,
	limits archiveLimits,
) (warnings []string, err error) {
	files, dirNames, warnings, totalBytes, err := collectArchiveEntries(roots, limits)
	if err != nil {
		return warnings, err
	}
	if len(files) == 0 && len(dirNames) == 0 {
		return warnings, fmt.Errorf("nothing to archive")
	}

	if err := ensureArchiveDiskSpace(filepath.Dir(tempPath), totalBytes); err != nil {
		return warnings, err
	}

	out, err := os.Create(tempPath)
	if err != nil {
		return warnings, fmt.Errorf("failed to create archive file: %w", err)
	}

	success := false
	defer func() {
		_ = out.Close()
		if !success {
			_ = os.Remove(tempPath)
		}
	}()

	zw := zip.NewWriter(out)
	usedDir := make(map[string]struct{})

	for _, dirName := range dirNames {
		if _, ok := usedDir[dirName]; ok {
			continue
		}
		usedDir[dirName] = struct{}{}
		header := &zip.FileHeader{
			Name:   dirName,
			Method: zip.Store,
		}
		header.SetMode(0o755 | os.ModeDir)
		if _, err := zw.CreateHeader(header); err != nil {
			return warnings, fmt.Errorf("failed to add directory %s: %w", dirName, err)
		}
	}

	for _, entry := range files {
		info, err := entryInfoFromPath(entry.absPath, entry.size)
		if err != nil {
			return warnings, fmt.Errorf("failed to stat %s: %w", entry.absPath, err)
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return warnings, fmt.Errorf("failed to create zip header for %s: %w", entry.absPath, err)
		}
		header.Name = entry.zipName
		header.Method = zip.Deflate
		header.UncompressedSize64 = uint64(entry.size)

		writer, err := zw.CreateHeader(header)
		if err != nil {
			return warnings, fmt.Errorf("failed to add %s: %w", entry.zipName, err)
		}

		src, err := os.Open(entry.absPath)
		if err != nil {
			return warnings, fmt.Errorf("permission denied or unreadable file %s: %w", entry.absPath, err)
		}
		if _, err := io.Copy(writer, src); err != nil {
			_ = src.Close()
			return warnings, fmt.Errorf("failed to write %s into archive: %w", entry.zipName, err)
		}
		_ = src.Close()
	}

	if err := zw.Close(); err != nil {
		return warnings, fmt.Errorf("failed to finalize archive: %w", err)
	}
	if err := out.Close(); err != nil {
		return warnings, fmt.Errorf("failed to close archive file: %w", err)
	}

	info, err := os.Stat(tempPath)
	if err != nil {
		return warnings, fmt.Errorf("failed to stat archive file: %w", err)
	}
	if info.Size() == 0 {
		return warnings, fmt.Errorf("archive file is empty")
	}

	success = true
	return warnings, nil
}

func entryInfoFromPath(path string, size int64) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if size >= 0 && !info.IsDir() {
		return &archiveFileInfo{FileInfo: info, size: size}, nil
	}
	return info, nil
}

type archiveFileInfo struct {
	os.FileInfo
	size int64
}

func (a *archiveFileInfo) Size() int64 {
	return a.size
}

func (a *Agent) PrepareFilesDownloadArchive(p *NatsMsg) (map[string]interface{}, error) {
	sessionID, err := parsePayloadString(p.Data, "session_id")
	if err != nil {
		return nil, err
	}

	paths, err := parsePayloadPathsJSON(p.Data)
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

	limits := parseArchiveLimits(p.Data)

	go a.buildAndServeArchive(sessionID, paths, chunkSize, limits)

	return map[string]interface{}{"status": "building"}, nil
}

// buildAndServeArchive func builds the temp zip, reports readiness to the server and then
// streams it through the normal download chunk relay.
func (a *Agent) buildAndServeArchive(
	sessionID string, paths []string, chunkSize int64, limits archiveLimits,
) {
	tempPath := archiveTempPath(sessionID)
	a.DownloadTransferSessionsMu.Lock()
	if existing, ok := a.DownloadTransferSessions[sessionID]; ok && existing != nil {
		if existing.StopStream != nil {
			close(existing.StopStream)
		}
		if existing.File != nil {
			_ = existing.File.Close()
		}
		if existing.RemoveOnClose && existing.SourcePath != "" {
			_ = os.Remove(existing.SourcePath)
		}
		delete(a.DownloadTransferSessions, sessionID)
	}
	a.DownloadTransferSessionsMu.Unlock()

	_ = os.Remove(tempPath)

	warnings, err := writeArchiveZip(tempPath, paths, limits)
	if err != nil {
		_ = os.Remove(tempPath)
		a.Logger.Errorln("files_download_archive build:", err)
		a.reportArchiveError(sessionID, err.Error())
		return
	}

	info, err := os.Stat(tempPath)
	if err != nil || info.Size() <= 0 {
		_ = os.Remove(tempPath)
		a.reportArchiveError(sessionID, "archive file is empty")
		return
	}
	totalSize := info.Size()

	if !a.reportArchiveReady(sessionID, tempPath, totalSize, warnings) {
		_ = os.Remove(tempPath)
		a.reportArchiveError(sessionID, "failed to report archive ready after retries")
		return
	}

	file, err := os.Open(tempPath)
	if err != nil {
		_ = os.Remove(tempPath)
		a.reportArchiveError(sessionID, fmt.Sprintf("failed to open archive: %v", err))
		return
	}

	stopStream := make(chan struct{})
	hasher := sha256.New()

	a.DownloadTransferSessionsMu.Lock()
	a.DownloadTransferSessions[sessionID] = &DownloadTransferSession{
		SessionID:     sessionID,
		SourcePath:    tempPath,
		TotalSize:     totalSize,
		ChunkSize:     chunkSize,
		File:          file,
		StopStream:    stopStream,
		LastActivity:  time.Now(),
		Hasher:        hasher,
		HashedOffset:  0,
		RemoveOnClose: true,
	}
	a.DownloadTransferSessionsMu.Unlock()

	go a.streamDownloadChunks(sessionID, stopStream, 0)
}

func (a *Agent) reportArchiveReady(
	sessionID, archivePath string, totalSize int64, warnings []string,
) bool {
	url := fmt.Sprintf("/api/v3/file-transfers/%s/archive-ready/", sessionID)
	payload := map[string]interface{}{
		"total_size":   totalSize,
		"archive_path": archivePath,
	}
	if len(warnings) > 0 {
		payload["warnings"] = warnings
	}

	const maxAttempts = 4
	backoff := 2 * time.Second
	for attempt := 1; ; attempt++ {
		resp, err := a.rClient.R().SetBody(payload).Post(url)
		if err == nil && resp.StatusCode() == 200 {
			return true
		}
		if err == nil && resp.StatusCode() >= 400 && resp.StatusCode() < 500 {
			a.Logger.Warnf(
				"file_transfer archive-ready callback session=%s status=%d body=%s (not retrying)",
				sessionID, resp.StatusCode(), string(resp.Body()),
			)
			return false
		}
		if attempt >= maxAttempts {
			if err != nil {
				a.Logger.Errorf(
					"file_transfer archive-ready callback session=%s failed after %d attempts err=%v",
					sessionID, attempt, err,
				)
			} else {
				a.Logger.Warnf(
					"file_transfer archive-ready callback session=%s failed after %d attempts status=%d",
					sessionID, attempt, resp.StatusCode(),
				)
			}
			return false
		}
		time.Sleep(backoff)
		backoff *= 2
	}
}

func (a *Agent) reportArchiveError(sessionID, message string) {
	url := fmt.Sprintf("/api/v3/file-transfers/%s/archive-ready/", sessionID)
	payload := map[string]interface{}{"error": message}
	if _, err := a.rClient.R().SetBody(payload).Post(url); err != nil {
		a.Logger.Errorf(
			"file_transfer archive-error callback session=%s err=%v", sessionID, err,
		)
	}
}

// SweepOrphanedArchives removes orphaned temporary ZIP archives left behind by previous agent runs
// (example: after a crash during archive creation or transfer).
func (a *Agent) SweepOrphanedArchives() {
	dir := os.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	removed := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, archiveTempPrefix) || !strings.HasSuffix(name, ".zip") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err == nil {
			removed++
		}
	}
	if removed > 0 {
		a.Logger.Infof("file_transfer startup: removed %d orphaned archive temp file(s)", removed)
	}
}
