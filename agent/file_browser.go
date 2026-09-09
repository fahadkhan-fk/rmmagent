package agent

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	fileBrowserDefaultPageSize      = 500
	fileBrowserMaxPageSize          = 1000
	fileBrowserMaxPage              = 10_000
	fileBrowserFilterSnapshotTTL    = 60 * time.Second
	fileBrowserFilterSnapshotMax    = 32
	fileBrowserFilterMaxLen         = 255
	defaultFolderSummaryMaxFiles    = 100_000
	defaultFolderSummaryMaxDepth    = 32
	defaultFolderSummaryMaxDuration = 20 * time.Second
)

type fileBrowserItem struct {
	ID        string
	Name      string
	Path      string
	Type      string
	Extension string
	Size      string
	Modified  string
	Created   string
	Accessed  string
	Hidden    bool
	System    bool
	ReadOnly  bool
}

type folderSummaryLimits struct {
	maxFiles    int
	maxDepth    int
	maxDuration time.Duration
}

type folderSummary struct {
	totalBytes  int64
	fileCount   int
	folderCount int
	truncated   bool
}

func parseFolderSummaryLimits(data map[string]string) folderSummaryLimits {
	limits := folderSummaryLimits{
		maxFiles:    defaultFolderSummaryMaxFiles,
		maxDepth:    defaultFolderSummaryMaxDepth,
		maxDuration: defaultFolderSummaryMaxDuration,
	}
	if data == nil {
		return limits
	}
	if v, err := strconv.Atoi(strings.TrimSpace(data["max_files"])); err == nil && v > 0 {
		limits.maxFiles = v
	}
	if v, err := strconv.Atoi(strings.TrimSpace(data["max_depth"])); err == nil && v > 0 {
		limits.maxDepth = v
	}
	if v, err := strconv.Atoi(strings.TrimSpace(data["max_duration_seconds"])); err == nil && v > 0 {
		limits.maxDuration = time.Duration(v) * time.Second
	}
	return limits
}

func summarizeFolder(root string, limits folderSummaryLimits) folderSummary {
	summary := folderSummary{}
	deadline := time.Now().Add(limits.maxDuration)
	entriesSeen := 0

	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			// Skip unreadable nodes
			if d != nil && d.IsDir() && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if path == root {
			return nil
		}
		if time.Now().After(deadline) {
			summary.truncated = true
			return filepath.SkipAll
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return nil
		}
		depth := len(strings.Split(filepath.ToSlash(rel), "/"))
		if depth > limits.maxDepth {
			summary.truncated = true
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, infoErr := d.Info()
		if infoErr != nil {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		entriesSeen++
		if entriesSeen > limits.maxFiles {
			summary.truncated = true
			return filepath.SkipAll
		}

		if d.IsDir() {
			summary.folderCount++
			return nil
		}

		summary.fileCount++
		if size := info.Size(); size > 0 {
			summary.totalBytes += size
		}
		return nil
	})

	return summary
}

func parseFileBrowserPageParams(data map[string]string) (int, int) {
	page := 1
	pageSize := fileBrowserDefaultPageSize

	if v, err := strconv.Atoi(strings.TrimSpace(data["page"])); err == nil && v >= 1 {
		page = v
	}
	if v, err := strconv.Atoi(strings.TrimSpace(data["page_size"])); err == nil && v >= 1 {
		pageSize = v
	}
	return normalizeFileBrowserPage(page, pageSize)
}

func parseFileBrowserNameFilter(data map[string]string) string {
	if data == nil {
		return ""
	}
	raw := strings.TrimSpace(data["filter"])
	if raw == "" {
		return ""
	}
	raw = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == 0 {
			return -1
		}
		return r
	}, raw)
	raw = strings.TrimSpace(raw)
	if len(raw) > fileBrowserFilterMaxLen {
		raw = raw[:fileBrowserFilterMaxLen]
	}
	return raw
}

type fileBrowserFilterSnapshot struct {
	path    string
	filter  string
	items   []fileBrowserItem
	expires time.Time
}

var (
	fileBrowserFilterSnapshotsMu sync.Mutex
	fileBrowserFilterSnapshots   = map[string]*fileBrowserFilterSnapshot{}
)

func fileBrowserFilterSnapshotKey(path, filter string) string {
	return path + "\x00" + strings.ToLower(filter)
}

func getFileBrowserFilterSnapshot(path, filter string) ([]fileBrowserItem, bool) {
	key := fileBrowserFilterSnapshotKey(path, filter)
	now := time.Now()

	fileBrowserFilterSnapshotsMu.Lock()
	defer fileBrowserFilterSnapshotsMu.Unlock()

	snap, ok := fileBrowserFilterSnapshots[key]
	if !ok {
		return nil, false
	}
	if now.After(snap.expires) {
		delete(fileBrowserFilterSnapshots, key)
		return nil, false
	}
	return snap.items, true
}

func storeFileBrowserFilterSnapshot(path, filter string, items []fileBrowserItem) {
	key := fileBrowserFilterSnapshotKey(path, filter)
	now := time.Now()

	fileBrowserFilterSnapshotsMu.Lock()
	defer fileBrowserFilterSnapshotsMu.Unlock()

	if len(fileBrowserFilterSnapshots) >= fileBrowserFilterSnapshotMax {
		var oldestKey string
		var oldestExp time.Time
		for k, snap := range fileBrowserFilterSnapshots {
			if now.After(snap.expires) {
				delete(fileBrowserFilterSnapshots, k)
				continue
			}
			if oldestKey == "" || snap.expires.Before(oldestExp) {
				oldestKey = k
				oldestExp = snap.expires
			}
		}
		if len(fileBrowserFilterSnapshots) >= fileBrowserFilterSnapshotMax && oldestKey != "" {
			delete(fileBrowserFilterSnapshots, oldestKey)
		}
	}

	copied := make([]fileBrowserItem, len(items))
	copy(copied, items)
	fileBrowserFilterSnapshots[key] = &fileBrowserFilterSnapshot{
		path:    path,
		filter:  filter,
		items:   copied,
		expires: now.Add(fileBrowserFilterSnapshotTTL),
	}
}

func clearFileBrowserFilterSnapshotsForTest() {
	fileBrowserFilterSnapshotsMu.Lock()
	defer fileBrowserFilterSnapshotsMu.Unlock()
	fileBrowserFilterSnapshots = map[string]*fileBrowserFilterSnapshot{}
}

func nameMatchesFileBrowserFilter(name, filter string) bool {
	if filter == "" {
		return true
	}
	return strings.Contains(strings.ToLower(name), strings.ToLower(filter))
}

func mulNonNeg(a, b int) (int, bool) {
	if a < 0 || b < 0 {
		return 0, false
	}
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > math.MaxInt/b {
		return 0, false
	}
	product := a * b
	if product < 0 {
		return 0, false
	}
	return product, true
}

func paginateFileBrowserItems(items []fileBrowserItem, page, pageSize int) (encoded []map[string]interface{}, total int, hasMore bool) {
	total = len(items)
	encoded = make([]map[string]interface{}, 0)
	page, pageSize = normalizeFileBrowserPage(page, pageSize)

	start, ok := mulNonNeg(page-1, pageSize)
	if !ok || start >= total {
		return encoded, total, false
	}
	end := start + pageSize
	if end < start || end > total {
		end = total
	}
	hasMore = end < total

	pageItems := items[start:end]
	encoded = make([]map[string]interface{}, 0, len(pageItems))
	for _, item := range pageItems {
		encoded = append(encoded, item.toMap())
	}
	return encoded, total, hasMore
}

func buildDirectoryItems(cleaned string) ([]fileBrowserItem, error) {
	entries, err := os.ReadDir(cleaned)
	if err != nil {
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to read directory")
	}

	items := make([]fileBrowserItem, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if name == "." || name == ".." {
			continue
		}
		if strings.HasSuffix(name, ".partial") {
			continue
		}

		fullPath := filepath.Join(cleaned, name)
		entryInfo, err := entry.Info()
		if err != nil {
			entryInfo, err = os.Lstat(fullPath)
			if err != nil {
				continue
			}
		}

		items = append(items, buildFileBrowserItem(cleaned, name, entryInfo))
	}
	return items, nil
}

func extensionFromName(name string) string {
	ext := filepath.Ext(name)
	if ext == "" || ext == "." || ext == ".." {
		return ""
	}
	return strings.ToUpper(strings.TrimPrefix(ext, "."))
}

func formatFileTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func classifyEntryType(fullPath string, info os.FileInfo) string {
	if info.IsDir() {
		return "folder"
	}
	if info.Mode()&os.ModeSymlink != 0 {
		if target, err := os.Stat(fullPath); err == nil && target.IsDir() {
			return "folder"
		}
	}
	return "file"
}

func buildFileBrowserItem(dirPath, name string, info os.FileInfo) fileBrowserItem {
	fullPath := filepath.Join(dirPath, name)
	itemType := classifyEntryType(fullPath, info)
	modified, created, accessed := fileTimes(info)
	hidden, system, readOnly := fileAttributeFlags(fullPath, info, name)

	size := "0"
	if itemType == "file" {
		size = strconv.FormatInt(info.Size(), 10)
	}

	return fileBrowserItem{
		ID:        fullPath,
		Name:      name,
		Path:      fullPath,
		Type:      itemType,
		Extension: extensionFromName(name),
		Size:      size,
		Modified:  modified,
		Created:   created,
		Accessed:  accessed,
		Hidden:    hidden,
		System:    system,
		ReadOnly:  readOnly,
	}
}

func (item fileBrowserItem) toMap() map[string]interface{} {
	out := map[string]interface{}{
		"id":       item.ID,
		"name":     item.Name,
		"path":     item.Path,
		"type":     item.Type,
		"size":     item.Size,
		"modified": item.Modified,
		"created":  item.Created,
		"accessed": item.Accessed,
		"hidden":   item.Hidden,
		"system":   item.System,
		"readonly": item.ReadOnly,
	}
	if item.Extension != "" {
		out["extension"] = item.Extension
	}
	return out
}

func compareFileBrowserItems(a, b fileBrowserItem) int {
	aFolder := a.Type == "folder"
	bFolder := b.Type == "folder"
	if aFolder && !bFolder {
		return -1
	}
	if !aFolder && bFolder {
		return 1
	}
	return strings.Compare(strings.ToLower(a.Name), strings.ToLower(b.Name))
}

func normalizeFileBrowserPage(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if page > fileBrowserMaxPage {
		page = fileBrowserMaxPage
	}
	if pageSize < 1 {
		pageSize = fileBrowserDefaultPageSize
	}
	if pageSize > fileBrowserMaxPageSize {
		pageSize = fileBrowserMaxPageSize
	}
	return page, pageSize
}

func listDirectory(rawPath string, page, pageSize int, nameFilter string) (map[string]interface{}, error) {
	path := strings.TrimSpace(rawPath)
	if path == "" {
		resolved, resolveErr := resolveDefaultFileBrowserPath()
		if resolveErr != nil {
			return nil, resolveErr
		}
		path = resolved
	}

	cleaned, err := validateUploadDestinationPath(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path")
	}

	info, err := os.Stat(cleaned)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("path not found")
		}
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to access path")
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("path is not a directory")
	}

	page, pageSize = normalizeFileBrowserPage(page, pageSize)
	nameFilter = strings.TrimSpace(nameFilter)

	var items []fileBrowserItem

	if nameFilter != "" {
		if cached, ok := getFileBrowserFilterSnapshot(cleaned, nameFilter); ok {
			items = cached
		} else {
			built, buildErr := buildDirectoryItems(cleaned)
			if buildErr != nil {
				return nil, buildErr
			}
			filtered := make([]fileBrowserItem, 0, len(built))
			for _, item := range built {
				if nameMatchesFileBrowserFilter(item.Name, nameFilter) {
					filtered = append(filtered, item)
				}
			}
			sort.Slice(filtered, func(i, j int) bool {
				return compareFileBrowserItems(filtered[i], filtered[j]) < 0
			})
			storeFileBrowserFilterSnapshot(cleaned, nameFilter, filtered)
			items = filtered
		}
	} else {
		if page > 1 {
			if cached, ok := getFileBrowserFilterSnapshot(cleaned, ""); ok {
				items = cached
			}
		}
		if items == nil {
			built, buildErr := buildDirectoryItems(cleaned)
			if buildErr != nil {
				return nil, buildErr
			}
			sort.Slice(built, func(i, j int) bool {
				return compareFileBrowserItems(built[i], built[j]) < 0
			})
			storeFileBrowserFilterSnapshot(cleaned, "", built)
			items = built
		}
	}

	encoded, total, hasMore := paginateFileBrowserItems(items, page, pageSize)

	return map[string]interface{}{
		"path":      cleaned,
		"items":     encoded,
		"has_more":  hasMore,
		"page":      page,
		"page_size": pageSize,
		"total":     total,
	}, nil
}

func resolveDefaultFileBrowserPath() (string, error) {
	candidates := defaultFileBrowserPathCandidates()
	if path := firstReadableDirectory(candidates); path != "" {
		return path, nil
	}
	return "", fmt.Errorf("no readable default directory found")
}

func defaultFileBrowserPathCandidates() []string {
	switch runtime.GOOS {
	case "windows":
		return defaultWindowsFileBrowserPathCandidates()
	case "darwin":
		return []string{"/Users", "/"}
	default:
		// linux and other unix-like agents
		return []string{"/home", "/"}
	}
}

func firstReadableDirectory(candidates []string) string {
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		cleaned := normalizeFileBrowserCandidate(candidate)
		if cleaned == "" {
			continue
		}
		key := cleaned
		if runtime.GOOS == "windows" {
			key = strings.ToLower(cleaned)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		info, err := os.Stat(cleaned)
		if err != nil || !info.IsDir() {
			continue
		}
		if _, err := os.ReadDir(cleaned); err != nil {
			continue
		}
		return cleaned
	}
	return ""
}

func normalizeFileBrowserCandidate(candidate string) string {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return ""
	}
	if runtime.GOOS == "windows" && len(candidate) == 2 && candidate[1] == ':' {
		candidate += `\`
	}
	cleaned, err := validateUploadDestinationPath(candidate)
	if err != nil {
		return ""
	}
	return cleaned
}

func entryNameFromPath(cleaned string) string {
	name := filepath.Base(cleaned)
	if name == "." || name == "" {
		return cleaned
	}
	if name == string(os.PathSeparator) {
		trimmed := strings.TrimRight(cleaned, string(os.PathSeparator))
		if trimmed != "" {
			return filepath.Base(trimmed)
		}
		return cleaned
	}
	return name
}

func fileProperties(rawPath string, limits *folderSummaryLimits) (map[string]interface{}, error) {
	cleaned, err := validateUploadDestinationPath(rawPath)
	if err != nil {
		return nil, fmt.Errorf("invalid path")
	}

	info, err := os.Lstat(cleaned)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("path not found")
		}
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to access path")
	}

	parent := filepath.Dir(cleaned)
	name := entryNameFromPath(cleaned)
	item := buildFileBrowserItem(parent, name, info)
	item.Path = cleaned
	item.ID = cleaned
	item.Name = name

	out := item.toMap()
	out["location"] = parent

	if item.Type == "folder" && limits != nil {
		summary := summarizeFolder(cleaned, *limits)
		out["size"] = strconv.FormatInt(summary.totalBytes, 10)
		out["file_count"] = summary.fileCount
		out["folder_count"] = summary.folderCount
		out["summary_truncated"] = summary.truncated
	}

	return out, nil
}

func validateFileBrowserName(name string) error {
	if strings.HasSuffix(name, " ") || strings.HasSuffix(name, ".") {
		return fmt.Errorf("invalid name")
	}
	trimmed := strings.TrimSpace(name)
	if err := validateUploadFilename(trimmed); err != nil {
		return err
	}
	return nil
}

func isProtectedDeletePath(cleaned string) bool {
	vol := filepath.VolumeName(cleaned)
	if vol != "" {
		rest := strings.TrimPrefix(cleaned, vol)
		rest = strings.Trim(strings.TrimPrefix(rest, `\`), `/`)
		if rest == "" {
			return true
		}
	}
	if cleaned == string(os.PathSeparator) {
		return true
	}
	return false
}

func removePathEntry(cleaned string) error {
	info, err := os.Lstat(cleaned)
	if err != nil {
		return err
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return os.Remove(cleaned)
	}
	if info.IsDir() {
		return os.RemoveAll(cleaned)
	}
	return os.Remove(cleaned)
}

func removePathEntryWithRetry(cleaned string) error {
	err := removePathEntry(cleaned)
	if err == nil {
		return nil
	}
	if clearPathReadOnlyIfNeeded(cleaned) {
		return removePathEntry(cleaned)
	}
	return err
}

func mapDeleteError(err error) string {
	if err == nil {
		return ""
	}
	if os.IsNotExist(err) {
		return "path not found"
	}
	if os.IsPermission(err) {
		return "permission denied"
	}
	return "unable to delete path"
}

func parseDeletePaths(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("missing paths")
	}

	if strings.HasPrefix(raw, "[") {
		var paths []string
		if err := json.Unmarshal([]byte(raw), &paths); err != nil {
			return nil, fmt.Errorf("invalid paths")
		}
		if len(paths) == 0 {
			return nil, fmt.Errorf("missing paths")
		}
		out := make([]string, 0, len(paths))
		for _, path := range paths {
			path = strings.TrimSpace(path)
			if path == "" {
				return nil, fmt.Errorf("invalid paths")
			}
			out = append(out, path)
		}
		return out, nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("invalid paths")
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("missing paths")
	}
	return out, nil
}

func fileMkdir(rawParentPath, rawName string) (map[string]interface{}, error) {
	name := strings.TrimSpace(rawName)
	if err := validateFileBrowserName(name); err != nil {
		return nil, fmt.Errorf("invalid name")
	}

	parentPath, err := validateUploadDestinationPath(rawParentPath)
	if err != nil {
		return nil, fmt.Errorf("invalid path")
	}

	parentInfo, err := os.Stat(parentPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("path not found")
		}
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to access path")
	}
	if !parentInfo.IsDir() {
		return nil, fmt.Errorf("path is not a directory")
	}

	newPath := filepath.Join(parentPath, name)
	if existing, err := os.Lstat(newPath); err == nil {
		if existing.IsDir() {
			return nil, fmt.Errorf(
				"This destination already contains a folder named %q", name,
			)
		}
		return nil, fmt.Errorf(
			"This destination already contains a file named %q", name,
		)
	} else if !os.IsNotExist(err) {
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to access path")
	}

	if err := os.Mkdir(newPath, 0o755); err != nil {
		if os.IsExist(err) {
			if existing, statErr := os.Lstat(newPath); statErr == nil && !existing.IsDir() {
				return nil, fmt.Errorf(
					"This destination already contains a file named %q", name,
				)
			}
			return nil, fmt.Errorf(
				"This destination already contains a folder named %q", name,
			)
		}
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to create folder")
	}

	return fileProperties(newPath, nil)
}

func fileRename(rawPath, rawNewName string) (map[string]interface{}, error) {
	newName := strings.TrimSpace(rawNewName)
	if err := validateFileBrowserName(newName); err != nil {
		return nil, fmt.Errorf("invalid name")
	}

	cleaned, err := validateUploadDestinationPath(rawPath)
	if err != nil {
		return nil, fmt.Errorf("invalid path")
	}

	oldName := entryNameFromPath(cleaned)
	if oldName == newName {
		return nil, fmt.Errorf("new name must differ")
	}

	if _, err := os.Lstat(cleaned); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("path not found")
		}
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to access path")
	}

	newPath := filepath.Join(filepath.Dir(cleaned), newName)
	if existing, err := os.Lstat(newPath); err == nil {
		if existing.IsDir() {
			return nil, fmt.Errorf(
				"This destination already contains a folder named %q", newName,
			)
		}
		return nil, fmt.Errorf(
			"This destination already contains a file named %q", newName,
		)
	} else if !os.IsNotExist(err) {
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to access path")
	}

	if err := os.Rename(cleaned, newPath); err != nil {
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		if os.IsExist(err) {
			if existing, statErr := os.Lstat(newPath); statErr == nil && existing.IsDir() {
				return nil, fmt.Errorf(
					"This destination already contains a folder named %q", newName,
				)
			}
			return nil, fmt.Errorf(
				"This destination already contains a file named %q", newName,
			)
		}
		return nil, fmt.Errorf("unable to rename path")
	}

	return fileProperties(newPath, nil)
}

func fileDelete(rawPaths []string) (map[string]interface{}, error) {
	if len(rawPaths) == 0 {
		return nil, fmt.Errorf("missing paths")
	}

	results := make([]map[string]interface{}, 0, len(rawPaths))
	for _, rawPath := range rawPaths {
		result := map[string]interface{}{
			"path": strings.TrimSpace(rawPath),
		}

		cleaned, err := validateUploadDestinationPath(rawPath)
		if err != nil {
			result["success"] = false
			result["error"] = "invalid path"
			results = append(results, result)
			continue
		}
		result["path"] = cleaned

		if isProtectedDeletePath(cleaned) {
			result["success"] = false
			result["error"] = "protected path"
			results = append(results, result)
			continue
		}

		if err := removePathEntryWithRetry(cleaned); err != nil {
			result["success"] = false
			result["error"] = mapDeleteError(err)
			results = append(results, result)
			continue
		}

		result["success"] = true
		results = append(results, result)
	}

	return map[string]interface{}{"results": results}, nil
}
