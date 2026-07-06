package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	fileBrowserDefaultPageSize = 500
	fileBrowserMaxPageSize     = 1000
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

func parseFileBrowserPageParams(data map[string]string) (int, int) {
	page := 1
	pageSize := fileBrowserDefaultPageSize

	if v, err := strconv.Atoi(strings.TrimSpace(data["page"])); err == nil && v >= 1 {
		page = v
	}
	if v, err := strconv.Atoi(strings.TrimSpace(data["page_size"])); err == nil && v >= 1 {
		pageSize = v
	}
	if pageSize > fileBrowserMaxPageSize {
		pageSize = fileBrowserMaxPageSize
	}
	return page, pageSize
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
	if pageSize < 1 {
		pageSize = fileBrowserDefaultPageSize
	}
	if pageSize > fileBrowserMaxPageSize {
		pageSize = fileBrowserMaxPageSize
	}
	return page, pageSize
}

func listDirectory(rawPath string, page, pageSize int) (map[string]interface{}, error) {
	cleaned, err := validateUploadDestinationPath(rawPath)
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

	sort.Slice(items, func(i, j int) bool {
		return compareFileBrowserItems(items[i], items[j]) < 0
	})

	total := len(items)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	hasMore := end < total

	pageItems := items[start:end]
	encoded := make([]map[string]interface{}, 0, len(pageItems))
	for _, item := range pageItems {
		encoded = append(encoded, item.toMap())
	}

	return map[string]interface{}{
		"path":      cleaned,
		"items":     encoded,
		"has_more":  hasMore,
		"page":      page,
		"page_size": pageSize,
		"total":     total,
	}, nil
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

func fileProperties(rawPath string) (map[string]interface{}, error) {
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

	name := entryNameFromPath(cleaned)
	item := buildFileBrowserItem(filepath.Dir(cleaned), name, info)
	item.Path = cleaned
	item.ID = cleaned
	item.Name = name

	return item.toMap(), nil
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
	if _, err := os.Lstat(newPath); err == nil {
		return nil, fmt.Errorf("already exists")
	} else if !os.IsNotExist(err) {
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to access path")
	}

	if err := os.Mkdir(newPath, 0o755); err != nil {
		if os.IsPermission(err) {
			return nil, fmt.Errorf("permission denied")
		}
		return nil, fmt.Errorf("unable to create folder")
	}

	return fileProperties(newPath)
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
	if _, err := os.Lstat(newPath); err == nil {
		return nil, fmt.Errorf("already exists")
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
			return nil, fmt.Errorf("already exists")
		}
		return nil, fmt.Errorf("unable to rename path")
	}

	return fileProperties(newPath)
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
