package agent

import (
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
