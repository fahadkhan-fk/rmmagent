//go:build windows

package agent

import (
	"os"
	"syscall"
	"time"
)

func ListDirectoryWindows(rawPath string, page, pageSize int, nameFilter string) (map[string]interface{}, error) {
	return listDirectory(rawPath, page, pageSize, nameFilter)
}

func FilePropertiesWindows(rawPath string, limits folderSummaryLimits) (map[string]interface{}, error) {
	return fileProperties(rawPath, &limits)
}

func FileMkdirWindows(rawParentPath, rawName string) (map[string]interface{}, error) {
	return fileMkdir(rawParentPath, rawName)
}

func FileRenameWindows(rawPath, rawNewName string) (map[string]interface{}, error) {
	return fileRename(rawPath, rawNewName)
}

func FileDeleteWindows(rawPaths []string) (map[string]interface{}, error) {
	return fileDelete(rawPaths)
}

func clearPathReadOnlyIfNeeded(path string) bool {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return false
	}

	attrs, err := syscall.GetFileAttributes(pathPtr)
	if err != nil {
		return false
	}
	if attrs&syscall.FILE_ATTRIBUTE_READONLY == 0 {
		return false
	}

	newAttrs := attrs &^ syscall.FILE_ATTRIBUTE_READONLY
	return syscall.SetFileAttributes(pathPtr, newAttrs) == nil
}

func fileTimes(info os.FileInfo) (modified, created, accessed string) {
	modified = formatFileTime(info.ModTime())
	created = modified
	accessed = modified

	sys, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return modified, created, accessed
	}

	created = formatFileTime(time.Unix(0, sys.CreationTime.Nanoseconds()))
	accessed = formatFileTime(time.Unix(0, sys.LastAccessTime.Nanoseconds()))
	modified = formatFileTime(time.Unix(0, sys.LastWriteTime.Nanoseconds()))
	return modified, created, accessed
}

func fileAttributeFlags(_ string, info os.FileInfo, _ string) (hidden, system, readOnly bool) {
	sys, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		readOnly = info.Mode()&0200 == 0
		return false, false, readOnly
	}

	attrs := sys.FileAttributes
	hidden = attrs&syscall.FILE_ATTRIBUTE_HIDDEN != 0
	system = attrs&syscall.FILE_ATTRIBUTE_SYSTEM != 0
	readOnly = attrs&syscall.FILE_ATTRIBUTE_READONLY != 0
	return hidden, system, readOnly
}
