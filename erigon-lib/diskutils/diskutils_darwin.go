//go:build darwin

package diskutils

import (
	"os"
	"syscall"

	"github.com/ledgerwatch/log/v3"
)

func MountPointForDirPath(dirPath string) string {
	actualPath := SmlinkForDirPath(dirPath)

	var stat syscall.Statfs_t
	if err := syscall.Statfs(actualPath, &stat); err != nil {
		log.Debug("[diskutils] Error getting mount point for dir path:", actualPath, "Error:", err)
		return "/"
	}

	mountPointBytes := make([]byte, 0, len(stat.Mntonname))
	for i := range stat.Mntonname {
		if stat.Mntonname[i] == 0 {
			break
		}
		mountPointBytes = append(mountPointBytes, byte(stat.Mntonname[i]))
	}
	mountPoint := string(mountPointBytes)

	return mountPoint
}

func SmlinkForDirPath(dirPath string) string {
	fileInfo, err := os.Lstat(dirPath)
	if err != nil {
		log.Debug("[diskutils] Error getting file info for dir path:", dirPath, "Error:", err)
		return dirPath
	}

	if fileInfo.Mode()&os.ModeSymlink != 0 {
		targetPath, err := os.Readlink(dirPath)
		if err != nil {
			log.Debug("[diskutils] Error getting target path for symlink:", dirPath, "Error:", err)
			return dirPath
		} else {
			return targetPath
		}
	} else {
		return dirPath
	}
}
