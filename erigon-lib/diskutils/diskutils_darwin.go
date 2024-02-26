//go:build darwin

package diskutils

import (
	"syscall"

	"github.com/ledgerwatch/log/v3"
)

func MountPointForDirPath(dirPath string) string {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dirPath, &stat); err != nil {
		log.Debug("[diskutils] Error getting mount point for dir path:", dirPath, "Error:", err)
		return "/"
	}

	var mountPointBytes []byte
	for _, b := range stat.Mntonname {
		if b == 0 {
			break
		}
		mountPointBytes = append(mountPointBytes, byte(b))
	}
	mountPoint := string(mountPointBytes)

	return mountPoint
}
