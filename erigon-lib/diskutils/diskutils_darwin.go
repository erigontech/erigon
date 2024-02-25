//go:build darwin

package diskutils

import (
	"fmt"
	"syscall"
)

func MountPointForDirPath(dirPath string) string {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dirPath, &stat); err != nil {
		fmt.Println("Error:", err)
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
