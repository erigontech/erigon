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
