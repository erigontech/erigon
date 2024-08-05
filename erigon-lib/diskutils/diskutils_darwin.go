//go:build darwin

package diskutils

import (
	"bytes"
	"os"
	"os/exec"
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

	mountPointBytes := []byte{}
	for _, b := range &stat.Mntonname {
		if b == 0 {
			break
		}
		mountPointBytes = append(mountPointBytes, byte(b))
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

func DiskInfo(disk string) (string, error) {
	cmd := exec.Command("diskutil", "info", disk)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", err
	}

	output := out.String()
	return output, nil
}
