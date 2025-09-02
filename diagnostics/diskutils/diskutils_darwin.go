// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

//go:build darwin

package diskutils

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"syscall"

	"github.com/erigontech/erigon-lib/log/v3"
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
	cmd := exec.CommandContext(context.Background(), "diskutil", "info", disk)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", err
	}

	output := out.String()
	return output, nil
}
