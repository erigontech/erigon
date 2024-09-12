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

//go:build linux

package diskutils

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
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
	cmd := exec.Command("lsblk", "-o", "NAME,KNAME,PATH,MAJ:MIN,FSAVAIL,FSUSE%,FSTYPE,MOUNTPOINT,LABEL,UUID,SIZE,TYPE,RO,RM,MODEL,SERIAL,STATE,OWNER,GROUP,MODE,ALIGNMENT,MIN-IO,OPT-IO,PHY-SEC,LOG-SEC,ROTA,SCHED,RQ-SIZE,DISC-ALN,DISC-GRAN,DISC-MAX,DISC-ZERO,WSAME,WWN,RAND,PKNAME,HCTL,TRAN,SUBSYSTEMS,REV,VENDOR")

	// Capture the output
	output, err := cmd.Output()
	if err != nil {
		log.Fatalf("Error executing lsblk command: %v", err)
	}

	// Process the output
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	header := true

	for scanner.Scan() {
		line := scanner.Text()

		// Skip the header line
		if header {
			header = false
			continue
		}

		// Check if the line contains the mount point
		if strings.Contains(line, mountPoint) {
			fmt.Println(line)
			return line, nil
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading output: %v", err)
	}

	outputStr := output.String()
	return outputStr, nil
}
