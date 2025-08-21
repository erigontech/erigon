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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/erigontech/erigon-lib/log/v3"
)

func getDeviceID(path string) (uint64, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(path, &stat)
	if err != nil {
		return 0, fmt.Errorf("error stating path: %w", err)
	}
	return stat.Dev, nil
}

func MountPointForDirPath(dirPath string) string {
	actualPath := SmlinkForDirPath(dirPath)

	devID, err := getDeviceID(actualPath)
	if err != nil {
		return ""
	}

	// Open /proc/self/mountinfo
	mountsFile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return ""
	}
	defer mountsFile.Close()

	// Read mountinfo to find matching device ID
	scanner := bufio.NewScanner(mountsFile)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, " ")
		if len(fields) < 5 {
			continue
		}

		// Extract device ID from the mountinfo line
		var deviceID uint64
		fmt.Sscanf(fields[4], "%d", &deviceID)
		if deviceID == devID {
			return fields[4]
		}
	}

	return ""
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

func diskUUID(disk string) (string, error) {
	cmd := exec.CommandContext(context.Background(), "lsblk", "-o", "MOUNTPOINT,UUID")

	// Capture the output
	output, err := cmd.Output()
	if err != nil {
		log.Debug("[diskutils] Error executing lsblk command: %v", err)
	}

	// Process the output
	scanner := bufio.NewScanner(bytes.NewReader(output))

	for scanner.Scan() {
		line := scanner.Text()

		// Check if the line contains the mount point
		arr := strings.Fields(line)
		if len(arr) > 1 {
			if arr[0] == disk {
				return arr[1], nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Debug("[diskutils] Error reading output: %v", err)
	}

	return "", nil
}

func DiskInfo(disk string) (string, error) {
	uuid, err := diskUUID(disk)
	if err != nil {
		log.Debug("[diskutils] Error getting disk UUID: %v", err)
		return "", err
	}

	headersArray := []string{"UUID", "NAME", "KNAME", "PATH", "MAJ:MIN", "FSAVAIL", "FSUSE%", "FSTYPE", "MOUNTPOINT", "LABEL", "SIZE", "TYPE", "RO", "RM", "MODEL", "SERIAL", "STATE", "OWNER", "GROUP", "MODE", "ALIGNMENT", "MIN-IO", "OPT-IO", "PHY-SEC", "LOG-SEC", "ROTA", "SCHED", "RQ-SIZE", "DISC-ALN", "DISC-GRAN", "DISC-MAX", "DISC-ZERO", "WSAME", "WWN", "RAND", "PKNAME", "HCTL", "TRAN", "SUBSYSTEMS", "REV", "VENDOR"}
	headersString := strings.Join(headersArray, ",")

	percentSstring := strings.Repeat("%s|", len(headersArray)-2) + "%s"
	valString := ""
	for i := 0; i < len(headersArray)-1; i++ {
		valString = fmt.Sprintf("%s$%d,", valString, i+1)
	}
	valString = fmt.Sprintf("%s$%d", valString, len(headersArray))

	cmd := exec.CommandContext(context.Background(), "bash", "-c", "lsblk -o"+headersString+` | awk 'NR>1 {printf "`+percentSstring+`\n", `+valString+`}'`)
	output, err := cmd.Output()
	if err != nil {
		log.Debug("[diskutils] Error executing lsblk command: %v", err)
	}

	return processOutput(output, uuid, headersArray)
}

func processOutput(output []byte, uuid string, headersArray []string) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()

		arr := strings.Split(line, "|")
		if len(arr) > 0 {
			if arr[0] == uuid {
				resultmap := make(map[string]string)
				for i, v := range arr {
					resultmap[headersArray[i]] = v
				}

				var builder strings.Builder
				for k, v := range resultmap {
					builder.WriteString(fmt.Sprintf("%s: %s\n", k, v))
				}

				return builder.String(), nil
			}
		}
	}

	return "", fmt.Errorf("UUID %s not found", uuid)
}
