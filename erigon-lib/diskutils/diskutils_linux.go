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
		return 0, fmt.Errorf("error stating path: %v", err)
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
		fields := strings.Fields(line)
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
	cmd := exec.Command("lsblk", "-o", "MOUNTPOINT,UUID")

	// Capture the output
	output, err := cmd.Output()
	if err != nil {
		fmt.Println("Error executing lsblk command: %v", err)
	}

	// Process the output
	scanner := bufio.NewScanner(strings.NewReader(string(output)))

	for scanner.Scan() {
		line := scanner.Text()

		// Check if the line contains the mount point
		arr := strings.Fields(line)
		fmt.Println("uuid search arr", arr)
		if len(arr) > 1 {
			if arr[0] == disk {
				fmt.Println("Found uuid:", arr[1])
				return arr[1], nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading output: %v", err)
	}

	fmt.Println("UUID not found")
	return "", nil
}

func DiskInfo(disk string) (string, error) {
	uuid, err := diskUUID(disk)
	if err != nil {
		fmt.Println("Error getting disk UUID: %v", err)
		return "", err
	}

	headrsArray := []string{"UUID", "NAME", "PATH", "FSAVAIL", "FSTYPE", "MOUNTPOINT", "TYPE", "MODEL", "TRAN", "VENDOR"}
	headersString := strings.Join(headrsArray, ",")

	cmd := exec.Command("lsblk", "-o", headersString)
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		fmt.Println("Error executing lsblk command: %v", err)
		return "", err
	}

	output := out.String()
	//find disk with uuid
	scanner := bufio.NewScanner(strings.NewReader(string(output)))

	for scanner.Scan() {
		line := scanner.Text()

		resultmap := make(map[string]string)
		arr := strings.Fields(line)
		for i, v := range arr {
			found := false
			fmt.Println("uuid to scan:", arr)
			if arr[0] == uuid {
				fmt.Println("found uuid:", arr[0])
				resultmap[headrsArray[i]] = v
				found = true
			}

			if found {
				fmt.Println("resultmap", resultmap)
				//map to string
				var str string
				for k, v := range resultmap {
					str = str + k + ":" + v + ","
				}
				fmt.Println("str", str)
				return str, nil
			}
		}
	}

	return output, nil
	/*
		// Capture the output
		output, err := cmd.Output()
		if err != nil {
			fmt.Println("Error executing lsblk command: %v", err)
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

			//Check if the line contains the mount point
			if strings.Contains(line, uuid) {
				fmt.Println("result", line)
				return line, nil
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading output: %v", err)
		}

		return "unknown", nil*/
}
