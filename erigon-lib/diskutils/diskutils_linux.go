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

	return "", nil
}

func DiskInfo(disk string) (string, error) {
	uuid, err := diskUUID(disk)
	if err != nil {
		fmt.Println("Error getting disk UUID: %v", err)
		return "", err
	}

	headrsArray := []string{"UUID", "NAME", "KNAME", "PATH", "MAJ:MIN", "FSAVAIL", "FSUSE%", "FSTYPE", "MOUNTPOINT", "LABEL", "SIZE", "TYPE", "RO", "RM", "MODEL", "SERIAL", "STATE", "OWNER", "GROUP", "MODE", "ALIGNMENT", "MIN-IO", "OPT-IO", "PHY-SEC", "LOG-SEC", "ROTA", "SCHED", "RQ-SIZE", "DISC-ALN", "DISC-GRAN", "DISC-MAX", "DISC-ZERO", "WSAME", "WWN", "RAND", "PKNAME", "HCTL", "TRAN", "SUBSYSTEMS", "REV", "VENDOR"}
	//headrsArray := []string{"UUID", "NAME", "PATH", "FSAVAIL", "FSTYPE", "MOUNTPOINT", "TYPE", "MODEL", "TRAN", "VENDOR"}
	headersString := strings.Join(headrsArray, ",")

	percentSstring := strings.Repeat("%s|", len(headrsArray)-2) + "%s"
	fmt.Println("percentSstring", percentSstring)
	fmt.Println("len", len(strings.Split(percentSstring, "|")))

	//cmd := exec.Command("bash", "-c", `lsblk -o UUID,NAME,KNAME,PATH,MAJ:MIN,FSAVAIL,FSUSE%,FSTYPE,MOUNTPOINT,LABEL,SIZE,TYPE,RO,RM,MODEL,SERIAL,STATE,OWNER,GROUP,MODE,ALIGNMENT,MIN-IO,OPT-IO,PHY-SEC,LOG-SEC,ROTA,SCHED,RQ-SIZE,DISC-ALN,DISC-GRAN,DISC-MAX,DISC-ZERO,WSAME,WWN,RAND,PKNAME,HCTL,TRAN,SUBSYSTEMS,REV,VENDOR | awk 'NR>1 {printf "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41}'`)
	//cmd := exec.Command("bash", "-c", "lsblk -o"+headersString+` | awk 'NR>1 {printf "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41}'`)
	cmd := exec.Command("bash", "-c", "lsblk -o"+headersString+` | awk 'NR>1 {printf "`+percentSstring+`\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41}'`)
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		fmt.Println("Error executing lsblk command: %v", err)
		return "", err
	}

	output := out.String()
	fmt.Println("output", output)
	//find disk with uuid
	scanner := bufio.NewScanner(strings.NewReader(string(output)))

	for scanner.Scan() {
		line := scanner.Text()

		resultmap := make(map[string]string)
		//arr := strings.Fields(line)
		arr := strings.Split(line, "|")
		found := false
		if len(arr) > 0 {
			if arr[0] == uuid {
				found = true
				for i, v := range arr {
					if arr[0] == uuid {
						resultmap[headrsArray[i]] = v
					}
				}
			}

			if found {
				var str string
				for k, v := range resultmap {
					str = str + k + ":" + v + "\n"
				}
				fmt.Println("str", str)
				return str, nil
			}
		}

	}

	return output, nil
}
