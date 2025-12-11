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

//go:build windows

package diskutils

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	shortcut "github.com/nyaosorg/go-windows-shortcut"
)

func MountPointForDirPath(dirPath string) string {
	mountPoint := "C"
	actualPath := SmlinkForDirPath(dirPath)

	psCommand := fmt.Sprintf(`(Get-Item -Path "%s").PSDrive.Name`, actualPath)
	cmd := exec.CommandContext(context.Background(), "powershell", "-Command", psCommand)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err == nil {
		mountPoint = strings.TrimSpace(out.String())
	}

	mountPoint = mountPoint + ":"
	return mountPoint
}

func SmlinkForDirPath(dirPath string) string {
	if filepath.Ext(dirPath) == ".lnk" {
		actualPath, _, err := shortcut.Read(dirPath)
		if err != nil {
			return dirPath
		}

		return actualPath
	} else {
		return dirPath
	}
}

func DiskInfo(disk string) (string, error) {
	disk = strings.TrimSuffix(disk, ":")
	// Get the serial number for the disk with the specified drive letter
	psCommand := fmt.Sprintf(`
	$volume = Get-Volume -DriveLetter %s

	if ($volume) {
    	# Get the partition associated with this volume
    	$partition = Get-Partition -DriveLetter %s

    	if ($partition) {
        	# Get the disk associated with this partition
        	$disk = Get-Disk -Number $partition.DiskNumber

        	if ($disk) {
		   		Get-PhysicalDisk  -SerialNumber $disk.SerialNumber | Select-Object  DeviceID, FriendlyName, SerialNumber, MediaType, BusType, FirmwareVersion, Manufacturer, Model, Size, PartitionStyle, OperationalStatus, Usage
        	} else {
				exit 1
        	}
    	} else {
			exit 2
    	}
	} else {	
		exit 3
	}
	`, disk, disk)
	cmd := exec.CommandContext(context.Background(), "powershell", "-Command", psCommand)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		exitCode := err.(*exec.ExitError).ExitCode()
		if exitCode == 1 {
			return "", fmt.Errorf("error getting disk for partition with drive letter: %s", disk)
		} else if exitCode == 2 {
			return "", fmt.Errorf("error getting partition for volume with drive letter: %s", disk)
		} else {
			return "", fmt.Errorf("error getting volume with drive letter: %s", disk)
		}
	}

	return strings.TrimSpace(out.String()), nil
}
