// Copyright 2026 The Erigon Authors
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
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// FilesystemType returns the volume's filesystem type, e.g. "NTFS", "ReFS".
func FilesystemType(dirPath string) (string, error) {
	if _, err := os.Stat(dirPath); err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(dirPath)
	if err != nil {
		return "", err
	}
	absPathPtr, err := windows.UTF16PtrFromString(absPath)
	if err != nil {
		return "", err
	}

	// GetVolumePathName rather than filepath.VolumeName: a volume can be mounted
	// as a folder on another drive, whose letter names a different filesystem.
	mountPoint := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumePathName(absPathPtr, &mountPoint[0], uint32(len(mountPoint))); err != nil {
		return "", err
	}
	fsName := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumeInformation(&mountPoint[0], nil, 0, nil, nil, nil, &fsName[0], uint32(len(fsName))); err != nil {
		return "", err
	}
	return windows.UTF16ToString(fsName), nil
}
