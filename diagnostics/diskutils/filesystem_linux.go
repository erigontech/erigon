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

//go:build linux

package diskutils

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// FilesystemType returns the mount's filesystem type, e.g. "ext4", "xfs", "zfs".
func FilesystemType(dirPath string) (string, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(dirPath, &stat); err != nil {
		return "", fmt.Errorf("stat %s: %w", dirPath, err)
	}
	devID := fmt.Sprintf("%d:%d", unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)))

	mountsFile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	defer mountsFile.Close()

	return fsTypeFromMountinfo(mountsFile, devID)
}
