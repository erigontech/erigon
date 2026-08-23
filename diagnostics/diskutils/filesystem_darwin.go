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

//go:build darwin

package diskutils

import (
	"fmt"
	"syscall"
)

// FilesystemType returns the mount's filesystem type, e.g. "apfs", "hfs".
func FilesystemType(dirPath string) (string, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dirPath, &stat); err != nil {
		return "", fmt.Errorf("statfs %s: %w", dirPath, err)
	}

	name := make([]byte, 0, len(stat.Fstypename))
	for _, b := range &stat.Fstypename {
		if b == 0 {
			break
		}
		name = append(name, byte(b))
	}
	return string(name), nil
}
