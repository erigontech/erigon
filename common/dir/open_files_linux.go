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

package dir

import (
	"os"
	"strings"
)

// OpenFiles counts this process's open file descriptors, and those of files under dir that were deleted on disk.
func OpenFiles(dir string) (open, deleted int, err error) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		open++
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil {
			continue // closed since ReadDir
		}
		if path, ok := strings.CutSuffix(target, " (deleted)"); ok && strings.HasPrefix(path, dir) {
			deleted++
		}
	}
	return open, deleted, nil
}
