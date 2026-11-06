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

package mmap

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// DeletedMappings counts this process's mappings of files under dir that were deleted on disk, and their total size.
func DeletedMappings(dir string) (count int, size uint64, err error) {
	f, err := os.Open("/proc/self/maps")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		path, deleted := strings.CutSuffix(line, " (deleted)")
		if !deleted {
			continue
		}
		fields := strings.Fields(path)
		if len(fields) < 6 || !strings.HasPrefix(fields[5], dir) {
			continue
		}
		var start, end uint64
		if _, err := fmt.Sscanf(fields[0], "%x-%x", &start, &end); err != nil {
			return 0, 0, err
		}
		count++
		size += end - start
	}
	return count, size, sc.Err()
}
