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
	"slices"
	"strings"
)

// DeletedMappings returns the path of each of this process's mappings of a file under one of dirs that was deleted
// on disk, and their total size. dirs must be symlink-resolved, as /proc shows paths.
func DeletedMappings(dirs ...string) (paths []string, size uint64, err error) {
	f, err := os.Open("/proc/self/maps")
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		line, deleted := strings.CutSuffix(line, " (deleted)")
		i := strings.IndexByte(line, '/')
		if !deleted || i < 0 || !slices.ContainsFunc(dirs, func(d string) bool { return strings.HasPrefix(line[i:], d+"/") }) {
			continue
		}
		var start, end uint64
		if _, err := fmt.Sscanf(line, "%x-%x", &start, &end); err != nil {
			return nil, 0, err
		}
		paths = append(paths, line[i:])
		size += end - start
	}
	return paths, size, sc.Err()
}
