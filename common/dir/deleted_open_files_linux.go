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
	"slices"
	"strings"
)

// DeletedOpenFiles returns the path of each of this process's open fds of a file under one of dirs that was deleted
// on disk. dirs must be symlink-resolved, as /proc shows paths.
func DeletedOpenFiles(dirs ...string) (paths []string, err error) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil {
			continue // closed since ReadDir
		}
		path, ok := strings.CutSuffix(target, " (deleted)")
		if ok && slices.ContainsFunc(dirs, func(d string) bool { return strings.HasPrefix(path, d+"/") }) {
			paths = append(paths, path)
		}
	}
	return paths, nil
}
