// Copyright 2025 The Erigon Authors
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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// residencySink defeats dead-code elimination of the page-touching loop.
var residencySink int

func TestResidencyProbe(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	pg := os.Getpagesize()
	data := make([]byte, pg*8)
	for i := range data {
		data[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(p, data, 0o644))

	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()

	// Flush to disk and drop the file's clean pages from the page cache so the
	// mapping starts cold.
	require.NoError(t, f.Sync())
	require.NoError(t, unix.Fadvise(int(f.Fd()), 0, int64(len(data)), unix.FADV_DONTNEED))

	m, h2, err := Mmap(f, len(data))
	require.NoError(t, err)
	defer Munmap(m, h2)

	res, err := Resident(m)
	require.NoError(t, err)
	require.False(t, res, "pages should be cold right after a page-cache drop")

	// Touch every page to fault them all in from disk.
	for i := 0; i < len(m); i += pg {
		residencySink += int(m[i])
	}

	res, err = Resident(m)
	require.NoError(t, err)
	require.True(t, res, "all pages should be resident after touching them")
}
