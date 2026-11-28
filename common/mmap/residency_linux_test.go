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
	"context"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	dir2 "github.com/erigontech/erigon/common/dir"
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

	m, err := OpenRo(f, len(data))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Unmap()) })

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

func TestReadPagesStartsNoBatchOnceTheContextIsDone(t *testing.T) {
	prev := BatchPrefetch
	BatchPrefetch = true
	defer func() { BatchPrefetch = prev }()
	p := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(p, make([]byte, 2*os.Getpagesize()), 0o600))
	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()
	m, err := OpenRo(f, 2*os.Getpagesize())
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Unmap()) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ReadPages(ctx, []uintptr{uintptr(unsafe.Pointer(&m[0]))}), context.Canceled)
}

func TestDeletedMappingsCountsAMappedFileUntilUnmap(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir()) // /proc shows resolved paths
	require.NoError(t, err)
	p := filepath.Join(dir, "f.seg")
	pg := os.Getpagesize()
	require.NoError(t, os.WriteFile(p, make([]byte, pg*4), 0o600))
	f, err := os.Open(p)
	require.NoError(t, err)
	m, err := OpenRo(f, pg*4)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	paths, _, err := DeletedMappings(dir)
	require.NoError(t, err)
	require.Empty(t, paths, "a mapped file that still exists is not counted")

	require.NoError(t, dir2.RemoveFile(p))
	paths, size, err := DeletedMappings(dir)
	require.NoError(t, err)
	require.Equal(t, []string{p}, paths)
	require.Equal(t, uint64(pg*4), size)

	paths, _, err = DeletedMappings(dir[:len(dir)-1])
	require.NoError(t, err)
	require.Empty(t, paths, "a sibling directory with the same prefix is not counted")

	require.NoError(t, m.Unmap())
	paths, _, err = DeletedMappings(dir)
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestMappingOfFindsTheMappingThatHoldsThePage(t *testing.T) {
	defer func(prev bool) { BatchPrefetch = prev }(BatchPrefetch)
	BatchPrefetch = true
	f, err := os.CreateTemp(t.TempDir(), "f")
	require.NoError(t, err)
	defer f.Close()
	bufs := [][]byte{make([]byte, 3*osPageSize), make([]byte, osPageSize), make([]byte, 2*osPageSize)}
	for _, b := range []int{2, 0, 1} {
		registerMapping(bufs[b], f)
	}
	defer func() {
		for _, b := range bufs {
			unregisterMapping(b)
		}
	}()
	for _, b := range bufs {
		start := uintptr(unsafe.Pointer(&b[0]))
		for _, p := range []uintptr{start, start + uintptr(len(b)) - 1} {
			m, ok := mappingOf(p)
			require.True(t, ok)
			require.Equal(t, start, m.start)
			require.Equal(t, int(f.Fd()), m.fd)
		}
	}
	unregisterMapping(bufs[1])
	_, ok := mappingOf(uintptr(unsafe.Pointer(&bufs[1][0])))
	require.False(t, ok, "an unregistered mapping is not found")
}
