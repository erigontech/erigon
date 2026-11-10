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

	"github.com/erigontech/erigon/common/iouring"
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

// probedAnon registers an anonymous mapping of length bytes, offset pages into a MaxReadSize-aligned window, with
// nothing mapped around it. Its pages stay non-resident until written.
func probedAnon(t *testing.T, offset, length int) []byte {
	t.Helper()
	prev := BatchPrefetch
	BatchPrefetch = true
	t.Cleanup(func() { BatchPrefetch = prev })

	pg, span := os.Getpagesize(), iouring.MaxReadSize
	reserved, err := unix.Mmap(-1, 0, 3*span, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	require.NoError(t, err)
	base := uintptr(unsafe.Pointer(&reserved[0]))
	unmap := func(from, to int) {
		if from < to {
			_, _, errno := unix.Syscall(unix.SYS_MUNMAP, base+uintptr(from), uintptr(to-from), 0)
			require.Zero(t, errno)
		}
	}
	start := (span-int(base%uintptr(span)))%span + offset*pg
	end := start + (length+pg-1)/pg*pg
	unmap(0, start)
	unmap(end, len(reserved))

	f, err := os.CreateTemp(t.TempDir(), "f")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	m := reserved[start : start+length : start+length]
	registerMapping(m, f)
	t.Cleanup(func() {
		unregisterMapping(m)
		unmap(start, end)
	})
	return m
}

func touchProbe(pr *Probe, b *byte) func() { return func() { pr.Touch(unsafe.Pointer(b)) } }

func coldPageAt(b *byte) ColdPage { return ColdPage(uintptr(unsafe.Pointer(b))) }

func TestProbeRemembersEveryPageOfTheMincoreSpan(t *testing.T) {
	pg := os.Getpagesize()
	m := probedAnon(t, 0, 3*pg)
	m[2*pg] = 1
	pr := newProbe()
	require.PanicsWithValue(t, coldPageAt(&m[0]), touchProbe(pr, &m[0]))

	m[0] = 1
	require.PanicsWithValue(t, coldPageAt(&m[0]), touchProbe(pr, &m[0]), "a remembered cold page needs no new mincore")
	require.NoError(t, unix.Madvise(m[2*pg:3*pg], unix.MADV_DONTNEED))
	require.NotPanics(t, touchProbe(pr, &m[2*pg]), "a remembered resident page needs no new mincore")
}

func TestProbeSpanStaysInsideTheMapping(t *testing.T) {
	pg := os.Getpagesize()
	m := probedAnon(t, 1, 2*pg-100)
	m[pg] = 1
	pr := newProbe()
	require.NotPanics(t, touchProbe(pr, &m[pg]))
	require.PanicsWithValue(t, coldPageAt(&m[0]), touchProbe(pr, &m[0]))
}

func TestMarkResidentOverridesARememberedColdPage(t *testing.T) {
	m := probedAnon(t, 0, os.Getpagesize())
	pr := newProbe()
	require.PanicsWithValue(t, coldPageAt(&m[0]), touchProbe(pr, &m[0]))

	pr.MarkResident([]uintptr{uintptr(unsafe.Pointer(&m[0]))})
	require.NotPanics(t, touchProbe(pr, &m[0]))
}

func TestProbeAsksOnePageOutsideRegisteredMappings(t *testing.T) {
	pg := os.Getpagesize()
	m := probedAnon(t, 0, 2*pg)
	unregisterMapping(m)
	pr := newProbe()
	require.PanicsWithValue(t, coldPageAt(&m[0]), touchProbe(pr, &m[0]))

	m[pg] = 1
	require.NotPanics(t, touchProbe(pr, &m[pg]), "the first mincore must not have answered for this page")
}
