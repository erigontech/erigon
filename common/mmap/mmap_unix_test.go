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

//go:build !windows

package mmap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// mapPages returns a real mapping of n pages, so the slice starts on a page
// boundary exactly like the mappings madvise is called on in production.
func mapPages(t *testing.T, n int) Ro {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pages.bin")
	size := n * os.Getpagesize()
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o600))

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	m, err := OpenRo(f, size)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Unmap()) })
	return m
}

func TestPageAligned(t *testing.T) {
	page := os.Getpagesize()
	m := mapPages(t, 2)

	t.Run("shorter than one page", func(t *testing.T) {
		require.Len(t, pageAligned(m[:300]), 300,
			"madvise rounds the length up, so a sub-page mapping must still be advised")
	})

	t.Run("partial last page", func(t *testing.T) {
		require.Len(t, pageAligned(m[:page+300]), page+300,
			"trimming the tail advises only part of the mapping and splits its VMA")
	})

	t.Run("whole pages", func(t *testing.T) {
		require.Len(t, pageAligned(m[:2*page]), 2*page)
	})

	t.Run("unaligned start runs to the next boundary", func(t *testing.T) {
		require.Len(t, pageAligned(m[7:page+7]), 7,
			"madvise needs a page-aligned address, so the head is skipped")
	})

	t.Run("no page boundary inside", func(t *testing.T) {
		require.Nil(t, pageAligned(m[7:20]))
	})

	t.Run("empty", func(t *testing.T) {
		require.Nil(t, pageAligned(nil))
	})
}
