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

package pagedidx

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func build(t *testing.T, name string, pageSize uint64, itemsPerGroup []uint64, values []uint64) *Index {
	t.Helper()
	var items uint64
	for _, n := range itemsPerGroup {
		items += n
	}
	path := filepath.Join(t.TempDir(), name)
	w, err := NewWriter(path, pageSize, uint64(len(itemsPerGroup)), items, values[len(values)-1])
	require.NoError(t, err)
	w.NoFsync()
	for _, n := range itemsPerGroup {
		w.AddGroup(n)
	}
	for _, v := range values {
		w.AddPage(v)
	}
	require.NoError(t, w.Build())

	idx, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(idx.Close)
	return idx
}

func TestGroupedLookup(t *testing.T) {
	itemsPerGroup := []uint64{3, 1, 5, 2, 1}
	const pageSize = 2
	var items uint64
	for _, n := range itemsPerGroup {
		items += n
	}
	pages := (items + pageSize - 1) / pageSize
	values := make([]uint64, pages)
	for i := range values {
		values[i] = uint64(i) * 37 // monotone, uneven gaps
	}

	idx := build(t, "grouped", pageSize, itemsPerGroup, values)

	var ordinal uint64
	for group, n := range itemsPerGroup {
		for member := range n {
			v, ok := idx.Get(uint64(group), member)
			require.True(t, ok, "group %d member %d", group, member)
			require.Equal(t, values[ordinal/pageSize], v, "group %d member %d", group, member)
			ordinal++
		}
	}
	require.Equal(t, items, ordinal)
}

func TestSingleItem(t *testing.T) {
	idx := build(t, "one", 64, []uint64{1}, []uint64{0})
	v, ok := idx.Get(0, 0)
	require.True(t, ok)
	require.Equal(t, uint64(0), v)
}

// A position from a file that does not belong to this index must not panic.
func TestOutOfRange(t *testing.T) {
	idx := build(t, "oob", 2, []uint64{3, 1}, []uint64{0, 37})
	_, ok := idx.Get(99, 0)
	require.False(t, ok, "group past the end")
	_, ok = idx.Get(0, 1<<40)
	require.False(t, ok, "member past the end")
}

func TestEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty")
	w, err := NewWriter(path, 64, 0, 0, 0)
	require.NoError(t, err)
	w.NoFsync()
	require.NoError(t, w.Build())

	idx, err := Open(path)
	require.NoError(t, err)
	defer idx.Close()
	require.True(t, idx.Empty())
	_, ok := idx.Get(0, 0)
	require.False(t, ok)
}

func TestPageSizeZeroRejected(t *testing.T) {
	_, err := NewWriter(filepath.Join(t.TempDir(), "bad"), 0, 1, 1, 1)
	require.Error(t, err)
}

// Group is the reverse of Get's first half: which group owns an ordinal.
func TestGroup(t *testing.T) {
	// group 0 holds ordinals 0-2, group 1 holds 3, group 2 holds 4-5
	itemsPerGroup := []uint64{3, 1, 2}
	idx := build(t, "group", 2, itemsPerGroup, []uint64{0, 37, 74})

	want := []uint64{0, 0, 0, 1, 2, 2}
	for ordinal, wantGroup := range want {
		g, ok := idx.Group(uint64(ordinal))
		require.True(t, ok, "ordinal %d", ordinal)
		require.Equal(t, wantGroup, g, "ordinal %d", ordinal)
	}

	_, ok := idx.Group(6)
	require.False(t, ok, "one past the last item")
	_, ok = idx.Group(1 << 40)
	require.False(t, ok, "far past the last item")
}

// An empty group owns no ordinal, so the search has to skip past it.
func TestGroupSkipsEmpty(t *testing.T) {
	// groups 1 and 3 are empty; ordinals 0-2 are group 0, ordinal 3 is group 2
	itemsPerGroup := []uint64{3, 0, 1, 0, 2}
	idx := build(t, "empty-groups", 2, itemsPerGroup, []uint64{0, 37, 74})

	want := []uint64{0, 0, 0, 2, 4, 4}
	for ordinal, wantGroup := range want {
		g, ok := idx.Group(uint64(ordinal))
		require.True(t, ok, "ordinal %d", ordinal)
		require.Equal(t, wantGroup, g, "ordinal %d", ordinal)
	}
}

// Page is the reverse of Get's second half: which page holds a value.
func TestPage(t *testing.T) {
	idx := build(t, "page", 2, []uint64{3, 1, 2}, []uint64{10, 37, 74})

	for _, tc := range []struct {
		value uint64
		page  uint64
	}{
		{10, 0}, {11, 0}, {36, 0},
		{37, 1}, {73, 1},
		{74, 2}, {1 << 40, 2}, // past the last page start, still the last page
	} {
		p, ok := idx.Page(tc.value)
		require.True(t, ok, "value %d", tc.value)
		require.Equal(t, tc.page, p, "value %d", tc.value)
	}

	_, ok := idx.Page(9)
	require.False(t, ok, "before the first page")
}

func TestReverseOnEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty2")
	w, err := NewWriter(path, 64, 0, 0, 0)
	require.NoError(t, err)
	w.NoFsync()
	require.NoError(t, w.Build())
	idx, err := Open(path)
	require.NoError(t, err)
	defer idx.Close()

	_, ok := idx.Group(0)
	require.False(t, ok)
	_, ok = idx.Page(0)
	require.False(t, ok)
}
