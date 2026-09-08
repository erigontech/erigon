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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func build(t *testing.T, name string, pageSize uint64, itemsPerRun, pageValues []uint64) *Index {
	t.Helper()
	var items uint64
	for _, n := range itemsPerRun {
		items += n
	}
	path := filepath.Join(t.TempDir(), name)
	w, err := NewWriter(path, pageSize, uint64(len(itemsPerRun)), items, pageValues[len(pageValues)-1])
	require.NoError(t, err)
	w.NoFsync()
	for _, n := range itemsPerRun {
		w.AddRun(n)
	}
	for _, v := range pageValues {
		w.AddPage(v)
	}
	require.NoError(t, w.Build())

	idx, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(idx.Close)
	return idx
}

func TestGet(t *testing.T) {
	itemsPerRun := []uint64{3, 1, 5, 2, 1}
	const pageSize = 2
	var items uint64
	for _, n := range itemsPerRun {
		items += n
	}
	pageValues := make([]uint64, (items+pageSize-1)/pageSize)
	for i := range pageValues {
		pageValues[i] = uint64(i) * 37 // monotone, uneven gaps
	}

	idx := build(t, "get", pageSize, itemsPerRun, pageValues)

	var ordinal uint64
	for run, n := range itemsPerRun {
		for item := range n {
			v, ok := idx.Get(uint64(run), item)
			require.True(t, ok, "run %d item %d", run, item)
			require.Equal(t, pageValues[ordinal/pageSize], v, "run %d item %d", run, item)
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
	require.False(t, ok, "run past the end")
	_, ok = idx.Get(0, 1<<40)
	require.False(t, ok, "item past the end")
}

// Run is the reverse of Get's first half: which run owns an ordinal.
func TestRun(t *testing.T) {
	// run 0 holds ordinals 0-2, run 1 holds 3, run 2 holds 4-5
	idx := build(t, "run", 2, []uint64{3, 1, 2}, []uint64{0, 37, 74})

	for ordinal, wantRun := range []uint64{0, 0, 0, 1, 2, 2} {
		r, ok := idx.Run(uint64(ordinal))
		require.True(t, ok, "ordinal %d", ordinal)
		require.Equal(t, wantRun, r, "ordinal %d", ordinal)
	}

	_, ok := idx.Run(6)
	require.False(t, ok, "one past the last item")
	_, ok = idx.Run(1 << 40)
	require.False(t, ok, "far past the last item")
}

// An empty run owns no ordinal, so the search has to skip past it.
func TestRunSkipsEmpty(t *testing.T) {
	// runs 1 and 3 are empty; ordinals 0-2 are run 0, ordinal 3 is run 2
	idx := build(t, "empty-runs", 2, []uint64{3, 0, 1, 0, 2}, []uint64{0, 37, 74})

	for ordinal, wantRun := range []uint64{0, 0, 0, 2, 4, 4} {
		r, ok := idx.Run(uint64(ordinal))
		require.True(t, ok, "ordinal %d", ordinal)
		require.Equal(t, wantRun, r, "ordinal %d", ordinal)
	}
}

// Page is the reverse of Get's second half: which page holds a value.
func TestPage(t *testing.T) {
	idx := build(t, "page", 2, []uint64{3, 1, 2}, []uint64{10, 37, 74})

	for _, tc := range []struct{ value, page uint64 }{
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
	_, ok = idx.Run(0)
	require.False(t, ok)
	_, ok = idx.Page(0)
	require.False(t, ok)
}

func TestPageSizeZeroRejected(t *testing.T) {
	_, err := NewWriter(filepath.Join(t.TempDir(), "bad"), 0, 1, 1, 1)
	require.Error(t, err)
}

// An item past its own run belongs to no run, even when the ordinal it lands on
// is a valid position in the next one.
func TestItemPastRunEnd(t *testing.T) {
	idx := build(t, "past-run", 2, []uint64{3, 1}, []uint64{0, 37})
	_, ok := idx.Get(0, 3) // ordinal 3 is run 1's only item
	require.False(t, ok)
	_, ok = idx.Get(1, 1)
	require.False(t, ok, "one past the last item of the last run")
}

// A truncated or forged file must be reported, not panicked on: openDirtyAccessor
// rebuilds an accessor it cannot open.
func TestOpenCorrupt(t *testing.T) {
	good := filepath.Join(t.TempDir(), "good")
	w, err := NewWriter(good, 2, 2, 4, 100)
	require.NoError(t, err)
	w.NoFsync()
	w.AddRun(3)
	w.AddRun(1)
	w.AddPage(0)
	w.AddPage(100)
	require.NoError(t, w.Build())
	full, err := os.ReadFile(good)
	require.NoError(t, err)

	for _, size := range []int{headerLen + 1, headerLen + 16, len(full) - 1} {
		path := filepath.Join(t.TempDir(), "trunc")
		require.NoError(t, os.WriteFile(path, full[:size], 0o644))
		idx, err := Open(path)
		require.Error(t, err, "size %d", size)
		require.Nil(t, idx, "size %d", size)
	}
}
