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
	require.Equal(t, uint64(pageSize), idx.PageSize())
	require.True(t, idx.HasGroups())
	require.Equal(t, uint64(len(itemsPerGroup)), idx.GroupCount())

	var ordinal uint64
	for group, n := range itemsPerGroup {
		for member := range n {
			require.Equal(t, ordinal, idx.Ordinal(uint64(group), member), "group %d member %d", group, member)
			require.Equal(t, values[ordinal/pageSize], idx.Get(uint64(group), member), "group %d member %d", group, member)
			ordinal++
		}
	}
	require.Equal(t, items, ordinal)
}

func TestUngrouped(t *testing.T) {
	const pageSize = 4
	values := []uint64{0, 10, 40, 41}
	path := filepath.Join(t.TempDir(), "flat")
	w, err := NewWriter(path, pageSize, 0, uint64(len(values))*pageSize, values[len(values)-1])
	require.NoError(t, err)
	w.NoFsync()
	for _, v := range values {
		w.AddPage(v)
	}
	require.NoError(t, w.Build())

	idx, err := Open(path)
	require.NoError(t, err)
	defer idx.Close()

	require.False(t, idx.HasGroups())
	require.Zero(t, idx.GroupCount())
	for ordinal := range uint64(len(values)) * pageSize {
		require.Equal(t, values[ordinal/pageSize], idx.Value(ordinal), "ordinal %d", ordinal)
	}
}

func TestSingleItem(t *testing.T) {
	idx := build(t, "one", 64, []uint64{1}, []uint64{0})
	require.Equal(t, uint64(0), idx.Get(0, 0))
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
}

func TestPageSizeZeroRejected(t *testing.T) {
	_, err := NewWriter(filepath.Join(t.TempDir(), "bad"), 0, 1, 1, 1)
	require.Error(t, err)
}
