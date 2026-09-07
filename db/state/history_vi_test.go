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

package state

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistoryValueIndex(t *testing.T) {
	valuesPerKey := []uint64{3, 1, 5, 2, 1}
	const pageSize = 2
	var valueCount uint64
	for _, n := range valuesPerKey {
		valueCount += n
	}
	pageCount := (valueCount + pageSize - 1) / pageSize
	pageOffsets := make([]uint64, pageCount)
	for i := range pageOffsets {
		pageOffsets[i] = uint64(i) * 37 // .v is monotone, gaps vary
	}
	vSize := pageOffsets[pageCount-1] + 1

	path := filepath.Join(t.TempDir(), "test.vi")
	w, err := NewHistoryValueIndexWriter(path, pageSize, uint64(len(valuesPerKey)), valueCount, vSize)
	require.NoError(t, err)
	for _, n := range valuesPerKey {
		w.AddKey(n)
	}
	for _, off := range pageOffsets {
		w.AddPageOffset(off)
	}
	require.NoError(t, w.Build())

	idx, err := OpenHistoryValueIndex(path)
	require.NoError(t, err)
	defer idx.Close()

	require.Equal(t, uint64(pageSize), idx.PageSize())

	var ordinal uint64
	for keyOrdinal, n := range valuesPerKey {
		for rank := range n {
			require.Equal(t, pageOffsets[ordinal/pageSize], idx.Lookup(uint64(keyOrdinal), rank),
				"key %d rank %d (ordinal %d)", keyOrdinal, rank, ordinal)
			ordinal++
		}
	}
	require.Equal(t, valueCount, ordinal)
}

func TestHistoryValueIndexSinglePage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "one.vi")
	w, err := NewHistoryValueIndexWriter(path, 64, 1, 1, 1)
	require.NoError(t, err)
	w.AddKey(1)
	w.AddPageOffset(0)
	require.NoError(t, w.Build())

	idx, err := OpenHistoryValueIndex(path)
	require.NoError(t, err)
	defer idx.Close()
	require.Equal(t, uint64(0), idx.Lookup(0, 0))
}
