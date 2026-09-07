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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/version"
)

func mustLookup(t *testing.T, idx *HistoryValueIndex, keyOrdinal, rank uint64) uint64 {
	t.Helper()
	off, ok := idx.Lookup(keyOrdinal, rank, 0, nil)
	require.True(t, ok)
	return off
}

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

	idx, err := OpenHistoryValueIndex(path, version.V2_0)
	require.NoError(t, err)
	defer idx.Close()

	require.Equal(t, uint64(pageSize), idx.PageSize())

	var ordinal uint64
	for keyOrdinal, n := range valuesPerKey {
		for rank := range n {
			require.Equal(t, pageOffsets[ordinal/pageSize], mustLookup(t, idx, uint64(keyOrdinal), rank),
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

	idx, err := OpenHistoryValueIndex(path, version.V2_0)
	require.NoError(t, err)
	defer idx.Close()
	require.Equal(t, uint64(0), mustLookup(t, idx, 0, 0))
}

// A v1 .vi is a perfect hash over txNum+key. Read-only consumers cannot rebuild
// accessors, so a datadir holding them has to keep working.
func TestHistoryValueIndexLegacy(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "legacy.vi")
	keys := [][]byte{[]byte("aaa"), []byte("bbb"), []byte("ccc")}
	txNums := []uint64{5, 9, 11}

	salt := uint32(1)
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount: len(keys), BucketSize: 10, Salt: &salt, TmpDir: tmpDir,
		IndexFile: path, LeafSize: 8, NoFsync: true,
	}, log.New())
	require.NoError(t, err)
	defer rs.Close()
	for i, k := range keys {
		require.NoError(t, rs.AddKey(historyKey(txNums[i], k, nil), uint64(i)*100))
	}
	require.NoError(t, rs.Build(t.Context()))

	idx, err := OpenHistoryValueIndex(path, version.V1_0)
	require.NoError(t, err)
	defer idx.Close()
	require.False(t, idx.Empty())
	require.Equal(t, uint64(len(keys)), idx.KeyCount())

	for i, k := range keys {
		// the position arguments are ignored by a v1 index
		off, ok := idx.Lookup(0, 0, txNums[i], k)
		require.True(t, ok)
		require.Equal(t, uint64(i)*100, off)
	}
}
