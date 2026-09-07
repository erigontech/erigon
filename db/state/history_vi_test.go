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
	"github.com/erigontech/erigon/db/datastruct/pagedidx"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/version"
)

// A v2 .vi is addressed by position: the key's ordinal in .ef and the rank of
// its txNum in that key's list.
func TestHistoryValueIndexV2(t *testing.T) {
	valuesPerKey := []uint64{3, 1, 2}
	const pageSize = 2
	offsets := []uint64{0, 40, 90} // one per page of 2 values

	path := filepath.Join(t.TempDir(), "v2.vi")
	w, err := pagedidx.NewWriter(path, pageSize, uint64(len(valuesPerKey)), 6, offsets[len(offsets)-1])
	require.NoError(t, err)
	w.NoFsync()
	for _, n := range valuesPerKey {
		w.AddRun(n)
	}
	for _, off := range offsets {
		w.AddPage(off)
	}
	require.NoError(t, w.Build())

	idx, err := OpenHistoryValueIndex(path, version.V2_0)
	require.NoError(t, err)
	defer idx.Close()
	require.False(t, idx.Empty())

	var ordinal uint64
	for keyOrdinal, n := range valuesPerKey {
		for rank := range n {
			// txNum and key are ignored by a v2 index
			off, ok := idx.Lookup(uint64(keyOrdinal), rank, 0, nil)
			require.True(t, ok)
			require.Equal(t, offsets[ordinal/pageSize], off, "key %d rank %d", keyOrdinal, rank)
			ordinal++
		}
	}
}

// A v1 .vi is a perfect hash over txNum+key. Read-only consumers cannot rebuild
// accessors, so a datadir holding them has to keep working.
func TestHistoryValueIndexV1(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "v1.vi")
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

	for i, k := range keys {
		// the position arguments are ignored by a v1 index
		off, ok := idx.Lookup(0, 0, txNums[i], k)
		require.True(t, ok)
		require.Equal(t, uint64(i)*100, off)
	}
}
