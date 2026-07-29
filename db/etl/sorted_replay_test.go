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

package etl

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestCanReplaySequentially(t *testing.T) {
	sr := func(first, last byte) *sortedRun {
		return &sortedRun{first: []byte{first}, last: []byte{last}, valid: true}
	}
	for _, tc := range []struct {
		name      string
		runs      []*sortedRun
		providers int
		want      bool
	}{
		{"empty", nil, 0, true},
		{"single", []*sortedRun{sr(1, 2)}, 1, true},
		{"single_unfilled", []*sortedRun{{}}, 1, false},
		{"single_nil", []*sortedRun{nil}, 1, false},
		{"disjoint", []*sortedRun{sr(1, 2), sr(3, 4)}, 2, true},
		{"equal_boundary", []*sortedRun{sr(1, 2), sr(2, 4)}, 2, true},
		{"overlap", []*sortedRun{sr(1, 3), sr(2, 4)}, 2, false},
		{"one_unfilled", []*sortedRun{sr(1, 2), {}}, 2, false},
		{"count_mismatch", []*sortedRun{sr(1, 2)}, 2, false},
		{"later_overlap", []*sortedRun{sr(1, 2), sr(3, 9), sr(4, 5)}, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, canReplaySequentially(tc.runs, tc.providers))
		})
	}
}

func collectMonotoneDups(t *testing.T, c *Collector, numKeys, valsPerKey int) {
	t.Helper()
	k := make([]byte, 8)
	v := make([]byte, 20)
	for i := range numKeys {
		binary.BigEndian.PutUint64(k, uint64(i))
		for j := range valsPerKey {
			// values under one key intentionally not in byte order (mimics ii indexKeys)
			binary.BigEndian.PutUint64(v, uint64(valsPerKey-j))
			require.NoError(t, c.Collect(k, v))
		}
	}
}

func TestCollectorRecordsRuns(t *testing.T) {
	t.Run("monotone_input_is_sequential", func(t *testing.T) {
		c := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(16*datasize.KB), log.New())
		defer c.Close()
		collectMonotoneDups(t, c, 1000, 3)
		require.Greater(t, len(c.dataProviders), 1)
		require.Len(t, c.runs, len(c.dataProviders))
		require.True(t, canReplaySequentially(c.runs, len(c.dataProviders)))
	})
	t.Run("overlapping_runs_are_not", func(t *testing.T) {
		c := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(16*datasize.KB), log.New())
		defer c.Close()
		k := make([]byte, 8)
		for i := range 3000 {
			binary.BigEndian.PutUint64(k, uint64(3000-i))
			require.NoError(t, c.Collect(k, []byte("value-of-some-len-20")))
		}
		require.Greater(t, len(c.dataProviders), 1)
		require.Len(t, c.runs, len(c.dataProviders))
		require.False(t, canReplaySequentially(c.runs, len(c.dataProviders)))
	})
}

func loadStream(t *testing.T, c *Collector) []sortableBufferEntry {
	t.Helper()
	var out []sortableBufferEntry
	require.NoError(t, c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		out = append(out, sortableBufferEntry{bytes.Clone(k), bytes.Clone(v)})
		return nil
	}, TransformArgs{}))
	return out
}

// stableSortByKey builds the reference Load output: global stable sort by key,
// which is what the heap merge over stably-sorted runs produces.
func stableSortByKey(pairs []sortableBufferEntry) []sortableBufferEntry {
	out := make([]sortableBufferEntry, len(pairs))
	copy(out, pairs)
	sort.SliceStable(out, func(i, j int) bool { return bytes.Compare(out[i].key, out[j].key) < 0 })
	return out
}

func TestSortedReplayEquivalence(t *testing.T) {
	const numKeys, valsPerKey = 1000, 3

	fast := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(16*datasize.KB), log.New())
	defer fast.Close()
	collectMonotoneDups(t, fast, numKeys, valsPerKey)
	require.Greater(t, len(fast.dataProviders), 1)
	require.True(t, canReplaySequentially(fast.runs, len(fast.dataProviders)))

	heap := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(16*datasize.KB), log.New())
	defer heap.Close()
	collectMonotoneDups(t, heap, numKeys, valsPerKey)
	heap.runs = nil // force the merge-heap path on identical data

	fastStream := loadStream(t, fast)
	heapStream := loadStream(t, heap)
	require.Len(t, fastStream, numKeys*valsPerKey)
	require.Equal(t, heapStream, fastStream)
}

func TestSortedReplayDupSortTable(t *testing.T) {
	const numKeys, valsPerKey = 500, 4
	putLoadFunc := func(k, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
		return next(k, k, v) // non-identity loadFunc forces cursor.Put, like ii indexKeys
	}

	t.Run("put_path", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)

		fast := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(16*datasize.KB), log.New())
		defer fast.Close()
		collectMonotoneDups(t, fast, numKeys, valsPerKey)
		require.True(t, canReplaySequentially(fast.runs, len(fast.dataProviders)))
		require.NoError(t, fast.Load(tx, kv.TblAccountHistoryKeys, putLoadFunc, TransformArgs{}))

		heap := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(16*datasize.KB), log.New())
		defer heap.Close()
		collectMonotoneDups(t, heap, numKeys, valsPerKey)
		heap.runs = nil
		require.NoError(t, heap.Load(tx, kv.TblStorageHistoryKeys, putLoadFunc, TransformArgs{}))

		require.Equal(t, dumpTable(t, tx, kv.TblStorageHistoryKeys), dumpTable(t, tx, kv.TblAccountHistoryKeys))
	})

	t.Run("append_dup_path", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		c := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(16*datasize.KB), log.New())
		defer c.Close()
		k := make([]byte, 8)
		v := make([]byte, 20)
		var expected []sortableBufferEntry
		for i := range numKeys {
			binary.BigEndian.PutUint64(k, uint64(i))
			for j := range valsPerKey {
				binary.BigEndian.PutUint64(v, uint64(j)) // ascending dup values: AppendDup-compatible
				require.NoError(t, c.Collect(k, v))
				expected = append(expected, sortableBufferEntry{bytes.Clone(k), bytes.Clone(v)})
			}
		}
		require.True(t, canReplaySequentially(c.runs, len(c.dataProviders)))
		require.NoError(t, c.Load(tx, kv.TblAccountHistoryKeys, IdentityLoadFunc, TransformArgs{}))
		require.Equal(t, expected, dumpTable(t, tx, kv.TblAccountHistoryKeys))
	})
}

func dumpTable(t *testing.T, tx kv.Tx, table string) []sortableBufferEntry {
	t.Helper()
	var out []sortableBufferEntry
	require.NoError(t, tx.ForEach(table, nil, func(k, v []byte) error {
		out = append(out, sortableBufferEntry{bytes.Clone(k), bytes.Clone(v)})
		return nil
	}))
	return out
}

// TestSortednessBreaksLoadStaysCorrect pins correctness when the producer breaks
// key order: whichever driver the run boundaries select, the loaded stream must
// equal the stable global sort. Intra-run breaks may still yield non-overlapping
// runs (sequential replay, legally); overlapping runs take the heap.
func TestSortednessBreaksLoadStaysCorrect(t *testing.T) {
	const n = 3000
	for _, tc := range []struct {
		name    string
		breakAt []int
	}{
		{"second_put", []int{1}},
		{"mid_first_run", []int{150}},
		{"near_run_boundary", []int{370, 371}},
		{"scattered", []int{7, 900, 2100, n - 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(16*datasize.KB), log.New())
			defer c.Close()

			var input []sortableBufferEntry
			k := make([]byte, 8)
			v := make([]byte, 20)
			for i := range n {
				key := uint64(i + 1000)
				for _, b := range tc.breakAt {
					if i == b {
						key = uint64(i / 2) // strictly smaller than the previous key
					}
				}
				binary.BigEndian.PutUint64(k, key)
				binary.BigEndian.PutUint64(v, uint64(i))
				require.NoError(t, c.Collect(k, v))
				input = append(input, sortableBufferEntry{bytes.Clone(k), bytes.Clone(v)})
			}

			require.Greater(t, len(c.dataProviders), 1)
			require.Equal(t, stableSortByKey(input), loadStream(t, c))
		})
	}
}
