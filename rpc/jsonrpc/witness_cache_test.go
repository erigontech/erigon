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

package jsonrpc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// mkResult builds a witness result whose resident size is exactly nBytes,
// carried as a single state node.
func mkResult(nBytes int) *ExecutionWitnessResult {
	return &ExecutionWitnessResult{State: []hexutil.Bytes{make([]byte, nBytes)}}
}

func hashN(n byte) common.Hash {
	var h common.Hash
	h[0] = n
	return h
}

func TestWitnessCacheGetHitAndHashMismatch(t *testing.T) {
	c := newWitnessCache(8, 1024)
	r := mkResult(10)
	c.put(5, hashN(0xaa), r)

	got, ok := c.get(5, hashN(0xaa))
	require.True(t, ok)
	require.Same(t, r, got)

	_, ok = c.get(5, hashN(0xbb))
	require.False(t, ok, "hash mismatch must miss")

	_, ok = c.get(6, hashN(0xaa))
	require.False(t, ok, "absent number must miss")
}

func TestNewWitnessCacheClampsBlocks(t *testing.T) {
	require.Equal(t, int(witnessCacheMaxBlocks), newWitnessCache(200, 1024).maxEntries,
		"a block count above the hard cap clamps to witnessCacheMaxBlocks")
	require.Equal(t, witnessCacheMaxBlocks, newWitnessCache(witnessCacheMaxBlocks, 1024).maxEntries,
		"a block count at the hard cap is kept")

	c := newWitnessCache(1, 1024)
	require.Equal(t, 1, c.maxEntries)
	c.put(1, hashN(1), mkResult(10))
	c.put(2, hashN(2), mkResult(10))
	require.Equal(t, 1, c.entryCount(), "a capacity-1 cache holds only the newest number")
	_, ok := c.get(1, hashN(1))
	require.False(t, ok, "lowest number evicted at capacity 1")
	_, ok = c.get(2, hashN(2))
	require.True(t, ok)
}

func TestWitnessCacheCountCapEviction(t *testing.T) {
	c := newWitnessCache(3, 1024)
	for n := uint64(1); n <= 4; n++ {
		c.put(n, hashN(byte(n)), mkResult(10))
	}

	require.Equal(t, 3, c.entryCount())
	_, ok := c.get(1, hashN(1))
	require.False(t, ok, "lowest number must be evicted first")
	for n := uint64(2); n <= 4; n++ {
		_, ok := c.get(n, hashN(byte(n)))
		require.True(t, ok, "num %d should be retained", n)
	}
	require.Equal(t, uint64(1), c.evictionCount())
}

func TestWitnessCacheByteCapEviction(t *testing.T) {
	c := newWitnessCache(96, 1024)
	c.maxBytes = 300

	for n := uint64(1); n <= 4; n++ {
		c.put(n, hashN(byte(n)), mkResult(100))
	}

	require.Equal(t, 3, c.entryCount())
	require.Equal(t, uint64(300), c.residentBytes())
	_, ok := c.get(1, hashN(1))
	require.False(t, ok, "lowest number evicted once byte cap binds")
}

func TestWitnessCacheByteCapKeepsSingleOversizeEntry(t *testing.T) {
	c := newWitnessCache(96, 1024)
	c.maxBytes = 300

	c.put(7, hashN(0x07), mkResult(500))

	require.Equal(t, 1, c.entryCount(), "a lone entry above the byte cap is still cached")
	require.Equal(t, uint64(500), c.residentBytes())
}

func TestWitnessCachePutReplacesSameNumber(t *testing.T) {
	c := newWitnessCache(8, 1024)
	c.put(9, hashN(0x01), mkResult(100))
	c.put(9, hashN(0x02), mkResult(40))

	require.Equal(t, 1, c.entryCount())
	require.Equal(t, uint64(40), c.residentBytes())
	_, ok := c.get(9, hashN(0x01))
	require.False(t, ok, "old hash gone after replacement")
	got, ok := c.get(9, hashN(0x02))
	require.True(t, ok)
	require.Equal(t, uint64(40), witnessResultBytes(got))
}

func TestWitnessCacheReconcileEvictsOrphanAndAboveHead(t *testing.T) {
	c := newWitnessCache(96, 1024)
	c.put(10, hashN(0xa0), mkResult(10))
	c.put(11, hashN(0xb0), mkResult(10))
	c.put(12, hashN(0xc0), mkResult(10))

	// New canonical chain: 10 unchanged, 11 replaced by a different hash, head is 11.
	c.reconcile([]headerRef{
		{num: 10, hash: hashN(0xa0)},
		{num: 11, hash: hashN(0xbf)},
	})

	require.Equal(t, 1, c.entryCount())
	_, ok := c.get(10, hashN(0xa0))
	require.True(t, ok, "matching hash survives")
	_, ok = c.get(11, hashN(0xb0))
	require.False(t, ok, "orphaned hash evicted")
	_, ok = c.get(12, hashN(0xc0))
	require.False(t, ok, "entry above the highest head dropped")
}

func TestWitnessCacheReconcileEmptyIsNoop(t *testing.T) {
	c := newWitnessCache(96, 1024)
	c.put(3, hashN(0x03), mkResult(10))
	c.reconcile(nil)
	require.Equal(t, 1, c.entryCount())
}

func TestWitnessCacheMetrics(t *testing.T) {
	evictBefore := witnessCacheEvictCounter.GetValueUint64()

	c := newWitnessCache(2, 1024)
	c.put(1, hashN(1), mkResult(10))
	c.put(2, hashN(2), mkResult(20))
	c.put(3, hashN(3), mkResult(30)) // count cap 2 evicts num 1

	require.Equal(t, uint64(1), witnessCacheEvictCounter.GetValueUint64()-evictBefore, "one eviction counted")
	require.Equal(t, c.residentBytes(), witnessCacheBytesResidentGauge.GetValueUint64(), "bytes gauge tracks resident bytes")
	require.Equal(t, uint64(c.entryCount()), witnessCacheEntriesResidentGauge.GetValueUint64(), "entries gauge tracks entry count")
}

func TestWitnessCacheConcurrentGetPut(t *testing.T) {
	c := newWitnessCache(16, 1024)
	const workers = 8
	const iters = 500

	var wg sync.WaitGroup
	wg.Add(workers * 2)
	for w := 0; w < workers; w++ {
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < iters; i++ {
				n := base*iters + i
				c.put(n, hashN(byte(n)), mkResult(64))
			}
		}(uint64(w))
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < iters; i++ {
				n := base*iters + i
				c.get(n, hashN(byte(n)))
				c.reconcile([]headerRef{{num: n, hash: hashN(byte(n))}})
			}
		}(uint64(w))
	}
	wg.Wait()

	require.LessOrEqual(t, c.entryCount(), 16)
}
