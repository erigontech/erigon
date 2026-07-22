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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func hashN(n byte) common.Hash {
	var h common.Hash
	h[0] = n
	return h
}

func mkResult() *ExecutionWitnessResult {
	return &ExecutionWitnessResult{State: []hexutil.Bytes{{0x01}}}
}

// mkSized returns a cached-shell result whose resident cost (len(cachedJSON)) is n.
func mkSized(n int) *ExecutionWitnessResult {
	return &ExecutionWitnessResult{cachedJSON: make([]byte, n)}
}

// TestNewWitnessResultCacheClampsBlocks pins the only behaviour the constructor
// owns on top of the hashicorp LRU: clamp above witnessCacheMaxBlocks, honor
// requested sizes below it.
func TestNewWitnessResultCacheClampsBlocks(t *testing.T) {
	c := newWitnessResultCache(witnessCacheMaxBlocks+100, 0, false, false)
	for n := 0; n < int(witnessCacheMaxBlocks)+100; n++ {
		var h common.Hash
		h[0], h[1] = byte(n), byte(n>>8)
		c.Add(h, mkResult())
	}
	require.Equal(t, int(witnessCacheMaxBlocks), c.Len(),
		"entry count is clamped to witnessCacheMaxBlocks")

	c1 := newWitnessResultCache(1, 0, false, false)
	c1.Add(hashN(1), mkResult())
	c1.Add(hashN(2), mkResult())
	require.Equal(t, 1, c1.Len(), "a sub-cap size is honored, not clamped up")
}

// TestWitnessResultCacheModeFieldsRoundTrip pins that the serving-mode flags set at
// construction are exactly what the accessors report back.
func TestWitnessResultCacheModeFieldsRoundTrip(t *testing.T) {
	on := newWitnessResultCache(4, 0, true, true)
	require.True(t, on.HeadCapture())
	require.True(t, on.CacheOnly())

	off := newWitnessResultCache(4, 0, false, false)
	require.False(t, off.HeadCapture())
	require.False(t, off.CacheOnly())
}

// TestWitnessResultCacheCountCapOnly: with maxBytes==0 only the count cap binds,
// evicting oldest-out regardless of resident bytes.
func TestWitnessResultCacheCountCapOnly(t *testing.T) {
	c := newWitnessResultCache(2, 0, false, false)
	c.Add(hashN(1), mkSized(1000))
	c.Add(hashN(2), mkSized(1000))
	c.Add(hashN(3), mkSized(1000))

	require.Equal(t, 2, c.Len(), "count cap of 2 binds")
	require.False(t, c.Contains(hashN(1)), "oldest is evicted first")
	require.True(t, c.Contains(hashN(2)))
	require.True(t, c.Contains(hashN(3)))
	require.Equal(t, 2000, c.ResidentBytes(), "resident bytes track the two surviving entries")
}

// TestWitnessResultCacheByteCapEvicts: the byte cap evicts oldest-out even while the
// count cap still has room.
func TestWitnessResultCacheByteCapEvicts(t *testing.T) {
	c := newWitnessResultCache(10, 300, false, false)
	c.Add(hashN(1), mkSized(100))
	c.Add(hashN(2), mkSized(100))
	c.Add(hashN(3), mkSized(100)) // resident == cap, no eviction yet
	require.Equal(t, 3, c.Len())
	require.Equal(t, 300, c.ResidentBytes())

	c.Add(hashN(4), mkSized(100)) // 400 > 300 → evict oldest back under cap

	require.Equal(t, 3, c.Len(), "byte cap binds before the count cap of 10")
	require.False(t, c.Contains(hashN(1)), "oldest is evicted first")
	require.True(t, c.Contains(hashN(2)))
	require.True(t, c.Contains(hashN(3)))
	require.True(t, c.Contains(hashN(4)))
	require.Equal(t, 300, c.ResidentBytes())
}

// TestWitnessResultCacheReAddRefreshesRecency: re-adding a key makes it most-recent,
// so a subsequent count-cap eviction drops a different (older) entry.
func TestWitnessResultCacheReAddRefreshesRecency(t *testing.T) {
	c := newWitnessResultCache(2, 0, false, false)
	c.Add(hashN(1), mkSized(100))
	c.Add(hashN(2), mkSized(100))
	c.Add(hashN(1), mkSized(100)) // hashN(1) now most-recent, hashN(2) least-recent
	c.Add(hashN(3), mkSized(100)) // count cap → evict least-recent (hashN(2))

	require.Equal(t, 2, c.Len())
	require.True(t, c.Contains(hashN(1)), "refreshed key survives")
	require.False(t, c.Contains(hashN(2)), "least-recent key is evicted")
	require.True(t, c.Contains(hashN(3)))
	require.Equal(t, 200, c.ResidentBytes(), "re-add does not double-count bytes")
}

// TestWitnessResultCacheSingleOversizedEntryKept: an entry alone larger than the byte
// cap is retained (the cache never evicts its last entry down to empty).
func TestWitnessResultCacheSingleOversizedEntryKept(t *testing.T) {
	c := newWitnessResultCache(10, 300, false, false)
	c.Add(hashN(1), mkSized(1000))
	require.Equal(t, 1, c.Len())
	require.True(t, c.Contains(hashN(1)))
	require.Equal(t, 1000, c.ResidentBytes())
}
