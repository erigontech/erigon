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

package commitment

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"
	"testing/synctest"
	"unsafe"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func randomTouchCases(seed int64, n, dupEvery int) []touchEntry {
	rnd := rand.New(rand.NewSource(seed))
	cases := make([]touchEntry, 0, n)
	for i := range n {
		hk := make([]byte, 64)
		for j := range hk {
			hk[j] = byte(rnd.Intn(16))
		}
		var pk []byte
		if i%7 != 0 {
			pk = []byte{byte(i), byte(i >> 8)}
		}
		var upd *Update
		if i%3 != 0 {
			upd = &Update{Flags: BalanceUpdate, Balance: *uint256.NewInt(uint64(i) + 1)}
		}
		cases = append(cases, touchEntry{hashedKey: hk, plainKey: pk, update: upd})
		if dupEvery > 0 && i%dupEvery == 0 {
			cases = append(cases, touchEntry{
				hashedKey: slices.Clone(hk),
				plainKey:  pk,
				update:    &Update{Flags: NonceUpdate, Nonce: uint64(i) + 1},
			})
		}
	}
	return cases
}

func buildReferenceTrie(cases []touchEntry) *prefixTrie {
	tr := newPrefixTrie()
	for _, c := range cases {
		tr.Insert(c.hashedKey, c.plainKey, c.update)
	}
	return tr
}

func collectAll(pu *parallelUpdate, cases []touchEntry) {
	for _, c := range cases {
		pu.Collect(c.hashedKey, c.plainKey, c.update)
	}
}

func buildChunked(cases []touchEntry) (*parallelUpdate, bool) {
	pu := newParallelUpdate()
	collectAll(pu, cases)
	backgrounded := pu.spare != nil
	pu.Build()
	return pu, backgrounded
}

func TestChunkBuild_WalkOrderIsSortedWhateverTheArrivalOrder(t *testing.T) {
	t.Parallel()

	cases := randomTouchCases(31337, 2*touchChunkKeys+3, 0)
	shuffled := slices.Clone(cases)
	rnd := rand.New(rand.NewSource(9))
	rnd.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	pu, backgrounded := buildChunked(shuffled)
	require.True(t, backgrounded, "the batch must cross a chunk boundary")
	walk := collectWalk(pu.trie)
	require.True(t, slices.IsSortedFunc(walk, func(a, b walkEntry) int { return bytes.Compare(a.prefix, b.prefix) }),
		"the walk must emit nodes in hashed-key order")

	var terminals, want [][]byte
	for _, e := range walk {
		if e.plainKey != nil {
			terminals = append(terminals, e.prefix)
		}
	}
	for _, c := range cases {
		if c.plainKey != nil {
			want = append(want, c.hashedKey)
		}
	}
	slices.SortFunc(want, bytes.Compare)
	require.Equal(t, want, terminals, "every collected key must appear once, in sorted order")
}

func TestChunkBuild_MatchesInsertionOrderTrie(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 2, 17, 5000, touchChunkKeys - 1, touchChunkKeys, touchChunkKeys + 1, 3*touchChunkKeys + 7} {
		cases := randomTouchCases(int64(n)*7919+3, n, 4)[:n]
		want := collectWalk(buildReferenceTrie(cases))
		pu, backgrounded := buildChunked(cases)
		require.Equal(t, n >= touchChunkKeys, backgrounded,
			"n=%d: hand-off must trigger exactly when the batch fills a chunk", n)
		require.Equal(t, want, collectWalk(pu.trie),
			"chunked build must match insertion-order build for n=%d", n)
	}
}

func TestChunkBuild_BuffersSurviveBatches(t *testing.T) {
	t.Parallel()

	cases := randomTouchCases(777, touchChunkKeys+8, 0)
	pu := newParallelUpdate()
	buffers := func() []uintptr {
		return []uintptr{uintptr(unsafe.Pointer(unsafe.SliceData(pu.pending))), uintptr(unsafe.Pointer(unsafe.SliceData(pu.spare)))}
	}
	var first []uintptr
	for batch := range 3 {
		pu.Reset()
		collectAll(pu, cases)
		pu.Build()
		require.EqualValues(t, len(cases), pu.trie.root.subtreeCount)
		if batch == 1 {
			first = buffers()
		}
	}
	require.GreaterOrEqual(t, cap(pu.pending), touchChunkKeys, "a recycled buffer must keep its capacity")
	require.GreaterOrEqual(t, cap(pu.spare), touchChunkKeys, "a recycled buffer must keep its capacity")
	require.ElementsMatch(t, first, buffers(), "later batches must reuse the same two buffers")
}

func TestChunkBuild_KeyTouchedInSeveralChunksKeepsArrivalOrder(t *testing.T) {
	t.Parallel()

	cases := randomTouchCases(41, 2*touchChunkKeys+5, 0)
	hk := bytes.Repeat([]byte{0x0a}, 64)
	for i, at := range []int{touchChunkKeys - 1, touchChunkKeys, 2*touchChunkKeys + 1} {
		cases[at] = touchEntry{hashedKey: hk, plainKey: []byte("pk"), update: &Update{Flags: BalanceUpdate, Balance: *uint256.NewInt(uint64(i) + 1)}}
	}
	want := collectWalk(buildReferenceTrie(cases))
	reordered := slices.Concat(cases[touchChunkKeys:], cases[:touchChunkKeys])
	require.NotEqual(t, want, collectWalk(buildReferenceTrie(reordered)), "the fixture must be sensitive to chunk order")

	pu, backgrounded := buildChunked(cases)
	require.True(t, backgrounded)
	require.Equal(t, want, collectWalk(pu.trie), "a key touched in several chunks must keep its last update")
}

func TestChunkBuild_TouchHashedKeyCopiesCallerBuffer(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()

	buf := make([]byte, 64)
	for v := byte(1); v <= 2; v++ {
		for i := range buf {
			buf[i] = v
		}
		ut.TouchHashedKey(buf)
	}

	require.Equal(t, uint64(2), ut.Size())
	ut.parallel.Build()
	require.EqualValues(t, 2, ut.parallel.trie.root.subtreeCount,
		"a reused caller buffer must not collapse two keys into one")
	require.Len(t, ut.parallel.trie.root.children, 2)
}

func TestChunkBuild_AbandonedCollectionLeavesNoGoroutine(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		collectAll(newParallelUpdate(), randomTouchCases(5, touchChunkKeys, 0))
		synctest.Wait()
	})
}
