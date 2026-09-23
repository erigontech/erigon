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
	"math/bits"
	"math/rand"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

type flatNode struct {
	path     string
	plainKey string
	update   string
	subtree  uint32
}

func flattenPrefixTrie(t *testing.T, tr *prefixTrie) []flatNode {
	t.Helper()
	var out []flatNode
	var walk func(n *prefixNode, path []byte)
	walk = func(n *prefixNode, path []byte) {
		path = append(path, n.ext...)
		rec := flatNode{path: NibblesToString(path), subtree: n.subtreeCount}
		if n.plainKey != nil {
			rec.plainKey = string(n.plainKey)
		}
		if n.update != nil {
			rec.update = n.update.String()
		}
		out = append(out, rec)
		for i, c := range n.children {
			nib := nthSetNibble(n.bitmap, i)
			walk(c, append(slices.Clone(path), nib))
		}
	}
	walk(tr.root, nil)
	return out
}

func nthSetNibble(bitmap uint16, n int) byte {
	for range n {
		bitmap &= bitmap - 1
	}
	return byte(bits.TrailingZeros16(bitmap))
}

type touchCase struct {
	hashedKey []byte
	plainKey  []byte
	update    *Update
}

func randomTouchCases(seed int64, n, dupEvery int) []touchCase {
	rnd := rand.New(rand.NewSource(seed))
	cases := make([]touchCase, 0, n)
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
		cases = append(cases, touchCase{hashedKey: hk, plainKey: pk, update: upd})
		if dupEvery > 0 && i%dupEvery == 0 {
			cases = append(cases, touchCase{
				hashedKey: slices.Clone(hk),
				plainKey:  pk,
				update:    &Update{Flags: NonceUpdate, Nonce: uint64(i) + 1},
			})
		}
	}
	return cases
}

func buildReferenceTrie(cases []touchCase) *prefixTrie {
	tr := newPrefixTrie()
	for _, c := range cases {
		tr.Insert(c.hashedKey, c.plainKey, c.update)
	}
	return tr
}

func buildChunked(cases []touchCase) (*parallelUpdate, bool) {
	pu := newParallelUpdate()
	for _, c := range cases {
		pu.Collect(c.hashedKey, c.plainKey, c.update)
	}
	backgrounded := pu.buildCh != nil
	pu.Build()
	return pu, backgrounded
}

func TestFlattenPrefixTrie_Paths(t *testing.T) {
	t.Parallel()

	tr := newPrefixTrie()
	for _, hk := range [][]byte{nibs(1, 2, 3), nibs(1, 2, 0xf), nibs(5)} {
		tr.Insert(hk, hk, nil)
	}

	var got []string
	for _, n := range flattenPrefixTrie(t, tr) {
		got = append(got, n.path)
	}
	require.Equal(t, []string{"", "12", "123", "12f", "5"}, got)
}

func TestChunkBuild_WalkOrderIsSortedWhateverTheArrivalOrder(t *testing.T) {
	t.Parallel()

	cases := randomTouchCases(31337, 2000, 0)
	shuffled := slices.Clone(cases)
	rnd := rand.New(rand.NewSource(9))
	rnd.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	pu, _ := buildChunked(shuffled)
	flat := flattenPrefixTrie(t, pu.trie)

	paths := make([]string, 0, len(flat))
	terminals := make([]string, 0, len(cases))
	for _, n := range flat {
		paths = append(paths, n.path)
		if n.plainKey != "" {
			terminals = append(terminals, n.path)
		}
	}
	require.True(t, slices.IsSorted(paths), "the walk must emit nodes in hashed-key order")

	want := make([]string, 0, len(cases))
	for _, c := range cases {
		if c.plainKey != nil {
			want = append(want, NibblesToString(c.hashedKey))
		}
	}
	slices.Sort(want)
	require.Equal(t, want, terminals, "every collected key must appear once, in sorted order")
}

func TestChunkBuild_MatchesInsertionOrderTrie(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 2, 17, 5000, touchChunkKeys + 1, 3*touchChunkKeys + 7} {
		cases := randomTouchCases(int64(n)*7919+3, n, 4)
		want := flattenPrefixTrie(t, buildReferenceTrie(cases))
		pu, backgrounded := buildChunked(cases)
		require.Equal(t, len(cases) > touchChunkKeys, backgrounded,
			"n=%d: hand-off must trigger exactly when the batch outgrows one chunk", n)
		require.Nil(t, pu.buildCh, "Build must reap the builder goroutine")
		require.Equal(t, want, flattenPrefixTrie(t, pu.trie),
			"chunked build must match insertion-order build for n=%d", n)
	}
}

func TestChunkBuild_BuffersSurviveBatches(t *testing.T) {
	t.Parallel()

	cases := randomTouchCases(777, touchChunkKeys+8, 0)
	pu, backgrounded := buildChunked(cases)
	require.True(t, backgrounded)
	require.Len(t, pu.freeCh, touchChunkBuffers, "every chunk buffer must be back in the free list")

	pu.Reset()
	for _, c := range cases {
		pu.Collect(c.hashedKey, c.plainKey, nil)
	}
	pu.Build()
	require.Len(t, pu.freeCh, touchChunkBuffers, "a second batch must reuse the same buffers")
	for range touchChunkBuffers {
		require.GreaterOrEqual(t, cap(<-pu.freeCh), touchChunkKeys,
			"a recycled buffer must keep its capacity, or every chunk re-grows it")
	}
	require.EqualValues(t, len(cases), pu.trie.root.subtreeCount)
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
