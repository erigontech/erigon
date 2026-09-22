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
	seen := 0
	for i := range 16 {
		if bitmap&(1<<i) == 0 {
			continue
		}
		if seen == n {
			return byte(i)
		}
		seen++
	}
	panic("bitmap has fewer set bits than requested")
}

type presortCase struct {
	hashedKey []byte
	plainKey  []byte
	update    *Update
}

func randomPresortCases(seed int64, n, dupEvery int) []presortCase {
	rnd := rand.New(rand.NewSource(seed))
	cases := make([]presortCase, 0, n)
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
		cases = append(cases, presortCase{hashedKey: hk, plainKey: pk, update: upd})
		if dupEvery > 0 && i%dupEvery == 0 {
			cases = append(cases, presortCase{
				hashedKey: slices.Clone(hk),
				plainKey:  pk,
				update:    &Update{Flags: NonceUpdate, Nonce: uint64(i) + 1},
			})
		}
	}
	return cases
}

func buildReferenceTrie(cases []presortCase) *prefixTrie {
	tr := newPrefixTrie()
	for _, c := range cases {
		var upd *Update
		if c.update != nil {
			cp := *c.update
			upd = &cp
		}
		tr.Insert(c.hashedKey, c.plainKey, upd)
	}
	return tr
}

func buildPresortedTrie(cases []presortCase, memLimit int) *prefixTrie {
	pu := newParallelUpdate()
	if memLimit > 0 {
		pu.memLimit = memLimit
	}
	for _, c := range cases {
		var upd *Update
		if c.update != nil {
			cp := *c.update
			upd = &cp
		}
		pu.Collect(c.hashedKey, c.plainKey, upd)
	}
	pu.Build()
	return pu.trie
}

func TestPresort_MatchesInsertionOrderTrie(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 2, 17, 5000} {
		cases := randomPresortCases(int64(n)*7919+3, n, 4)
		want := flattenPrefixTrie(t, buildReferenceTrie(cases))
		got := flattenPrefixTrie(t, buildPresortedTrie(cases, 0))
		require.Equal(t, want, got, "presorted build must match insertion-order build for n=%d", n)
	}
}

func TestPresort_MidBatchFlushKeepsMergeOrder(t *testing.T) {
	t.Parallel()

	cases := randomPresortCases(4242, 3000, 3)
	want := flattenPrefixTrie(t, buildReferenceTrie(cases))
	got := flattenPrefixTrie(t, buildPresortedTrie(cases, 8*presortEntrySize))
	require.Equal(t, want, got, "a memory-limited flush must not change the merge order")
}

func TestPresort_BucketOfIsOrderPreserving(t *testing.T) {
	t.Parallel()

	prev := -1
	for hi := range 16 {
		require.Equal(t, hi<<4, presortBucketOf([]byte{byte(hi)}), "a one-nibble key sorts before its extensions")
		for lo := range 16 {
			b := presortBucketOf([]byte{byte(hi), byte(lo), 0x0a})
			require.Greater(t, b, prev, "bucket index must rise with the first two nibbles")
			prev = b
		}
	}
	require.Equal(t, 0, presortBucketOf(nil))
	require.Equal(t, presortBuckets-1, prev)
}
