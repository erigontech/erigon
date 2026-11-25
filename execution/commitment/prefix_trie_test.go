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
	"errors"
	"math/bits"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type walkEntry struct {
	prefix       []byte
	subtreeCount uint32
	bitmap       uint16
	plainKey     []byte
	update       *Update
}

func collectWalk(t *prefixTrie) []walkEntry {
	var out []walkEntry
	var dfs func(n *PrefixNode, accPrefix []byte)
	dfs = func(n *PrefixNode, accPrefix []byte) {
		if n == nil {
			return
		}
		prefix := make([]byte, len(accPrefix)+len(n.Ext))
		copy(prefix, accPrefix)
		copy(prefix[len(accPrefix):], n.Ext)
		out = append(out, walkEntry{prefix: prefix, subtreeCount: n.SubtreeCount, bitmap: n.Bitmap, plainKey: n.PlainKey, update: n.Update})
		childIdx := 0
		for bm := n.Bitmap; bm != 0; {
			nib := byte(bits.TrailingZeros16(bm))
			childPrefix := make([]byte, len(prefix)+1)
			copy(childPrefix, prefix)
			childPrefix[len(prefix)] = nib
			dfs(n.Children[childIdx], childPrefix)
			childIdx++
			bm &^= uint16(1) << nib
		}
	}
	dfs(t.Root, nil)
	return out
}

func TestPrefixTrieEmpty(t *testing.T) {
	tr := newPrefixTrie()
	require.NotNil(t, tr.Root)
	assert.Equal(t, 1, tr.arena.nodeCount())
	assert.Equal(t, uint32(0), tr.Root.SubtreeCount)
	assert.Equal(t, uint16(0), tr.Root.Bitmap)
	assert.Empty(t, tr.Root.Children)
	assert.Empty(t, tr.Root.Ext)
}

func TestPrefixTrieSingleInsert(t *testing.T) {
	tr := newPrefixTrie()
	key := nibs(0x01, 0x02, 0x03, 0x04)
	tr.Insert(key, nil, nil)

	assert.Equal(t, 2, tr.arena.nodeCount())
	assert.Equal(t, uint32(1), tr.Root.SubtreeCount)
	assert.Equal(t, uint16(1)<<0x01, tr.Root.Bitmap)
	require.Len(t, tr.Root.Children, 1)

	leaf := tr.Root.Children[0]
	assert.Equal(t, uint32(1), leaf.SubtreeCount)
	assert.Equal(t, nibs(0x02, 0x03, 0x04), leaf.Ext)
	assert.Equal(t, uint16(0), leaf.Bitmap)
}

func TestPrefixTrieTwoInsertsDivergeAtRoot(t *testing.T) {
	t.Run("symmetric", func(t *testing.T) {
		tr := newPrefixTrie()
		tr.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
		tr.Insert(nibs(0x05, 0x06, 0x07), nil, nil)

		assert.Equal(t, 3, tr.arena.nodeCount())
		assert.Equal(t, uint32(2), tr.Root.SubtreeCount)
		assert.Equal(t, uint16(1)<<0x01|uint16(1)<<0x05, tr.Root.Bitmap)
		require.Len(t, tr.Root.Children, 2)

		assert.Equal(t, nibs(0x02, 0x03), tr.Root.Children[0].Ext)
		assert.Equal(t, uint32(1), tr.Root.Children[0].SubtreeCount)
		assert.Equal(t, nibs(0x06, 0x07), tr.Root.Children[1].Ext)
		assert.Equal(t, uint32(1), tr.Root.Children[1].SubtreeCount)
	})

	t.Run("asymmetricCounts", func(t *testing.T) {
		tr := newPrefixTrie()
		for _, suf := range [][]byte{
			{0x02, 0x00}, {0x02, 0x01}, {0x02, 0x02}, {0x02, 0x03},
		} {
			tr.Insert(append([]byte{0x01}, suf...), nil, nil)
		}
		for _, suf := range [][]byte{
			{0x06, 0x00}, {0x06, 0x01}, {0x06, 0x02},
		} {
			tr.Insert(append([]byte{0x05}, suf...), nil, nil)
		}

		assert.Equal(t, uint32(7), tr.Root.SubtreeCount)
		require.Len(t, tr.Root.Children, 2)
		assert.Equal(t, uint32(4), tr.Root.Children[0].SubtreeCount, "left subtree")
		assert.Equal(t, uint32(3), tr.Root.Children[1].SubtreeCount, "right subtree")
	})
}

func TestPrefixTrieDivergenceInsideExtension(t *testing.T) {
	t.Run("inside", func(t *testing.T) {
		tr := newPrefixTrie()
		tr.Insert(nibs(0x01, 0x02, 0x03, 0x04, 0x05), nil, nil)
		tr.Insert(nibs(0x01, 0x02, 0x03, 0x06, 0x07), nil, nil)

		assert.Equal(t, 4, tr.arena.nodeCount())

		require.Len(t, tr.Root.Children, 1)
		mid := tr.Root.Children[0]
		assert.Equal(t, nibs(0x02, 0x03), mid.Ext)
		assert.Equal(t, uint32(2), mid.SubtreeCount)
		assert.Equal(t, uint16(1)<<0x04|uint16(1)<<0x06, mid.Bitmap)
		require.Len(t, mid.Children, 2)

		assert.Equal(t, nibs(0x05), mid.Children[0].Ext)
		assert.Equal(t, uint32(1), mid.Children[0].SubtreeCount)
		assert.Equal(t, nibs(0x07), mid.Children[1].Ext)
		assert.Equal(t, uint32(1), mid.Children[1].SubtreeCount)
	})

	t.Run("atEndOfExtension", func(t *testing.T) {
		tr := newPrefixTrie()
		tr.Insert(nibs(0x01, 0x02, 0x03, 0x04, 0x05), nil, nil)
		tr.Insert(nibs(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07), nil, nil)

		assert.Equal(t, 3, tr.arena.nodeCount())

		require.Len(t, tr.Root.Children, 1)
		mid := tr.Root.Children[0]
		assert.Equal(t, nibs(0x02, 0x03, 0x04, 0x05), mid.Ext)
		assert.Equal(t, uint32(2), mid.SubtreeCount)
		assert.Equal(t, uint16(1)<<0x06, mid.Bitmap)
		require.Len(t, mid.Children, 1)

		leaf := mid.Children[0]
		assert.Equal(t, nibs(0x07), leaf.Ext)
		assert.Equal(t, uint32(1), leaf.SubtreeCount)
	})
}

func TestPrefixTrieDuplicateInsert(t *testing.T) {
	tr := newPrefixTrie()
	key := nibs(0x01, 0x02, 0x03, 0x04)
	tr.Insert(key, nil, nil)
	nodesAfterFirst := tr.arena.nodeCount()

	tr.Insert(key, nil, nil)
	tr.Insert(key, nil, nil)

	assert.Equal(t, nodesAfterFirst, tr.arena.nodeCount(), "duplicate inserts must not grow the trie")
	assert.Equal(t, uint32(3), tr.Root.SubtreeCount, "subtreeCount must reflect path traversals")
	require.Len(t, tr.Root.Children, 1)
	assert.Equal(t, uint32(3), tr.Root.Children[0].SubtreeCount)
}

func TestPrefixTrieDeepInsert(t *testing.T) {
	tr := newPrefixTrie()
	deep := make([]byte, 128)
	for i := range deep {
		deep[i] = byte(i % 16)
	}
	tr.Insert(deep, nil, nil)

	assert.Equal(t, 2, tr.arena.nodeCount())
	assert.Equal(t, uint32(1), tr.Root.SubtreeCount)
	require.Len(t, tr.Root.Children, 1)
	assert.Equal(t, 127, len(tr.Root.Children[0].Ext), "leaf ext = remainder after consuming first nibble")
	assert.True(t, bytes.Equal(tr.Root.Children[0].Ext, deep[1:]))
}

func TestPrefixTrieSubtreeCountAccumulation(t *testing.T) {
	tr := newPrefixTrie()
	const N = 50
	for i := range N {
		k := []byte{0x01, 0x02, 0x03, byte(i & 0x0F), byte((i >> 4) & 0x0F), byte(i & 0x0F)}
		tr.Insert(k, nil, nil)
	}
	assert.Equal(t, uint32(N), tr.Root.SubtreeCount)

	require.Len(t, tr.Root.Children, 1)
	assert.Equal(t, uint32(N), tr.Root.Children[0].SubtreeCount)
}

func TestPrefixTrieArenaReuse(t *testing.T) {
	tr := newPrefixTrie()
	tr.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	tr.Insert(nibs(0x01, 0x02, 0x04), nil, nil)
	tr.Insert(nibs(0x05, 0x06, 0x07), nil, nil)
	first := tr.arena.nodeCount()

	tr.Reset()
	assert.Equal(t, 1, tr.arena.nodeCount(), "Reset must leave only the fresh root")
	assert.Equal(t, uint32(0), tr.Root.SubtreeCount)
	assert.Equal(t, uint16(0), tr.Root.Bitmap)
	assert.Empty(t, tr.Root.Children)

	tr.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	tr.Insert(nibs(0x01, 0x02, 0x04), nil, nil)
	tr.Insert(nibs(0x05, 0x06, 0x07), nil, nil)
	assert.Equal(t, first, tr.arena.nodeCount())
}

func TestPrefixArenaAllocExt(t *testing.T) {
	t.Run("capEqualsLen", func(t *testing.T) {
		a := newPrefixArena()
		ext := a.allocExt([]byte{1, 2, 3, 4, 5})
		assert.Equal(t, []byte{1, 2, 3, 4, 5}, ext)
		assert.Equal(t, len(ext), cap(ext), "allocExt must not hand out spare capacity")
	})

	t.Run("emptyInputReturnsNil", func(t *testing.T) {
		a := newPrefixArena()
		assert.Nil(t, a.allocExt(nil))
		assert.Nil(t, a.allocExt([]byte{}))
	})

	t.Run("appendPastCapDoesNotAliasNextExtension", func(t *testing.T) {
		a := newPrefixArena()
		first := a.allocExt([]byte{1, 2, 3})
		second := a.allocExt([]byte{4, 5, 6})

		first = append(first, 0xFF, 0xFF, 0xFF, 0xFF)

		assert.Equal(t, []byte{4, 5, 6}, second, "growing one extension past its cap must not corrupt the next")
		assert.Equal(t, []byte{1, 2, 3, 0xFF, 0xFF, 0xFF, 0xFF}, first)
	})
}

func TestPrefixTrieExtSurvivesChunkBoundary(t *testing.T) {
	tr := newPrefixTrie()

	const keyLen = 32
	const total = 2 * prefixExtChunkMax / keyLen
	want := make(map[string]bool, total)
	for i := range total {
		k := make([]byte, keyLen)
		v := i
		for j := range 4 {
			k[j] = byte(v % 16)
			v /= 16
		}
		want[string(k)] = true
		tr.Insert(k, nil, nil)
	}
	require.Greater(t, len(tr.arena.extChunks), 1, "test must actually cross a chunk boundary")

	got := make(map[string]bool, total)
	for _, e := range collectWalk(tr) {
		if len(e.prefix) == keyLen {
			got[string(e.prefix)] = true
		}
	}
	assert.Equal(t, want, got, "leaf extensions must reproduce their original key bytes after crossing a chunk boundary")
}

func TestPrefixTrieArenaReusesExtChunkBacking(t *testing.T) {
	tr := newPrefixTrie()
	tr.Insert(nibs(0x01, 0x02, 0x03, 0x04), nil, nil)

	chunk := tr.arena.extChunks[0]
	full := chunk[:cap(chunk)]

	tr.Reset()

	require.Len(t, tr.arena.extChunks, 1, "Reset must trim trailing chunks")
	assert.Empty(t, tr.arena.extChunks[0], "Reset must truncate the reused chunk's length")
	assert.Equal(t, cap(chunk), cap(tr.arena.extChunks[0]), "Reset must keep the chunk's capacity")

	tr.Insert(nibs(0x05, 0x06, 0x07, 0x08), nil, nil)
	require.Len(t, tr.Root.Children, 1)
	newExt := tr.Root.Children[0].Ext
	assert.Equal(t, nibs(0x06, 0x07, 0x08), newExt)
	assert.Equal(t, newExt, full[:len(newExt)], "post-reset extension must land in the same backing array as before Reset")
}

func TestPrefixTrieArenaSpansMultipleSlabs(t *testing.T) {
	tr := newPrefixTrie()
	for range prefixSlabMax + 5 {
		tr.arena.allocNode()
	}
	assert.Equal(t, prefixSlabMax+5+1 /*root*/, tr.arena.nodeCount())
	assert.GreaterOrEqual(t, len(tr.arena.slabs), 2)

	grown := len(tr.arena.slabs)

	tr.Reset()
	assert.Equal(t, 1, tr.arena.nodeCount())
	assert.Less(t, len(tr.arena.slabs), grown, "Reset must drop the slabs past the budget")
	held := 0
	for _, s := range tr.arena.slabs {
		held += len(s)
	}
	assert.LessOrEqual(t, held, prefixSlabMax, "Reset must not retain more than one max slab's worth")
}

func TestPrefixArenaGrowsGeometrically(t *testing.T) {
	a := newPrefixArena()
	require.Len(t, a.slabs, 1)
	require.Len(t, a.slabs[0], prefixSlabMin, "a fresh arena must not pay for a peak-sized slab")
	require.Empty(t, a.extChunks, "a fresh arena must not pay for an ext chunk")

	for range prefixSlabMin + 1 {
		a.allocNode()
	}
	require.Len(t, a.slabs, 2)
	require.Len(t, a.slabs[1], 2*prefixSlabMin, "each slab must double the previous one")
	require.Equal(t, prefixSlabMin+1, a.nodeCount())

	a.allocExt([]byte{1, 2, 3})
	require.Len(t, a.extChunks, 1)
	require.Equal(t, prefixExtChunkMin, cap(a.extChunks[0]), "the first ext chunk must start small")
}

func TestPrefixArenaSlabSizeIsCapped(t *testing.T) {
	a := newPrefixArena()
	for range 4 * prefixSlabMax {
		a.allocNode()
	}
	for i, s := range a.slabs {
		require.LessOrEqual(t, len(s), prefixSlabMax, "slab %d exceeds the cap", i)
	}
	require.Equal(t, 4*prefixSlabMax, a.nodeCount())
}

func TestPrefixTrieWalkDFSOrder(t *testing.T) {
	tr := newPrefixTrie()
	tr.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	tr.Insert(nibs(0x01, 0x05, 0x06), nil, nil)
	tr.Insert(nibs(0x07, 0x08, 0x09), nil, nil)

	entries := collectWalk(tr)

	require.Len(t, entries, 5)
	assert.Empty(t, entries[0].prefix, "root walk entry has empty prefix")
	assert.Equal(t, nibs(0x01), entries[1].prefix)
	assert.Equal(t, nibs(0x01, 0x02, 0x03), entries[2].prefix)
	assert.Equal(t, nibs(0x01, 0x05, 0x06), entries[3].prefix)
	assert.Equal(t, nibs(0x07, 0x08, 0x09), entries[4].prefix)
}

func TestPrefixTrieChildIndex(t *testing.T) {
	t.Run("childIndex", func(t *testing.T) {
		n := &PrefixNode{Bitmap: 0}
		idx, ok := childIndex(n, 0x05)
		assert.False(t, ok)
		assert.Equal(t, 0, idx)

		n.Bitmap = uint16(1)<<0x01 | uint16(1)<<0x05 | uint16(1)<<0x0A
		idx, ok = childIndex(n, 0x01)
		assert.True(t, ok)
		assert.Equal(t, 0, idx)
		idx, ok = childIndex(n, 0x05)
		assert.True(t, ok)
		assert.Equal(t, 1, idx)
		idx, ok = childIndex(n, 0x0A)
		assert.True(t, ok)
		assert.Equal(t, 2, idx)
		idx, ok = childIndex(n, 0x03)
		assert.False(t, ok)
		assert.Equal(t, 1, idx)
		idx, ok = childIndex(n, 0x0F)
		assert.False(t, ok)
		assert.Equal(t, 3, idx)
	})
}

func TestParallelUpdateLifecycle(t *testing.T) {
	t.Run("construction", func(t *testing.T) {
		pu := NewParallelUpdate()
		require.NotNil(t, pu)
		require.NotNil(t, pu.Trie, "trie must be allocated")
		require.NotNil(t, pu.Trie.Root, "trie root must be allocated")
		assert.Empty(t, pu.DeferredCombined, "deferredCombined is empty on construction")
	})

	t.Run("close", func(t *testing.T) {
		pu := NewParallelUpdate()
		pu.Collect(nibs(0x01, 0x02), nil, nil)
		pu.DeferredCombined = append(pu.DeferredCombined, &DeferredBranchUpdate{})

		pu.Close()

		assert.Nil(t, pu.Trie, "trie must be released")
		assert.Nil(t, pu.DeferredCombined, "deferredCombined must be released")
	})
}

func TestParallelUpdateInsertDelegates(t *testing.T) {
	pu := NewParallelUpdate()

	pu.Collect(nibs(0x01, 0x02, 0x03), nil, nil)
	pu.Collect(nibs(0x01, 0x02, 0x04), nil, nil)
	pu.Collect(nibs(0x05, 0x06, 0x07), nil, nil)
	pu.Build()

	expected := newPrefixTrie()
	expected.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	expected.Insert(nibs(0x01, 0x02, 0x04), nil, nil)
	expected.Insert(nibs(0x05, 0x06, 0x07), nil, nil)

	assert.Equal(t, expected.arena.nodeCount(), pu.Trie.arena.nodeCount(),
		"parallelUpdate.Insert must produce same node count as direct trie.Insert")
	assert.Equal(t, expected.Root.SubtreeCount, pu.Trie.Root.SubtreeCount,
		"subtreeCount must reflect all inserts delegated through parallelUpdate")
}

func TestParallelUpdateResetClearsAllState(t *testing.T) {
	pu := NewParallelUpdate()

	pu.Collect(nibs(0x01, 0x02, 0x03), nil, nil)
	pu.Collect(nibs(0x05, 0x06, 0x07), nil, nil)
	pu.DeferredCombined = append(pu.DeferredCombined, &DeferredBranchUpdate{})
	pu.Build()

	require.Equal(t, uint32(2), pu.Trie.Root.SubtreeCount)
	require.Len(t, pu.DeferredCombined, 1)

	pu.Reset()

	assert.Equal(t, uint32(0), pu.Trie.Root.SubtreeCount, "trie must be reset")
	assert.Equal(t, 1, pu.Trie.arena.nodeCount(), "arena must be reset to a single root node")
	assert.Empty(t, pu.DeferredCombined, "deferredCombined must be cleared")

	pu.Collect(nibs(0x0A, 0x0B), nil, nil)
	pu.Build()
	assert.Equal(t, uint32(1), pu.Trie.Root.SubtreeCount)
}

func TestParallelUpdateAppendDeferredSequential(t *testing.T) {
	pu := NewParallelUpdate()

	pu.AppendDeferred(nil)
	pu.AppendDeferred([]*DeferredBranchUpdate{})
	assert.Empty(t, pu.DeferredCombined, "empty inputs must not grow deferredCombined")

	a := &DeferredBranchUpdate{}
	b := &DeferredBranchUpdate{}
	c := &DeferredBranchUpdate{}

	pu.AppendDeferred([]*DeferredBranchUpdate{a})
	pu.AppendDeferred([]*DeferredBranchUpdate{b, c})

	require.Len(t, pu.DeferredCombined, 3)
	assert.Same(t, a, pu.DeferredCombined[0])
	assert.Same(t, b, pu.DeferredCombined[1])
	assert.Same(t, c, pu.DeferredCombined[2])
}

func TestParallelUpdateAppendDeferredConcurrent(t *testing.T) {
	pu := NewParallelUpdate()

	const (
		workers      = 16
		perWorker    = 32
		expectedSize = workers * perWorker
	)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			batch := make([]*DeferredBranchUpdate, perWorker)
			for i := range batch {
				batch[i] = &DeferredBranchUpdate{}
			}
			pu.AppendDeferred(batch)
		})
	}
	wg.Wait()

	assert.Len(t, pu.DeferredCombined, expectedSize,
		"appendDeferred must atomically merge every worker's batch")

	for i, upd := range pu.DeferredCombined {
		assert.NotNil(t, upd, "deferredCombined[%d] must not be nil", i)
	}
}

func TestPrefixTrieInsertDuplicateMerges(t *testing.T) {
	t.Parallel()

	trie := newPrefixTrie()
	key := nibs(0x01, 0x02, 0x03)
	first := &Update{Flags: BalanceUpdate, Balance: *uint256.NewInt(100)}
	trie.Insert(key, []byte("pk"), first)
	require.Equal(t, uint32(1), trie.Root.SubtreeCount)

	trie.Insert(key, []byte("pk"), &Update{Flags: NonceUpdate, Nonce: 5})

	assert.Equal(t, uint32(1), trie.Root.SubtreeCount, "duplicate insert must not inflate subtreeCount")

	var got *Update
	count := 0
	require.NoError(t, dfsSubtree(trie.Root, nil, func(_, _ []byte, upd *Update) error {
		got = upd
		count++
		return nil
	}))
	require.Equal(t, 1, count, "duplicate insert must not add a second key")
	require.NotNil(t, got)
	assert.Equal(t, BalanceUpdate|NonceUpdate, got.Flags, "flags must accumulate")
	assert.Equal(t, uint64(100), got.Balance.Uint64())
	assert.Equal(t, uint64(5), got.Nonce)

	assert.Equal(t, BalanceUpdate, first.Flags, "merge must not mutate the previously stored update")
}

func TestPrefixArenaAllocExt_OversizeAndReuse(t *testing.T) {
	t.Run("extension larger than a chunk gets its own backing", func(t *testing.T) {
		a := newPrefixArena()
		big := bytes.Repeat([]byte{0x7}, prefixExtChunkMax+1)
		got := a.allocExt(big)
		require.Equal(t, big, got)
		require.Equal(t, len(got), cap(got))
		require.Empty(t, a.extChunks, "an oversize extension must not force a chunk into being")
	})

	t.Run("reset refills existing chunks instead of reallocating", func(t *testing.T) {
		a := newPrefixArena()
		block := make([]byte, prefixExtChunkMax/4)
		for range 12 {
			a.allocExt(block)
		}
		require.Greater(t, len(a.extChunks), 1, "test must cross a chunk boundary")
		grown := len(a.extChunks)
		backing := make([]*byte, grown)
		for i := range a.extChunks {
			backing[i] = &a.extChunks[i][:1][0]
		}

		a.resetArena()
		require.Len(t, a.extChunks, grown, "reset must keep the grown chunks")
		for i := range a.extChunks {
			require.Empty(t, a.extChunks[i])
		}
		for range 12 {
			a.allocExt(block)
		}
		require.Len(t, a.extChunks, grown, "refill must reuse chunks, not allocate new ones")
		for i := range a.extChunks {
			require.Same(t, backing[i], &a.extChunks[i][:1][0], "chunk backing array must be reused")
		}
	})
}

func TestPlainKeyArenaGrowsGeometrically(t *testing.T) {
	var a plainKeyArena
	a.intern([]byte("addr"))
	require.Equal(t, plainKeyArenaChunkMin, cap(a.buf), "a fresh key arena must not pay for a full chunk")

	block := make([]byte, plainKeyArenaChunkMin)
	a.intern(block)
	require.Equal(t, 2*plainKeyArenaChunkMin, cap(a.buf), "each chunk must double the previous one")

	for cap(a.buf) < plainKeyArenaChunkMax {
		a.intern(block)
	}
	require.Equal(t, plainKeyArenaChunkMax, cap(a.buf))
	for range 128 {
		a.intern(block)
	}
	require.Equal(t, plainKeyArenaChunkMax, cap(a.buf), "chunk size must stay capped")

	a.reset()
	require.Empty(t, a.buf)
	require.Positive(t, cap(a.buf), "reset must keep the grown capacity")
}

func TestPrefixArenaKeepsMaxSlabAcrossReset(t *testing.T) {
	a := newPrefixArena()
	for range prefixSlabMax + 1 {
		a.allocNode()
	}
	var maxSlab *PrefixNode
	for _, s := range a.slabs {
		if len(s) == prefixSlabMax {
			maxSlab = &s[0]
			break
		}
	}
	require.NotNil(t, maxSlab, "test must actually reach a max-sized slab")

	a.resetArena()
	for range prefixSlabMax {
		a.allocNode()
	}
	for _, s := range a.slabs {
		if len(s) == prefixSlabMax {
			require.Same(t, maxSlab, &s[0], "refill must reuse the max slab, not allocate a new one")
			return
		}
	}
	t.Fatal("no max-sized slab after refill")
}

func TestPrefixTrieResetClearsVisited(t *testing.T) {
	tr := newPrefixTrie()
	for i := range 64 {
		k := make([]byte, 64)
		k[0] = byte(i % 16)
		k[1] = byte(i / 16)
		tr.Insert(k, nil, nil)
	}
	require.NotZero(t, cap(tr.visited), "test must actually populate visited")

	tr.Reset()
	for i, n := range tr.visited[:cap(tr.visited)] {
		require.Nil(t, n, "Reset left a node pointer at visited[%d], pinning a dropped slab", i)
	}
}

// hashedKey passed to fn is mutated in place and must not be retained.
func dfsSubtree(node *PrefixNode, path []byte, fn func(hashedKey, plainKey []byte, update *Update) error) error {
	if node == nil {
		return nil
	}
	if node.PlainKey != nil {
		if err := fn(path, node.PlainKey, node.Update); err != nil {
			return err
		}
	} else if node.Bitmap == 0 {
		return errors.New("ParallelPatriciaHashed: trie leaf without a plainKey")
	}
	childIdx := 0
	for bm := node.Bitmap; bm != 0; {
		nib := byte(bits.TrailingZeros16(bm))
		child := node.Children[childIdx]
		base := len(path)
		path = append(path, nib)
		path = append(path, child.Ext...)
		if err := dfsSubtree(child, path, fn); err != nil {
			return err
		}
		path = path[:base]
		childIdx++
		bm &^= uint16(1) << nib
	}
	return nil
}
