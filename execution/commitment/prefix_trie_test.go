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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nibs converts a byte slice (one nibble per byte) helper for readability.
func nibs(vals ...byte) []byte {
	out := make([]byte, len(vals))
	copy(out, vals)
	return out
}

// collectWalk returns the set of (prefix, subtreeCount) pairs in DFS order.
type walkEntry struct {
	prefix       []byte
	subtreeCount uint32
	bitmap       uint16
}

func collectWalk(t *prefixTrie) []walkEntry {
	var out []walkEntry
	t.Walk(func(prefix []byte, node *prefixNode) {
		p := make([]byte, len(prefix))
		copy(p, prefix)
		out = append(out, walkEntry{prefix: p, subtreeCount: node.subtreeCount, bitmap: node.bitmap})
	})
	return out
}

func TestPrefixTrieEmpty(t *testing.T) {
	tr := newPrefixTrie()
	require.NotNil(t, tr.root)
	assert.Equal(t, 1, tr.arena.nodeCount())
	assert.Equal(t, uint32(0), tr.root.subtreeCount)
	assert.Equal(t, uint16(0), tr.root.bitmap)
	assert.Empty(t, tr.root.children)
	assert.Empty(t, tr.root.ext)
}

func TestPrefixTrieSingleInsert(t *testing.T) {
	tr := newPrefixTrie()
	key := nibs(0x01, 0x02, 0x03, 0x04)
	tr.Insert(key)

	// root + one leaf
	assert.Equal(t, 2, tr.arena.nodeCount())
	assert.Equal(t, uint32(1), tr.root.subtreeCount)
	assert.Equal(t, uint16(1)<<0x01, tr.root.bitmap)
	require.Len(t, tr.root.children, 1)

	leaf := tr.root.children[0]
	assert.Equal(t, uint32(1), leaf.subtreeCount)
	assert.Equal(t, nibs(0x02, 0x03, 0x04), leaf.ext)
	assert.Equal(t, uint16(0), leaf.bitmap)
}

func TestPrefixTrieTwoInsertsDivergeAtRoot(t *testing.T) {
	tr := newPrefixTrie()
	tr.Insert(nibs(0x01, 0x02, 0x03))
	tr.Insert(nibs(0x05, 0x06, 0x07))

	assert.Equal(t, 3, tr.arena.nodeCount())
	assert.Equal(t, uint32(2), tr.root.subtreeCount)
	assert.Equal(t, uint16(1)<<0x01|uint16(1)<<0x05, tr.root.bitmap)
	require.Len(t, tr.root.children, 2)

	// Children are in nibble order: 0x01 first, then 0x05.
	assert.Equal(t, nibs(0x02, 0x03), tr.root.children[0].ext)
	assert.Equal(t, uint32(1), tr.root.children[0].subtreeCount)
	assert.Equal(t, nibs(0x06, 0x07), tr.root.children[1].ext)
	assert.Equal(t, uint32(1), tr.root.children[1].subtreeCount)
}

func TestPrefixTrieDivergenceInsideExtension(t *testing.T) {
	tr := newPrefixTrie()
	// Both keys descend on 0x01, then share extension [0x02, 0x03], then diverge.
	tr.Insert(nibs(0x01, 0x02, 0x03, 0x04, 0x05))
	tr.Insert(nibs(0x01, 0x02, 0x03, 0x06, 0x07))

	// root -> child (ext=[0x02,0x03], bitmap has 2 children) -> two grandchildren
	assert.Equal(t, 4, tr.arena.nodeCount())

	require.Len(t, tr.root.children, 1)
	mid := tr.root.children[0]
	assert.Equal(t, nibs(0x02, 0x03), mid.ext)
	assert.Equal(t, uint32(2), mid.subtreeCount)
	assert.Equal(t, uint16(1)<<0x04|uint16(1)<<0x06, mid.bitmap)
	require.Len(t, mid.children, 2)

	assert.Equal(t, nibs(0x05), mid.children[0].ext)
	assert.Equal(t, uint32(1), mid.children[0].subtreeCount)
	assert.Equal(t, nibs(0x07), mid.children[1].ext)
	assert.Equal(t, uint32(1), mid.children[1].subtreeCount)
}

func TestPrefixTrieDivergenceAtEndOfExtension(t *testing.T) {
	tr := newPrefixTrie()
	// First insert creates leaf with ext = [0x02, 0x03, 0x04, 0x05].
	tr.Insert(nibs(0x01, 0x02, 0x03, 0x04, 0x05))
	// Second insert shares 0x01,0x02,0x03,0x04 with leaf, then continues — m == len(ext) - 1.
	// Actually shares 0x01, descends into leaf ext = [0x02,0x03,0x04,0x05], common prefix = 4 (full),
	// then we need a tail. So this lands on the descend-into-existing-child path after splitting.
	// Use a key that shares fully then descends with a new nibble:
	tr.Insert(nibs(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07))

	// Walk through expected structure:
	// root -> child (ext=[0x02,0x03,0x04,0x05], one child at nibble 0x06) -> grandchild (ext=[0x07])
	assert.Equal(t, 3, tr.arena.nodeCount())

	require.Len(t, tr.root.children, 1)
	mid := tr.root.children[0]
	assert.Equal(t, nibs(0x02, 0x03, 0x04, 0x05), mid.ext)
	assert.Equal(t, uint32(2), mid.subtreeCount)
	assert.Equal(t, uint16(1)<<0x06, mid.bitmap)
	require.Len(t, mid.children, 1)

	leaf := mid.children[0]
	assert.Equal(t, nibs(0x07), leaf.ext)
	assert.Equal(t, uint32(1), leaf.subtreeCount)
}

func TestPrefixTrieDuplicateInsert(t *testing.T) {
	tr := newPrefixTrie()
	key := nibs(0x01, 0x02, 0x03, 0x04)
	tr.Insert(key)
	nodesAfterFirst := tr.arena.nodeCount()

	// Insert same key twice more — no growth.
	tr.Insert(key)
	tr.Insert(key)

	assert.Equal(t, nodesAfterFirst, tr.arena.nodeCount(), "duplicate inserts must not grow the trie")
	assert.Equal(t, uint32(3), tr.root.subtreeCount, "subtreeCount must reflect path traversals")
	require.Len(t, tr.root.children, 1)
	assert.Equal(t, uint32(3), tr.root.children[0].subtreeCount)
}

func TestPrefixTrieDeepInsert(t *testing.T) {
	tr := newPrefixTrie()
	deep := make([]byte, 128)
	for i := range deep {
		deep[i] = byte(i % 16)
	}
	tr.Insert(deep)

	// root + one leaf
	assert.Equal(t, 2, tr.arena.nodeCount())
	assert.Equal(t, uint32(1), tr.root.subtreeCount)
	require.Len(t, tr.root.children, 1)
	assert.Equal(t, 127, len(tr.root.children[0].ext), "leaf ext = remainder after consuming first nibble")
	assert.True(t, bytes.Equal(tr.root.children[0].ext, deep[1:]))
}

func TestPrefixTrieSubtreeCountAccumulation(t *testing.T) {
	tr := newPrefixTrie()
	const N = 50
	for i := 0; i < N; i++ {
		// keep first three nibbles shared, vary the rest deterministically
		k := []byte{0x01, 0x02, 0x03, byte(i & 0x0F), byte((i >> 4) & 0x0F), byte(i & 0x0F)}
		tr.Insert(k)
	}
	assert.Equal(t, uint32(N), tr.root.subtreeCount)

	// First-level child rooted at nibble 0x01 should also have N entries.
	require.Len(t, tr.root.children, 1)
	assert.Equal(t, uint32(N), tr.root.children[0].subtreeCount)
}

func TestPrefixTrieMixedPrefixCountsPropagate(t *testing.T) {
	tr := newPrefixTrie()
	// 4 inserts in left subtree (nibble 0x01), 3 inserts in right subtree (nibble 0x05).
	for _, suf := range [][]byte{
		{0x02, 0x00}, {0x02, 0x01}, {0x02, 0x02}, {0x02, 0x03},
	} {
		tr.Insert(append([]byte{0x01}, suf...))
	}
	for _, suf := range [][]byte{
		{0x06, 0x00}, {0x06, 0x01}, {0x06, 0x02},
	} {
		tr.Insert(append([]byte{0x05}, suf...))
	}

	assert.Equal(t, uint32(7), tr.root.subtreeCount)
	require.Len(t, tr.root.children, 2)
	assert.Equal(t, uint32(4), tr.root.children[0].subtreeCount, "left subtree")
	assert.Equal(t, uint32(3), tr.root.children[1].subtreeCount, "right subtree")
}

func TestPrefixTrieArenaReuse(t *testing.T) {
	tr := newPrefixTrie()
	tr.Insert(nibs(0x01, 0x02, 0x03))
	tr.Insert(nibs(0x01, 0x02, 0x04))
	tr.Insert(nibs(0x05, 0x06, 0x07))
	first := tr.arena.nodeCount()

	tr.Reset()
	assert.Equal(t, 1, tr.arena.nodeCount(), "Reset must leave only the fresh root")
	assert.Equal(t, uint32(0), tr.root.subtreeCount)
	assert.Equal(t, uint16(0), tr.root.bitmap)
	assert.Empty(t, tr.root.children)

	// Re-insert the same set — should produce same node count.
	tr.Insert(nibs(0x01, 0x02, 0x03))
	tr.Insert(nibs(0x01, 0x02, 0x04))
	tr.Insert(nibs(0x05, 0x06, 0x07))
	assert.Equal(t, first, tr.arena.nodeCount())
}

func TestPrefixTrieArenaSpansMultipleSlabs(t *testing.T) {
	tr := newPrefixTrie()
	// Force allocation past one slab by inserting distinct keys diverging at nibble 0.
	// Each insert under a unique top nibble creates one new leaf, but we only have 16
	// possible top nibbles, so vary the second nibble too. We need > prefixSlabSize allocs
	// to be sure, but that's 16K — too slow for a unit test. Instead validate the slab
	// boundary by stuffing the arena directly.
	for i := 0; i < prefixSlabSize+5; i++ {
		tr.arena.allocNode()
	}
	assert.Equal(t, prefixSlabSize+5+1 /*root*/, tr.arena.nodeCount())
	assert.GreaterOrEqual(t, len(tr.arena.slabs), 2)

	tr.Reset()
	assert.Equal(t, 1, tr.arena.nodeCount())
	assert.Len(t, tr.arena.slabs, 1, "Reset must trim trailing slabs")
}

func TestPrefixTrieWalkDFSOrder(t *testing.T) {
	tr := newPrefixTrie()
	tr.Insert(nibs(0x01, 0x02, 0x03))
	tr.Insert(nibs(0x01, 0x05, 0x06))
	tr.Insert(nibs(0x07, 0x08, 0x09))

	entries := collectWalk(tr)

	// Walk visits root, then full DFS:
	//   root (prefix=[])
	//   inner at 0x01 (prefix=[0x01])
	//     leaf at 0x02 (prefix=[0x01,0x02,0x03])
	//     leaf at 0x05 (prefix=[0x01,0x05,0x06])
	//   leaf at 0x07 (prefix=[0x07,0x08,0x09])
	require.Len(t, entries, 5)
	assert.Empty(t, entries[0].prefix, "root walk entry has empty prefix")
	assert.Equal(t, nibs(0x01), entries[1].prefix)
	assert.Equal(t, nibs(0x01, 0x02, 0x03), entries[2].prefix)
	assert.Equal(t, nibs(0x01, 0x05, 0x06), entries[3].prefix)
	assert.Equal(t, nibs(0x07, 0x08, 0x09), entries[4].prefix)
}

func TestPrefixTrieChildIndex(t *testing.T) {
	n := &prefixNode{bitmap: 0}
	// no bits set
	idx, ok := childIndex(n, 0x05)
	assert.False(t, ok)
	assert.Equal(t, 0, idx)

	n.bitmap = uint16(1)<<0x01 | uint16(1)<<0x05 | uint16(1)<<0x0A
	idx, ok = childIndex(n, 0x01)
	assert.True(t, ok)
	assert.Equal(t, 0, idx)
	idx, ok = childIndex(n, 0x05)
	assert.True(t, ok)
	assert.Equal(t, 1, idx)
	idx, ok = childIndex(n, 0x0A)
	assert.True(t, ok)
	assert.Equal(t, 2, idx)
	// missing nibble — index reports where it would be inserted
	idx, ok = childIndex(n, 0x03)
	assert.False(t, ok)
	assert.Equal(t, 1, idx)
	idx, ok = childIndex(n, 0x0F)
	assert.False(t, ok)
	assert.Equal(t, 3, idx)
}

func TestPrefixTriePopcount(t *testing.T) {
	n := &prefixNode{}
	assert.Equal(t, 0, popcount(n))
	n.bitmap = 0xFFFF
	assert.Equal(t, 16, popcount(n))
	n.bitmap = 0x0001
	assert.Equal(t, 1, popcount(n))
	n.bitmap = uint16(1)<<0x03 | uint16(1)<<0x07 | uint16(1)<<0x0B
	assert.Equal(t, 3, popcount(n))
}
