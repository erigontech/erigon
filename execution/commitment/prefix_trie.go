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

import "math/bits"

const prefixSlabSize = 16384

// prefixNode is a path-compressed prefix-trie node keyed on nibbles.
// ext is the compressed nibble path from the parent (each byte is one nibble 0x00..0x0F).
// bitmap marks which children are present; children is dense (len == popcount(bitmap)).
// subtreeCount is incremented on every insert that traverses the node — it counts
// path traversals, not distinct keys.
type prefixNode struct {
	ext      []byte
	children []*prefixNode
	plainKey []byte // un-hashed key; set only where a key terminates
	subtreeCount uint32
	bitmap       uint16
}

// prefixSlab is a fixed-size backing array for prefixNodes. Pointers into the slab
// remain stable as long as the slab itself is not freed, so the arena never resizes
// individual slabs — it appends new ones.
type prefixSlab struct {
	nodes [prefixSlabSize]prefixNode
}

// prefixArena bump-allocates prefixNodes from a list of slabs.
type prefixArena struct {
	slabs   []*prefixSlab
	slabIdx int
	nextIdx int
}

func newPrefixArena() *prefixArena {
	return &prefixArena{slabs: []*prefixSlab{new(prefixSlab)}}
}

func (a *prefixArena) allocNode() *prefixNode {
	if a.nextIdx >= prefixSlabSize {
		a.slabIdx++
		if a.slabIdx >= len(a.slabs) {
			a.slabs = append(a.slabs, new(prefixSlab))
		}
		a.nextIdx = 0
	}
	n := &a.slabs[a.slabIdx].nodes[a.nextIdx]
	a.nextIdx++
	*n = prefixNode{}
	return n
}

// resetArena clears every node touched so far so the slabs can be re-used without
// re-allocating fresh memory. Keeps the first slab; trailing slabs are released.
func (a *prefixArena) resetArena() {
	for i := 0; i <= a.slabIdx && i < len(a.slabs); i++ {
		limit := prefixSlabSize
		if i == a.slabIdx {
			limit = a.nextIdx
		}
		clear(a.slabs[i].nodes[:limit])
	}
	// Drop pointers to trailing slabs so the GC can reclaim them; the slice
	// reslice below would otherwise keep them alive via the backing array
	// until subsequent appends overwrote each slot.
	clear(a.slabs[1:])
	a.slabs = a.slabs[:1]
	a.slabIdx = 0
	a.nextIdx = 0
}

// nodeCount reports how many nodes the arena has handed out since the last reset.
func (a *prefixArena) nodeCount() int {
	return a.slabIdx*prefixSlabSize + a.nextIdx
}

// popcount returns the number of children present on the node.
func popcount(n *prefixNode) int {
	return bits.OnesCount16(n.bitmap)
}

// childIndex returns the dense index of a child nibble in node.children and whether
// the child is present.
func childIndex(n *prefixNode, nib byte) (int, bool) {
	mask := uint16(1) << nib
	idx := bits.OnesCount16(n.bitmap & (mask - 1))
	return idx, n.bitmap&mask != 0
}

// prefixTrie is a path-compressed nibble trie. Insert is sequential — concurrent
// callers must serialize themselves.
type prefixTrie struct {
	root  *prefixNode
	arena *prefixArena
}

func newPrefixTrie() *prefixTrie {
	a := newPrefixArena()
	return &prefixTrie{root: a.allocNode(), arena: a}
}

// Reset clears the trie and reuses the underlying arena.
func (t *prefixTrie) Reset() {
	t.arena.resetArena()
	t.root = t.arena.allocNode()
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// Insert adds hashedKey (in nibble form) to the trie and records plainKey on the
// node where the key terminates. Each call bumps subtreeCount on every node along
// the traversal path — duplicate inserts therefore increase counts but allocate
// no new nodes. plainKey backing must stay stable for the trie's lifetime.
func (t *prefixTrie) Insert(hashedKey, plainKey []byte) {
	node := t.root
	keyOffset := 0
	for {
		node.subtreeCount++

		remain := hashedKey[keyOffset:]
		m := commonPrefixLen(remain, node.ext)

		if m < len(node.ext) {
			// Partial match — split the current node at position m.
			oldExt := node.ext
			oldBitmap := node.bitmap
			oldChildren := node.children
			oldSubtreeCount := node.subtreeCount - 1

			oldChild := t.arena.allocNode()
			oldChild.ext = oldExt[m+1:]
			oldChild.bitmap = oldBitmap
			oldChild.children = oldChildren
			oldChild.subtreeCount = oldSubtreeCount
			// terminator on node was a key reaching past m; it moves to oldChild
			oldChild.plainKey = node.plainKey
			node.plainKey = nil

			node.ext = oldExt[:m]

			if m == len(remain) {
				// Key terminates inside the old extension — no new sibling, just one child.
				node.bitmap = uint16(1) << oldExt[m]
				node.children = []*prefixNode{oldChild}
				node.plainKey = plainKey
				return
			}

			newLeaf := t.arena.allocNode()
			newNib := remain[m]
			newLeaf.ext = append([]byte(nil), remain[m+1:]...)
			newLeaf.subtreeCount = 1
			newLeaf.plainKey = plainKey

			oldNib := oldExt[m]
			node.bitmap = (uint16(1) << oldNib) | (uint16(1) << newNib)
			if oldNib < newNib {
				node.children = []*prefixNode{oldChild, newLeaf}
			} else {
				node.children = []*prefixNode{newLeaf, oldChild}
			}
			return
		}

		// Full match on node.ext.
		keyOffset += m
		if keyOffset == len(hashedKey) {
			// Key terminates exactly at this node.
			node.plainKey = plainKey
			return
		}

		nib := hashedKey[keyOffset]
		idx, ok := childIndex(node, nib)
		if !ok {
			newLeaf := t.arena.allocNode()
			newLeaf.ext = append([]byte(nil), hashedKey[keyOffset+1:]...)
			newLeaf.subtreeCount = 1
			newLeaf.plainKey = plainKey
			node.bitmap |= uint16(1) << nib
			node.children = append(node.children, nil)
			copy(node.children[idx+1:], node.children[idx:])
			node.children[idx] = newLeaf
			return
		}
		keyOffset++
		node = node.children[idx]
	}
}
