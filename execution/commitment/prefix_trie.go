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

// prefixNode is a path-compressed prefix-trie node keyed on nibbles (each ext byte is one nibble 0x00..0x0F).
// children is dense: len == popcount(bitmap). subtreeCount is the number of distinct keys in the
// subtree; re-inserting an existing key merges its update without bumping it.
type prefixNode struct {
	ext          []byte
	children     []*prefixNode
	plainKey     []byte  // set only where a key terminates
	update       *Update // carried value (nil = re-read from ctx); set only where a key terminates
	subtreeCount uint32
	bitmap       uint16
}

// prefixSlab is a fixed-size backing array for prefixNodes; pointers into it stay stable until freed.
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

// resetArena clears touched nodes for reuse, keeping the first slab and releasing the rest.
func (a *prefixArena) resetArena() {
	for i := 0; i <= a.slabIdx && i < len(a.slabs); i++ {
		limit := prefixSlabSize
		if i == a.slabIdx {
			limit = a.nextIdx
		}
		clear(a.slabs[i].nodes[:limit])
	}
	// Nil out trailing slabs so the GC reclaims them; the reslice below keeps them alive via the backing array otherwise.
	clear(a.slabs[1:])
	a.slabs = a.slabs[:1]
	a.slabIdx = 0
	a.nextIdx = 0
}

func (a *prefixArena) nodeCount() int {
	return a.slabIdx*prefixSlabSize + a.nextIdx
}

func popcount(n *prefixNode) int {
	return bits.OnesCount16(n.bitmap)
}

func childIndex(n *prefixNode, nib byte) (int, bool) {
	mask := uint16(1) << nib
	idx := bits.OnesCount16(n.bitmap & (mask - 1))
	return idx, n.bitmap&mask != 0
}

// prefixTrie is a path-compressed nibble trie; Insert is not safe for concurrent use.
type prefixTrie struct {
	root    *prefixNode
	arena   *prefixArena
	visited []*prefixNode // Insert scratch: path nodes pending a subtreeCount bump
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

// Insert adds hashedKey (nibble form), recording plainKey and optional update (nil = re-read from ctx) at its terminating node.
// Re-inserting merges updates copy-on-write so a snapshot of the old update stays intact; plainKey backing must outlive the trie.
func (t *prefixTrie) Insert(hashedKey, plainKey []byte, update *Update) (isNew bool) {
	node := t.root
	keyOffset := 0
	t.visited = t.visited[:0]
	bumpPath := func() {
		for _, n := range t.visited {
			n.subtreeCount++
		}
	}
	for {
		t.visited = append(t.visited, node)

		remain := hashedKey[keyOffset:]
		m := commonPrefixLen(remain, node.ext)

		if m < len(node.ext) {
			// Partial match: split the node at position m.
			oldExt := node.ext
			oldBitmap := node.bitmap
			oldChildren := node.children
			oldSubtreeCount := node.subtreeCount

			oldChild := t.arena.allocNode()
			oldChild.ext = oldExt[m+1:]
			oldChild.bitmap = oldBitmap
			oldChild.children = oldChildren
			oldChild.subtreeCount = oldSubtreeCount
			// A terminator on the split node belonged to a longer key; move it to oldChild.
			oldChild.plainKey = node.plainKey
			oldChild.update = node.update
			node.plainKey = nil
			node.update = nil

			node.ext = oldExt[:m]

			if m == len(remain) {
				// Key ends inside the old extension: one child, no new sibling.
				node.bitmap = uint16(1) << oldExt[m]
				node.children = []*prefixNode{oldChild}
				node.plainKey = plainKey
				node.update = update
				bumpPath()
				return true
			}

			newLeaf := t.arena.allocNode()
			newNib := remain[m]
			newLeaf.ext = append([]byte(nil), remain[m+1:]...)
			newLeaf.subtreeCount = 1
			newLeaf.plainKey = plainKey
			newLeaf.update = update

			oldNib := oldExt[m]
			node.bitmap = (uint16(1) << oldNib) | (uint16(1) << newNib)
			if oldNib < newNib {
				node.children = []*prefixNode{oldChild, newLeaf}
			} else {
				node.children = []*prefixNode{newLeaf, oldChild}
			}
			bumpPath()
			return true
		}

		keyOffset += m
		if keyOffset == len(hashedKey) {
			if node.plainKey != nil {
				if update != nil {
					merged := &Update{}
					if node.update != nil {
						*merged = *node.update
						merged.Merge(update)
					} else {
						*merged = *update
					}
					node.update = merged
				}
				return false
			}
			node.plainKey = plainKey
			node.update = update
			bumpPath()
			return true
		}

		nib := hashedKey[keyOffset]
		idx, ok := childIndex(node, nib)
		if !ok {
			newLeaf := t.arena.allocNode()
			newLeaf.ext = append([]byte(nil), hashedKey[keyOffset+1:]...)
			newLeaf.subtreeCount = 1
			newLeaf.plainKey = plainKey
			newLeaf.update = update
			node.bitmap |= uint16(1) << nib
			node.children = append(node.children, nil)
			copy(node.children[idx+1:], node.children[idx:])
			node.children[idx] = newLeaf
			bumpPath()
			return true
		}
		keyOffset++
		node = node.children[idx]
	}
}
