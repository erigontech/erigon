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

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// Slabs and ext chunks grow geometrically: a batch touching a handful of keys must
// not pay for a peak-sized arena, because a fresh Updates is built per block.
const prefixSlabMin = 256
const prefixSlabMax = 16384

const prefixExtChunkMin = 4 * 1024
const prefixExtChunkMax = 64 * 1024

type prefixNode struct {
	// ext is arena-backed: it stays valid only until the owning trie's Reset, which
	// recycles the chunk in place. A reader that must outlive the batch copies it.
	ext          []byte
	children     []*prefixNode
	plainKey     []byte
	update       *Update
	subtreeCount uint32
	bitmap       uint16
}

type prefixArena struct {
	slabs       [][]prefixNode
	slabIdx     int
	nextIdx     int
	priorNodes  int // nodes held by slabs[:slabIdx], so nodeCount stays O(1)
	extChunks   [][]byte
	extChunkIdx int
}

func newPrefixArena() *prefixArena {
	return &prefixArena{slabs: [][]prefixNode{make([]prefixNode, prefixSlabMin)}}
}

func (a *prefixArena) allocNode() *prefixNode {
	slab := a.slabs[a.slabIdx]
	if a.nextIdx >= len(slab) {
		a.priorNodes += len(slab)
		a.slabIdx++
		if a.slabIdx >= len(a.slabs) {
			a.slabs = append(a.slabs, make([]prefixNode, min(len(slab)*2, prefixSlabMax)))
		}
		slab = a.slabs[a.slabIdx]
		a.nextIdx = 0
	}
	n := &slab[a.nextIdx]
	a.nextIdx++
	*n = prefixNode{}
	return n
}

// On overflow, swaps in a fresh chunk, keeping prior slices valid.
func (a *prefixArena) allocExt(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	if len(b) > prefixExtChunkMax {
		own := make([]byte, len(b))
		copy(own, b)
		return own
	}
	if len(a.extChunks) == 0 {
		a.extChunks = append(a.extChunks, make([]byte, 0, max(prefixExtChunkMin, len(b))))
	}
	chunk := a.extChunks[a.extChunkIdx]
	if cap(chunk)-len(chunk) < len(b) {
		a.extChunkIdx++
		if a.extChunkIdx >= len(a.extChunks) {
			a.extChunks = append(a.extChunks, make([]byte, 0, min(max(cap(chunk)*2, len(b)), prefixExtChunkMax)))
		}
		chunk = a.extChunks[a.extChunkIdx]
	}
	off := len(chunk)
	chunk = append(chunk, b...)
	a.extChunks[a.extChunkIdx] = chunk
	return chunk[off:len(chunk):len(chunk)]
}

func (a *prefixArena) resetArena() {
	for i := 0; i <= a.slabIdx && i < len(a.slabs); i++ {
		limit := len(a.slabs[i])
		if i == a.slabIdx {
			limit = a.nextIdx
		}
		clear(a.slabs[i][:limit])
	}
	// Keep at most one max-slab's worth of capacity so the arena settles at its
	// steady-state size; releasing the rest is what stops a peak from being pinned.
	keep, held := 1, len(a.slabs[0])
	for keep < len(a.slabs) && held+len(a.slabs[keep]) <= prefixSlabMax {
		held += len(a.slabs[keep])
		keep++
	}
	// nil trailing slabs first: reslicing alone keeps them GC-reachable via the backing array.
	clear(a.slabs[keep:])
	a.slabs = a.slabs[:keep]
	a.slabIdx = 0
	a.nextIdx = 0
	a.priorNodes = 0

	for i := range a.extChunks {
		a.extChunks[i] = a.extChunks[i][:0]
	}
	a.extChunkIdx = 0
}

func (a *prefixArena) nodeCount() int {
	return a.priorNodes + a.nextIdx
}

func popcount(n *prefixNode) int {
	return bits.OnesCount16(n.bitmap)
}

func childIndex(n *prefixNode, nib byte) (int, bool) {
	mask := uint16(1) << nib
	idx := bits.OnesCount16(n.bitmap & (mask - 1))
	return idx, n.bitmap&mask != 0
}

type prefixTrie struct {
	root    *prefixNode
	arena   *prefixArena
	visited []*prefixNode
}

func newPrefixTrie() *prefixTrie {
	a := newPrefixArena()
	return &prefixTrie{root: a.allocNode(), arena: a}
}

func (t *prefixTrie) Reset() {
	t.arena.resetArena()
	t.root = t.arena.allocNode()
}

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
		m := nibbles.CommonPrefixLen(remain, node.ext)

		if m < len(node.ext) {
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

			node.ext = oldExt[:m:m]

			if m == len(remain) {
				node.bitmap = uint16(1) << oldExt[m]
				node.children = []*prefixNode{oldChild}
				node.plainKey = plainKey
				node.update = update
				bumpPath()
				return true
			}

			newLeaf := t.arena.allocNode()
			newNib := remain[m]
			newLeaf.ext = t.arena.allocExt(remain[m+1:])
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
			newLeaf.ext = t.arena.allocExt(hashedKey[keyOffset+1:])
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
