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
const (
	prefixSlabMin = 256
	prefixSlabMax = 16384
)

const (
	prefixExtChunkMin = 4 * 1024
	prefixExtChunkMax = 64 * 1024
)

type PrefixNode struct {
	// Ext is arena-backed: it stays valid only until the owning trie's Reset, which
	// recycles the chunk in place. A reader that must outlive the batch copies it.
	Ext          []byte
	Children     []*PrefixNode
	PlainKey     []byte
	Update       *Update
	SubtreeCount uint32
	Bitmap       uint16
}

type prefixArena struct {
	slabs       [][]PrefixNode
	slabIdx     int
	nextIdx     int
	priorNodes  int // nodes held by slabs[:slabIdx], so nodeCount stays O(1)
	extChunks   [][]byte
	extChunkIdx int
}

func newPrefixArena() *prefixArena {
	return &prefixArena{slabs: [][]PrefixNode{make([]PrefixNode, prefixSlabMin)}}
}

func (a *prefixArena) allocNode() *PrefixNode {
	slab := a.slabs[a.slabIdx]
	if a.nextIdx >= len(slab) {
		a.priorNodes += len(slab)
		a.slabIdx++
		if a.slabIdx >= len(a.slabs) {
			a.slabs = append(a.slabs, make([]PrefixNode, min(len(slab)*2, prefixSlabMax)))
		}
		slab = a.slabs[a.slabIdx]
		a.nextIdx = 0
	}
	n := &slab[a.nextIdx]
	a.nextIdx++
	*n = PrefixNode{}
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
	// The ramp sums to less than one max slab, so an arena that reached a max-sized
	// slab must keep that slab instead: keeping the ramp would re-allocate it on
	// every reset, which is the per-batch allocation this arena exists to avoid.
	if len(a.slabs[0]) < prefixSlabMax && keep < len(a.slabs) && len(a.slabs[keep]) == prefixSlabMax {
		a.slabs[0] = a.slabs[keep]
		keep = 1
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

func childIndex(n *PrefixNode, nib byte) (int, bool) {
	mask := uint16(1) << nib
	idx := bits.OnesCount16(n.Bitmap & (mask - 1))
	return idx, n.Bitmap&mask != 0
}

type prefixTrie struct {
	Root    *PrefixNode
	arena   *prefixArena
	visited []*PrefixNode
}

func newPrefixTrie() *prefixTrie {
	a := newPrefixArena()
	return &prefixTrie{Root: a.allocNode(), arena: a}
}

func (t *prefixTrie) Reset() {
	// Past len, visited still holds nodes from earlier inserts; a single one keeps a
	// released slab's whole backing array reachable.
	clear(t.visited[:cap(t.visited)])
	t.visited = t.visited[:0]
	t.arena.resetArena()
	t.Root = t.arena.allocNode()
}

func (t *prefixTrie) Insert(hashedKey, plainKey []byte, update *Update) (isNew bool) {
	node := t.Root
	keyOffset := 0
	t.visited = t.visited[:0]
	bumpPath := func() {
		for _, n := range t.visited {
			n.SubtreeCount++
		}
	}
	for {
		t.visited = append(t.visited, node)

		remain := hashedKey[keyOffset:]
		m := nibbles.CommonPrefixLen(remain, node.Ext)

		if m < len(node.Ext) {
			oldExt := node.Ext
			oldBitmap := node.Bitmap
			oldChildren := node.Children
			oldSubtreeCount := node.SubtreeCount

			oldChild := t.arena.allocNode()
			oldChild.Ext = oldExt[m+1:]
			oldChild.Bitmap = oldBitmap
			oldChild.Children = oldChildren
			oldChild.SubtreeCount = oldSubtreeCount
			// A terminator on the split node belonged to a longer key; move it to oldChild.
			oldChild.PlainKey = node.PlainKey
			oldChild.Update = node.Update
			node.PlainKey = nil
			node.Update = nil

			node.Ext = oldExt[:m:m]

			if m == len(remain) {
				node.Bitmap = uint16(1) << oldExt[m]
				node.Children = []*PrefixNode{oldChild}
				node.PlainKey = plainKey
				node.Update = update
				bumpPath()
				return true
			}

			newLeaf := t.arena.allocNode()
			newNib := remain[m]
			newLeaf.Ext = t.arena.allocExt(remain[m+1:])
			newLeaf.SubtreeCount = 1
			newLeaf.PlainKey = plainKey
			newLeaf.Update = update

			oldNib := oldExt[m]
			node.Bitmap = (uint16(1) << oldNib) | (uint16(1) << newNib)
			if oldNib < newNib {
				node.Children = []*PrefixNode{oldChild, newLeaf}
			} else {
				node.Children = []*PrefixNode{newLeaf, oldChild}
			}
			bumpPath()
			return true
		}

		keyOffset += m
		if keyOffset == len(hashedKey) {
			if node.PlainKey != nil {
				if update != nil {
					merged := &Update{}
					if node.Update != nil {
						*merged = *node.Update
						merged.Merge(update)
					} else {
						*merged = *update
					}
					node.Update = merged
				}
				return false
			}
			node.PlainKey = plainKey
			node.Update = update
			bumpPath()
			return true
		}

		nib := hashedKey[keyOffset]
		idx, ok := childIndex(node, nib)
		if !ok {
			newLeaf := t.arena.allocNode()
			newLeaf.Ext = t.arena.allocExt(hashedKey[keyOffset+1:])
			newLeaf.SubtreeCount = 1
			newLeaf.PlainKey = plainKey
			newLeaf.Update = update
			node.Bitmap |= uint16(1) << nib
			node.Children = append(node.Children, nil)
			copy(node.Children[idx+1:], node.Children[idx:])
			node.Children[idx] = newLeaf
			bumpPath()
			return true
		}
		keyOffset++
		node = node.Children[idx]
	}
}
