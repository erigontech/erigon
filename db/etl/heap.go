// Copyright 2021 The Erigon Authors
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

package etl

import (
	"bytes"
	"encoding/binary"
	"slices"
)

type HeapElem struct {
	Key     []byte
	Value   []byte
	TimeIdx int
}

type Heap struct {
	elems []*HeapElem
}

func (h *Heap) Len() int {
	return len(h.elems)
}

func (h *Heap) Less(i, j int) bool {
	if c := bytes.Compare(h.elems[i].Key, h.elems[j].Key); c != 0 {
		return c < 0
	}
	return h.elems[i].TimeIdx < h.elems[j].TimeIdx
}

func (h *Heap) Swap(i, j int) {
	h.elems[i], h.elems[j] = h.elems[j], h.elems[i]
}

func (h *Heap) Push(x *HeapElem) {
	h.elems = append(h.elems, x)
}

func (h *Heap) Pop() *HeapElem {
	old := h.elems
	n := len(old) - 1
	x := old[n]
	//old[n].Key, old[n].Value, old[n].TimeIdx = nil, nil, 0
	old[n] = nil
	h.elems = old[0:n]
	return x
}

// ------ Copy-Paste of `container/heap/heap.go` without interface conversion

// Init establishes the heap invariants required by the other routines in this package.
// Init is idempotent with respect to the heap invariants
// and may be called whenever the heap invariants may have been invalidated.
// The complexity is O(n) where n = h.Len().
func heapInit(h *Heap) {
	// heapify
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

// Push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func heapPush(h *Heap, x *HeapElem) {
	h.Push(x)
	up(h, h.Len()-1)
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
// Pop is equivalent to Remove(h, 0).
func heapPop(h *Heap) *HeapElem {
	n := h.Len() - 1
	h.Swap(0, n)
	down(h, 0, n)
	return h.Pop()
}

func up(h *Heap, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down(h *Heap, i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

// ------ the merge over a sortableBuffer's sorted chunks

// keyPrefix is the first 8 bytes of k, zero-padded. Big-endian, so comparing
// two prefixes as integers orders them the way bytes.Compare would.
func keyPrefix(k []byte) uint64 {
	if len(k) >= 8 {
		return binary.BigEndian.Uint64(k)
	}
	var pad [8]byte // len(k) < 8
	copy(pad[:], k)
	return binary.BigEndian.Uint64(pad[:])
}

// merger walks already-sorted chunks in key order, a cursor per chunk under a
// heap of the cursors that still have entries.
type merger struct {
	heap []*cursor
	cur  []cursor

	// Chunks already in order end to end, which ascending keys produce, are
	// read straight through instead of merged.
	concat bool
	chunk  int // chunk the straight-through cursor sits in
}

// cursor is one chunk's read position. pfx caches the first 8 key bytes, so a
// comparison reads the chunk only when two cursors agree there.
type cursor struct {
	ents []entryLoc
	buf  []byte
	key  []byte
	pfx  uint64
	at   int32
	id   int32 // chunks fill in insertion order, so the lower id wins a tie
}

// advance moves the cursor to its next entry, or reports that it has none.
func (c *cursor) advance() bool {
	if c.at++; int(c.at) >= len(c.ents) {
		return false
	}
	c.key = keyOf(c.buf, c.ents[c.at])
	c.pfx = keyPrefix(c.key)
	return true
}

// rewind puts the cursor on the first entry in key order.
func (m *merger) rewind(chunks []dataChunk) {
	clear(m.cur) // a shorter run would leave the old cursors pinning their chunks
	m.cur = slices.Grow(m.cur[:0], len(chunks))[:len(chunks)]
	for i := range chunks {
		c := &m.cur[i]
		*c = cursor{ents: chunks[i].entries(), buf: chunks[i].buf, at: -1, id: int32(i)} //nolint:gosec
	}
	m.chunk = 0

	if m.concat = m.chunksInOrder(); m.concat {
		for i := range m.cur {
			m.cur[i].at = 0
		}
		return
	}
	m.heap = m.heap[:0]
	for i := range m.cur {
		if c := &m.cur[i]; c.advance() {
			m.heap = append(m.heap, c)
		}
	}
	for i := len(m.heap)/2 - 1; i >= 0; i-- {
		m.siftRoot(i)
	}
}

// next returns the entry the cursor sits on and moves it to the next in key
// order.
func (m *merger) next() ([]byte, entryLoc, bool) {
	if m.concat {
		for ; m.chunk < len(m.cur); m.chunk++ {
			c := &m.cur[m.chunk]
			if int(c.at) < len(c.ents) {
				e := c.ents[c.at]
				c.at++
				return c.buf, e, true
			}
		}
		return nil, 0, false
	}
	if len(m.heap) == 0 {
		return nil, 0, false
	}
	c := m.heap[0]
	buf, e := c.buf, c.ents[c.at]
	if !c.advance() {
		last := len(m.heap) - 1
		m.heap[0], m.heap[last] = m.heap[last], nil
		m.heap = m.heap[:last]
	}
	if len(m.heap) > 0 {
		m.siftRoot(0)
	}
	return buf, e, true
}

func (m *merger) release() {
	clear(m.cur)
	clear(m.heap)
	m.cur, m.heap = m.cur[:0], m.heap[:0]
	m.chunk, m.concat = 0, false
}

// chunksInOrder reports whether every chunk's last key comes before the next
// chunk's first. A tie keeps the earlier chunk, which is insertion order.
func (m *merger) chunksInOrder() bool {
	prev := -1 // last chunk holding anything, so an empty one does not hide a pair
	for i := range m.cur {
		cur := &m.cur[i]
		if len(cur.ents) == 0 {
			continue
		}
		if prev >= 0 {
			p := &m.cur[prev]
			if bytes.Compare(keyOf(p.buf, p.ents[len(p.ents)-1]), keyOf(cur.buf, cur.ents[0])) > 0 {
				return false
			}
		}
		prev = i
	}
	return true
}

// less orders two cursors by the key they sit on. Equal keys keep the order
// they went in, which is the order the chunks filled.
func less(a, b *cursor) bool {
	if a.pfx != b.pfx {
		return a.pfx < b.pfx
	}
	if r := bytes.Compare(a.key, b.key); r != 0 {
		return r < 0
	}
	return a.id < b.id
}

// siftRoot restores the heap under i, whose cursor just moved: sink the hole
// to a leaf taking the smaller child, then climb back until the old value
// fits. One compare a level instead of the usual two, and the cursor that just
// won usually holds a larger key, so the hole nearly always reaches a leaf.
// The climb back is also what makes it serve as the heapify step.
func (m *merger) siftRoot(i int) {
	x, top := m.heap[i], i
	for {
		l := 2*i + 1
		if l >= len(m.heap) {
			break
		}
		if r := l + 1; r < len(m.heap) && less(m.heap[r], m.heap[l]) {
			l = r
		}
		m.heap[i] = m.heap[l]
		i = l
	}
	for i > top {
		p := (i - 1) / 2
		if less(m.heap[p], x) {
			break
		}
		m.heap[i] = m.heap[p]
		i = p
	}
	m.heap[i] = x
}
