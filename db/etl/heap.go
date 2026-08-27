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

// merger walks already-sorted chunks in key order, a cursor per chunk under a
// heap of chunk ids.
type merger struct {
	heap []int32  // chunk ids, ordered by their cursor's key
	cur  []cursor // by chunk id

	// Chunks already in order end to end, which ascending keys produce, are
	// read straight through instead of merged.
	concat bool
	chunk  int // chunk the straight-through cursor sits in
}

type cursor struct {
	ents []entryLoc
	buf  []byte
	at   int32
	key  []byte
}

// rewind puts the cursor on the first entry in key order.
func (m *merger) rewind(chunks []dataChunk) {
	clear(m.cur) // a shorter run would leave the old cursors pinning their chunks
	m.cur = slices.Grow(m.cur[:0], len(chunks))[:len(chunks)]
	for i := range chunks {
		m.cur[i] = cursor{ents: chunks[i].entries(), buf: chunks[i].buf}
	}
	m.chunk = 0

	if m.concat = m.chunksInOrder(); m.concat {
		return
	}
	m.heap = m.heap[:0]
	for i := range m.cur {
		if len(m.cur[i].ents) == 0 {
			continue
		}
		m.load(int32(i)) //nolint:gosec
		m.heap = append(m.heap, int32(i))
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
	id := m.heap[0]
	c := &m.cur[id]
	buf, e := c.buf, c.ents[c.at]
	c.at++
	if int(c.at) == len(c.ents) {
		last := len(m.heap) - 1
		m.heap[0] = m.heap[last]
		m.heap = m.heap[:last]
	} else {
		m.load(id)
	}
	if len(m.heap) > 0 {
		m.siftRoot(0)
	}
	return buf, e, true
}

func (m *merger) release() {
	clear(m.cur)
	m.cur, m.heap = m.cur[:0], m.heap[:0]
	m.chunk, m.concat = 0, false
}

func (m *merger) load(id int32) {
	c := &m.cur[id]
	c.key = keyOf(c.buf, c.ents[c.at])
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

// less orders two cursors by the key they sit on. Chunks fill in insertion
// order, so the lower id wins a tie and equal keys keep the order they went in.
func (m *merger) less(x, y int32) bool {
	if r := bytes.Compare(m.cur[x].key, m.cur[y].key); r != 0 {
		return r < 0
	}
	return x < y
}

// siftRoot restores the heap under i, whose element changed.
func (m *merger) siftRoot(i int) {
	for {
		s, l, r := i, 2*i+1, 2*i+2
		if l < len(m.heap) && m.less(m.heap[l], m.heap[s]) {
			s = l
		}
		if r < len(m.heap) && m.less(m.heap[r], m.heap[s]) {
			s = r
		}
		if s == i {
			return
		}
		m.heap[i], m.heap[s] = m.heap[s], m.heap[i]
		i = s
	}
}
