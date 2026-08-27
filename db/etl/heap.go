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

// HeapElem is the entry a provider currently sits on. pfx caches the first 8
// key bytes, so a comparison reaches Key only when two providers agree there.
type HeapElem struct {
	pfx     uint64
	Key     []byte
	Value   []byte
	TimeIdx int
}

func (e *HeapElem) setKey(k []byte) { e.Key, e.pfx = k, keyPrefix(k) }

// Heap orders providers by the key each one sits on. Providers are numbered in
// the order their files were written, so the lower TimeIdx wins a tie and
// equal keys come out in the order they went in.
type Heap struct {
	elems []*HeapElem
}

func (h *Heap) Len() int { return len(h.elems) }

func lessElem(a, b *HeapElem) bool {
	if a.pfx != b.pfx {
		return a.pfx < b.pfx
	}
	if c := bytes.Compare(a.Key, b.Key); c != 0 {
		return c < 0
	}
	return a.TimeIdx < b.TimeIdx
}

// heapInit orders elems appended in provider order.
func heapInit(h *Heap) {
	for i := len(h.elems)/2 - 1; i >= 0; i-- {
		h.siftRoot(i)
	}
}

// heapFixRoot restores the order after the root provider moved to its next key.
func heapFixRoot(h *Heap) { h.siftRoot(0) }

// heapPopRoot drops the root, whose provider has no more keys.
func heapPopRoot(h *Heap) {
	last := len(h.elems) - 1
	h.elems[0] = h.elems[last]
	h.elems[last] = nil
	h.elems = h.elems[:last]
	if last > 0 {
		h.siftRoot(0)
	}
}

// siftRoot restores the heap under i, whose element changed: sink the hole to
// a leaf taking the smaller child, then climb back until the old element fits.
// One compare a level rather than the two a top-down sift needs, and the
// element that just moved on usually holds a larger key now, so the hole
// nearly always reaches a leaf. The climb back is also the heapify step.
func (h *Heap) siftRoot(i int) {
	x, top, n := h.elems[i], i, len(h.elems)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		if r := l + 1; r < n && lessElem(h.elems[r], h.elems[l]) {
			l = r
		}
		h.elems[i] = h.elems[l]
		i = l
	}
	for i > top {
		p := (i - 1) / 2
		if lessElem(h.elems[p], x) {
			break
		}
		h.elems[i] = h.elems[p]
		i = p
	}
	h.elems[i] = x
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
