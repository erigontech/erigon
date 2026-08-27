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
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dbg"
)

const (
	// SliceBuffer - just simple slice w
	SortableSliceBuffer = iota
	// SortableAppendBuffer - map[k] [v1 v2 v3]
	SortableAppendBuffer
	// SortableOldestAppearedBuffer - buffer that keeps only the oldest entries.
	// if first v1 was added under key K, then v2; only v1 will stay
	SortableOldestAppearedBuffer

	// BufIOSize - 128 pages | default is 1 page | increasing over `64 * 4096` doesn't show speedup on SSD/NVMe, but show speedup in cloud drives
	BufIOSize = 128 * 4096

	entryLocSize    = 4 // sizeof(entryLoc)
	entryHeaderSize = 4 // valLen, in front of the entry's bytes in its chunk
)

// writeSortedEntries writes buffer entries to w in varint-length-prefixed format.
func writeSortedEntries(w io.Writer, entries []sortableBufferEntry) error {
	var numBuf [binary.MaxVarintLen64]byte
	for _, entry := range entries {
		lk := int64(len(entry.key))
		if entry.key == nil {
			lk = -1
		}
		n := binary.PutVarint(numBuf[:], lk)
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err := w.Write(entry.key); err != nil {
			return err
		}
		lv := int64(len(entry.value))
		if entry.value == nil {
			lv = -1
		}
		n = binary.PutVarint(numBuf[:], lv)
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err := w.Write(entry.value); err != nil {
			return err
		}
	}
	return nil
}

var BufferOptimalSize = dbg.EnvDataSize("ETL_OPTIMAL", 256*datasize.MB) /*  var because we want to sometimes change it from tests or command-line flags */

// etlSmallBufRAM (BufferOptimalSize/8) bounds the flush threshold:
// 3_domains * 2 + 3_history * 1 + 4_indices * 2 = 17 etl collectors,
// 17*(256Mb/8) = 544Mb for all collectors combined. Buffers pool their
// chunks — see dataChunks below.
var (
	etlSmallBufRAM       = dbg.EnvDataSize("ETL_SMALL", BufferOptimalSize/8)
	SmallSortableBuffers = NewAllocator(&sync.Pool{
		New: func() any {
			return NewSortableBuffer(etlSmallBufRAM)
		},
	})
)

var (
	etlLargeBufRAM       = BufferOptimalSize
	LargeSortableBuffers = NewAllocator(&sync.Pool{
		New: func() any {
			return NewSortableBuffer(etlLargeBufRAM)
		},
	})
)

const (
	// Each chunk carries the index of its own entries, so an entry addresses
	// only bytes inside its chunk. 1MB is also the least a collector holds
	// once it takes a chunk at all.
	dataChunkBits = 20
	dataChunkSize = 1 << dataChunkBits // 1MB

	// What is left of entryLoc's uint32 once the offset has dataChunkBits,
	// less one for the bias. Sort slices a key out of its chunk, so only a
	// value may outgrow one.
	keyLenBits = 32 - dataChunkBits
	maxKeyLen  = 1<<keyLenBits - 2
)

// dataChunks are shared by all sortableBuffer instances: a buffer takes chunks as
// it fills and gives them back on Reset, instead of pinning its peak size forever.
var dataChunks = sync.Pool{New: func() any {
	c := make([]byte, dataChunkSize)
	return &c
}}

func getDataChunk() []byte { return *dataChunks.Get().(*[]byte) }

// isPooledChunk reports whether c came from the pool. Handing back the private
// chunk an oversized entry gets would let a later getDataChunk give an
// unrelated buffer a chunk of the wrong size.
func isPooledChunk(c []byte) bool { return len(c) == dataChunkSize }

func putDataChunk(c []byte) {
	if !isPooledChunk(c) {
		return
	}
	dataChunks.Put(&c)
}

type Buffer interface {
	// Put does copy `k` and `v`
	Put(k, v []byte)
	// Next returns the entries in key order. Sort must have run since the
	// last Put, and puts the cursor back at the first entry; reading without
	// it panics rather than quietly returning a stale run. The slices point
	// into the buffer's own storage and must not be modified.
	Next() (k, v []byte, ok bool)
	Len() int
	Reset()
	SizeLimit() int
	Prealloc(predictKeysAmount, predictDataAmount int) Buffer
	Write(io.Writer) error
	Sort()
	CheckFlushSize() bool
}

type sortableBufferEntry struct {
	key   []byte
	value []byte
}

var (
	_ Buffer = &sortableBuffer{}
	_ Buffer = &appendSortableBuffer{}
	_ Buffer = &oldestEntrySortableBuffer{}
)

// entryLoc packs an entry's offset inside its chunk with its key length. The
// key length is biased by one, so nil and empty both fail the `> 0` test the
// comparator makes on it. Offsets rise with insertion order, which is what
// orders duplicate keys without a stable sort.
type entryLoc uint32

func makeEntryLoc(keyLenPlus1, offset int32) entryLoc {
	return entryLoc(uint32(keyLenPlus1))<<dataChunkBits | entryLoc(uint32(offset)) //nolint:gosec
}
func (e entryLoc) keyLen() int32 { return int32(e>>dataChunkBits) - 1 } //nolint:gosec
func (e entryLoc) offset() int32 { return int32(e) & (dataChunkSize - 1) }

func NewSortableBuffer(bufferOptimalSize datasize.ByteSize) *sortableBuffer {
	if bufferOptimalSize.Bytes() > math.MaxInt32 {
		panic(fmt.Sprintf("etl: sortableBuffer size %d exceeds MaxInt32", bufferOptimalSize.Bytes()))
	}
	return &sortableBuffer{
		optimalSize: int(bufferOptimalSize.Bytes()),
		sortedN:     -1,
	}
}

// dataChunk holds entry bytes growing up from the front and the index of those
// entries growing down from the back, and is full when the two meet. So the
// index costs no allocation and a buffer's footprint is the chunks it holds.
type dataChunk struct {
	buf    []byte
	entTop int32
}

func keyOf(buf []byte, e entryLoc) []byte {
	if kLen := e.keyLen(); kLen > 0 {
		off := e.offset() + entryHeaderSize
		return buf[off : off+kLen]
	}
	return nil
}

// keyPrefix is the first 8 bytes of k, zero-padded. Big-endian, so comparing
// two prefixes as integers orders them the way bytes.Compare would.
func keyPrefix(k []byte) uint64 {
	if len(k) >= 8 {
		return binary.BigEndian.Uint64(k)
	}
	var pad [8]byte
	copy(pad[:], k)
	return binary.BigEndian.Uint64(pad[:])
}

func (c *dataChunk) len() int { return (len(c.buf) - int(c.entTop)) / entryLocSize }

// entries views the chunk's index as entryLoc. Sort leaves it in key order;
// before that it runs newest-first, because it grows downward.
func (c *dataChunk) entries() []entryLoc {
	n := c.len()
	if n == 0 {
		return nil
	}
	// Aligned: nextChunk checks the chunk itself, and entTop starts at a
	// multiple of entryLocSize and only moves by it.
	return unsafe.Slice((*entryLoc)(unsafe.Pointer(&c.buf[c.entTop])), n)
}

type sortableBuffer struct {
	// The chunk being filled, hoisted out of chunks so Put touches only this
	// struct. syncCur writes it back, and has to run before anything reads
	// the last chunk.
	curBuf []byte
	curEnd int32 // data grows up to here
	curTop int32 // the index grows down to here
	n      int

	chunks []dataChunk

	// Sort orders each chunk on its own, so reading in key order is a k-way
	// merge over the chunks.
	mrg     merger
	sortedN int // n as of the last Sort; -1 while unsorted

	chunkBytes  int
	optimalSize int
}

// chunkSizeFor returns the size of a chunk able to hold an entry of n bytes
// and its index slot.
func chunkSizeFor(n int) int {
	if size := n + entryLocSize; size > dataChunkSize {
		return (size + entryLocSize - 1) &^ (entryLocSize - 1)
	}
	return dataChunkSize
}

// nextChunk starts a chunk able to hold an entry of n bytes and its index
// slot. An entry never straddles chunks, so Next hands out direct references.
func (b *sortableBuffer) nextChunk(n int) {
	size := chunkSizeFor(n)
	var buf []byte
	if size == dataChunkSize {
		buf = getDataChunk()
	} else {
		if size > math.MaxInt32 {
			panic(fmt.Sprintf("etl: entry of %d bytes needs a chunk of %d, over %d", n, size, math.MaxInt32))
		}
		buf = make([]byte, size)
	}
	if uintptr(unsafe.Pointer(&buf[0]))%entryLocSize != 0 {
		panic("etl: chunk is not aligned for its entry index")
	}
	b.syncCur()
	b.chunks = append(b.chunks, dataChunk{buf: buf, entTop: int32(len(buf))}) //nolint:gosec
	b.curBuf, b.curEnd, b.curTop = buf, 0, int32(len(buf))                    //nolint:gosec
	b.chunkBytes += len(buf)
}

func (b *sortableBuffer) syncCur() {
	if len(b.chunks) == 0 {
		return
	}
	c := &b.chunks[len(b.chunks)-1]
	c.entTop = b.curTop
}

// Put adds key and value to the buffer. These slices will not be accessed later,
// so no copying is necessary
func (b *sortableBuffer) Put(k, v []byte) {
	lk, lv := len(k), len(v)
	n := entryHeaderSize + lk + lv
	off := int(b.curEnd)
	// One test for both, so the fast path holds no call and Put keeps its
	// arguments in registers.
	if lk > maxKeyLen || off+n+entryLocSize > int(b.curTop) {
		b.putSlow(k, v)
		return
	}
	kLen, vLen := int32(0), int32(-1)
	if k != nil {
		kLen = int32(lk) + 1 //nolint:gosec
	}
	if v != nil {
		vLen = int32(lv) //nolint:gosec
	}
	// Sliced to exactly n, so the two copies below take the length from the
	// destination and the compiler drops the min against the source.
	data := b.curBuf[off : off+n : off+n]
	binary.NativeEndian.PutUint32(data, uint32(vLen)) //nolint:gosec
	b.curTop -= entryLocSize
	binary.NativeEndian.PutUint32(b.curBuf[b.curTop:], uint32(makeEntryLoc(kLen, int32(off)))) //nolint:gosec
	b.curEnd = int32(off + n)                                                                  //nolint:gosec
	b.n++
	copy(data[entryHeaderSize:entryHeaderSize+lk], k)
	copy(data[entryHeaderSize+lk:], v)
}

// putSlow handles what Put's single guard rejects: a key too long to index,
// and an entry the current chunk has no room for. nextChunk always leaves
// room, so the retry cannot come back here.
//
//go:noinline
func (b *sortableBuffer) putSlow(k, v []byte) {
	if len(k) > maxKeyLen {
		panic(fmt.Sprintf("etl: key of %d bytes exceeds %d", len(k), maxKeyLen))
	}
	b.nextChunk(entryHeaderSize + len(k) + len(v))
	b.Put(k, v)
}

// Size counts every chunk taken, less what is free in the one being filled.
// The entry index lives inside the chunks, so it is counted.
func (b *sortableBuffer) Size() int { return b.chunkBytes - int(b.curTop-b.curEnd) }

func (b *sortableBuffer) Len() int { return b.n }

// Next returns the entry the read cursor sits on and moves it along. The
// buffer carries the merge state, so no two goroutines may read at once.
func (b *sortableBuffer) Next() ([]byte, []byte, bool) {
	if b.sortedN != b.n {
		panic("etl: Next before Sort")
	}
	buf, e, ok := b.mrg.next()
	if !ok {
		return nil, nil, false
	}
	data := buf[e.offset():]
	kLen := e.keyLen()
	vLen := int32(binary.NativeEndian.Uint32(data)) //nolint:gosec
	data = data[entryHeaderSize:]
	var key, val []byte
	if kLen >= 0 {
		key = data[:kLen:kLen]
		data = data[kLen:]
	}
	if vLen >= 0 {
		val = data[:vLen:vLen]
	}
	return key, val, true
}

// Prealloc only reserves room for the chunk headers. The chunks come from
// their pool one at a time, so an idle buffer holds nothing.
func (b *sortableBuffer) Prealloc(_, predictDataSize int) Buffer {
	if n := predictDataSize/dataChunkSize + 1; cap(b.chunks) < n {
		b.chunks = slices.Grow(b.chunks, n)
	}
	return b
}

func (b *sortableBuffer) Reset() {
	b.mrg.release() // cursors alias the chunks, so drop them before the pool takes them
	for i := range b.chunks {
		putDataChunk(b.chunks[i].buf)
	}
	clear(b.chunks)
	b.chunks = b.chunks[:0]
	b.curBuf, b.curEnd, b.curTop = nil, 0, 0
	b.n, b.chunkBytes = 0, 0
	b.sortedN = -1
}

func (b *sortableBuffer) SizeLimit() int { return b.optimalSize }

// Sort orders each chunk on its own, so it stays inside 1MB however large the
// buffer is; reading the buffer back merges the runs.
func (b *sortableBuffer) Sort() {
	if b.sortedN != b.n {
		b.syncCur()
		for i := range b.chunks {
			b.chunks[i].sort()
		}
		b.sortedN = b.n
	}
	b.mrg.rewind(b.chunks)
}

// sort orders the chunk's index by the keys it holds.
func (c *dataChunk) sort() {
	ents := c.entries()
	if len(ents) < 2 {
		return
	}
	buf := c.buf
	// Key extraction stays inside cmp: the comparator is called indirectly,
	// so a separate closure never inlines and costs a call per key.
	cmp := func(x, y entryLoc) int {
		var xk, yk []byte
		if kLen := x.keyLen(); kLen > 0 {
			off := x.offset() + entryHeaderSize
			xk = buf[off : off+kLen]
		}
		if kLen := y.keyLen(); kLen > 0 {
			off := y.offset() + entryHeaderSize
			yk = buf[off : off+kLen]
		}
		if r := bytes.Compare(xk, yk); r != 0 {
			return r
		}
		return int(x.offset() - y.offset()) // StableSort: offsets rise with insertion order
	}
	// The index grows downward, so ascending keys arrive reversed. pdqsort
	// spots that too, but only after sampling for a pivot.
	for j := 1; j < len(ents); j++ {
		if cmp(ents[j-1], ents[j]) < 0 {
			slices.SortFunc(ents, cmp)
			return
		}
	}
	slices.Reverse(ents)
}

// merger walks already-sorted chunks in key order, a cursor per chunk under a
// heap of chunk ids. The prefixes live apart from the cursors because the heap
// reads them on nearly every comparison and they stay in L1.
type merger struct {
	heap []int32  // chunk ids, ordered by their cursor's key
	pfx  []uint64 // each cursor's key prefix, by chunk id
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
	m.pfx = slices.Grow(m.pfx[:0], len(chunks))[:len(chunks)]
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
		m.siftDown(i)
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
		m.siftRoot()
	}
	return buf, e, true
}

func (m *merger) release() {
	clear(m.cur)
	m.cur, m.pfx, m.heap = m.cur[:0], m.pfx[:0], m.heap[:0]
	m.chunk, m.concat = 0, false
}

func (m *merger) load(id int32) {
	c := &m.cur[id]
	c.key = keyOf(c.buf, c.ents[c.at])
	m.pfx[id] = keyPrefix(c.key)
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
	if px, py := m.pfx[x], m.pfx[y]; px != py {
		return px < py
	}
	if r := bytes.Compare(m.cur[x].key, m.cur[y].key); r != 0 {
		return r < 0
	}
	return x < y
}

// siftRoot restores the heap after the root's cursor moved on: sink the hole
// to a leaf taking the smaller child, then climb back until the old root fits.
// One compare a level instead of siftDown's two, and the cursor that just won
// usually holds a larger key, so the hole nearly always reaches a leaf.
func (m *merger) siftRoot() {
	x, i := m.heap[0], 0
	for {
		l := 2*i + 1
		if l >= len(m.heap) {
			break
		}
		if r := l + 1; r < len(m.heap) && m.less(m.heap[r], m.heap[l]) {
			l = r
		}
		m.heap[i] = m.heap[l]
		i = l
	}
	for i > 0 {
		p := (i - 1) / 2
		if m.less(m.heap[p], x) {
			break
		}
		m.heap[i] = m.heap[p]
		i = p
	}
	m.heap[i] = x
}

func (m *merger) siftDown(i int) {
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

func (b *sortableBuffer) CheckFlushSize() bool {
	return b.Size() >= b.optimalSize
}

func (b *sortableBuffer) Write(w io.Writer) error {
	b.Sort() // Write drives the read cursor, so put it back at the first entry
	var numBuf [binary.MaxVarintLen64]byte
	for {
		k, v, ok := b.Next()
		if !ok {
			return nil
		}
		// writeSortedEntries says the same, but a call per field does not
		// inline and costs Write more than the duplication does.
		lk := int64(len(k))
		if k == nil {
			lk = -1
		}
		n := binary.PutVarint(numBuf[:], lk)
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if len(k) > 0 {
			if _, err := w.Write(k); err != nil {
				return err
			}
		}
		lv := int64(len(v))
		if v == nil {
			lv = -1
		}
		n = binary.PutVarint(numBuf[:], lv)
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if len(v) > 0 {
			if _, err := w.Write(v); err != nil {
				return err
			}
		}
	}
}

func NewAppendBuffer(bufferOptimalSize datasize.ByteSize) *appendSortableBuffer {
	return &appendSortableBuffer{
		entries:     make(map[string][]byte),
		size:        0,
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type appendSortableBuffer struct {
	entries     map[string][]byte
	sortedBuf   []sortableBufferEntry
	at          int
	unsorted    bool // sortedBuf does not hold what entries does
	size        int
	optimalSize int
}

func (b *appendSortableBuffer) Put(k, v []byte) {
	stored, ok := b.entries[string(k)]
	if !ok {
		b.size += len(k)
	}
	b.size += len(v)
	b.entries[string(k)] = append(stored, v...)
	b.unsorted = true
}

func (b *appendSortableBuffer) Size() int      { return b.size }
func (b *appendSortableBuffer) SizeLimit() int { return b.optimalSize }

func (b *appendSortableBuffer) Len() int {
	return len(b.entries)
}

func (b *appendSortableBuffer) Sort() {
	b.sortedBuf, b.at, b.unsorted = b.sortedBuf[:0], 0, false
	if cap(b.sortedBuf) < len(b.entries) {
		b.sortedBuf = make([]sortableBufferEntry, 0, len(b.entries))
	}
	for key, val := range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(key), value: val})
	}
	sort.Sort(b) // Doesn't need `sort.Stable` because this buffer type can't produce duplicated values
}

func (b *appendSortableBuffer) Less(i, j int) bool {
	return bytes.Compare(b.sortedBuf[i].key, b.sortedBuf[j].key) < 0
}

func (b *appendSortableBuffer) Swap(i, j int) {
	b.sortedBuf[i], b.sortedBuf[j] = b.sortedBuf[j], b.sortedBuf[i]
}

func (b *appendSortableBuffer) Next() ([]byte, []byte, bool) {
	if b.unsorted {
		panic("etl: Next before Sort")
	}
	if b.at >= len(b.sortedBuf) {
		return nil, nil, false
	}
	e := b.sortedBuf[b.at]
	b.at++
	return e.key, e.value, true
}

func (b *appendSortableBuffer) Reset() {
	b.sortedBuf = nil
	b.at, b.unsorted = 0, false
	b.entries = make(map[string][]byte)
	b.size = 0
}

func (b *appendSortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) Buffer {
	b.entries = make(map[string][]byte, predictKeysAmount) // maps have no cap(), always recreate
	if cap(b.sortedBuf) < predictKeysAmount {
		b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount)
	}
	return b
}

func (b *appendSortableBuffer) Write(w io.Writer) error {
	b.Sort()
	return writeSortedEntries(w, b.sortedBuf)
}

func (b *appendSortableBuffer) CheckFlushSize() bool {
	return b.size >= b.optimalSize
}

func NewOldestEntryBuffer(bufferOptimalSize datasize.ByteSize) *oldestEntrySortableBuffer {
	return &oldestEntrySortableBuffer{
		entries:     make(map[string][]byte),
		size:        0,
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type oldestEntrySortableBuffer struct {
	entries     map[string][]byte
	sortedBuf   []sortableBufferEntry
	at          int
	unsorted    bool // sortedBuf does not hold what entries does
	size        int
	optimalSize int
}

func (b *oldestEntrySortableBuffer) Put(k, v []byte) {
	_, ok := b.entries[string(k)]
	if ok {
		// if we already had this entry, we are going to keep it and ignore new value
		return
	}

	b.size += len(k)*2 + len(v)
	b.entries[string(k)] = bytes.Clone(v)
	b.unsorted = true
}

func (b *oldestEntrySortableBuffer) Size() int      { return b.size }
func (b *oldestEntrySortableBuffer) SizeLimit() int { return b.optimalSize }

func (b *oldestEntrySortableBuffer) Len() int {
	return len(b.entries)
}

func (b *oldestEntrySortableBuffer) Sort() {
	b.sortedBuf, b.at, b.unsorted = b.sortedBuf[:0], 0, false
	if cap(b.sortedBuf) < len(b.entries) {
		b.sortedBuf = make([]sortableBufferEntry, 0, len(b.entries))
	}
	for k, v := range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(k), value: v})
	}
	sort.Sort(b) // Doesn't need `sort.Stable` because this buffer type can't produce duplicated values
}

func (b *oldestEntrySortableBuffer) Less(i, j int) bool {
	return bytes.Compare(b.sortedBuf[i].key, b.sortedBuf[j].key) < 0
}

func (b *oldestEntrySortableBuffer) Swap(i, j int) {
	b.sortedBuf[i], b.sortedBuf[j] = b.sortedBuf[j], b.sortedBuf[i]
}

func (b *oldestEntrySortableBuffer) Next() ([]byte, []byte, bool) {
	if b.unsorted {
		panic("etl: Next before Sort")
	}
	if b.at >= len(b.sortedBuf) {
		return nil, nil, false
	}
	e := b.sortedBuf[b.at]
	b.at++
	return e.key, e.value, true
}

func (b *oldestEntrySortableBuffer) Reset() {
	b.sortedBuf = nil
	b.at, b.unsorted = 0, false
	b.entries = make(map[string][]byte)
	b.size = 0
}

func (b *oldestEntrySortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) Buffer {
	b.entries = make(map[string][]byte, predictKeysAmount) // maps have no cap(), always recreate
	if cap(b.sortedBuf) < predictKeysAmount {
		b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount)
	}
	return b
}

func (b *oldestEntrySortableBuffer) Write(w io.Writer) error {
	b.Sort()
	return writeSortedEntries(w, b.sortedBuf)
}

func (b *oldestEntrySortableBuffer) CheckFlushSize() bool {
	return b.size >= b.optimalSize
}

func getBufferByType(tp int, size datasize.ByteSize) Buffer {
	switch tp {
	case SortableSliceBuffer:
		return NewSortableBuffer(size)
	case SortableAppendBuffer:
		return NewAppendBuffer(size)
	case SortableOldestAppearedBuffer:
		return NewOldestEntryBuffer(size)
	default:
		panic("unknown buffer type " + strconv.Itoa(tp))
	}
}

func getTypeByBuffer(b Buffer) int {
	switch b.(type) {
	case *sortableBuffer:
		return SortableSliceBuffer
	case *appendSortableBuffer:
		return SortableAppendBuffer
	case *oldestEntrySortableBuffer:
		return SortableOldestAppearedBuffer
	default:
		panic(fmt.Sprintf("unknown buffer type: %T ", b))
	}
}
