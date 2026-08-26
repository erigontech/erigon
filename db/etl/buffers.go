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

var (
	// etlSmallBufRAM (BufferOptimalSize/8) bounds the flush threshold:
	// 3_domains * 2 + 3_history * 1 + 4_indices * 2 = 17 etl collectors,
	// 17*(256Mb/8) = 512Mb for all collectors combined. Buffers pool their
	// chunks — see dataChunks below.
	etlSmallBufRAM       = dbg.EnvDataSize("ETL_SMALL", BufferOptimalSize/8)
	SmallSortableBuffers = NewAllocator(&sync.Pool{
		New: func() any {
			// Sortable Buffer now pre-allocs only metadata arrays not internal buffers for data-holding (they are-preallocated and have own sync.Pool)
			return NewSortableBuffer(etlSmallBufRAM).Prealloc(512, int(etlSmallBufRAM))
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
	// sortableBuffer stores key/value bytes in chunks of a power-of-two size,
	// each chunk carrying the index of its own entries, so an entry only has to
	// address bytes inside its chunk. 1MB is also the least a collector can hold
	// once it takes a chunk at all.
	dataChunkBits = 20
	dataChunkSize = 1 << dataChunkBits // 1MB

	// entryLoc gives the offset dataChunkBits and the key length what is left of
	// a uint32, biased by one. Sort also slices a key straight out of its chunk,
	// so only a value may outgrow one.
	keyLenBits = 32 - dataChunkBits
	maxKeyLen  = 1<<keyLenBits - 2

	// One buffer holds at most maxDataChunks*dataChunkSize bytes (~2GB), which
	// keeps its size inside a positive int32; nextChunk panics past that.
	// NewSortableBuffer's MaxInt32 bound on optimalSize does not fully rule this
	// out, since Put can grow the buffer past optimalSize before CheckFlushSize
	// is checked.
	maxDataChunks = math.MaxInt32>>dataChunkBits + 1
)

// dataChunks are shared by all sortableBuffer instances: a buffer takes chunks as
// it fills and gives them back on Reset, instead of pinning its peak size forever.
var dataChunks = sync.Pool{New: func() any {
	c := make([]byte, dataChunkSize)
	return &c
}}

func getDataChunk() []byte { return *dataChunks.Get().(*[]byte) }

func putDataChunk(c []byte) {
	if len(c) != dataChunkSize { // private chunk of an oversized entry
		return
	}
	dataChunks.Put(&c)
}

type Buffer interface {
	// Put does copy `k` and `v`
	Put(k, v []byte)
	// Get returns direct references to the internal key/value storage without copying.
	// The returned slices must not be modified by the caller.
	Get(i int) ([]byte, []byte)
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

// entryLoc packs the offset of an entry inside its chunk with the entry's key
// length. The key length is stored biased by one, so nil and empty both fail
// the `> 0` test the comparator makes on it. Offsets rise with insertion order,
// which is what lets Sort order duplicate keys without a stable sort.
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

// dataChunk holds the bytes of the entries put into it, growing up from the
// front, and the index of those entries, growing down from the back. The chunk
// is full when the two meet, so the index costs no allocation of its own and a
// buffer's whole footprint is the chunks it holds.
type dataChunk struct {
	buf     []byte
	dataEnd int32
	entTop  int32

	// The merge reads these instead of chasing the index on every compare.
	// ents drains from the front, so ents[0] is the chunk's current entry and
	// key and pfx describe it.
	ents []entryLoc
	key  []byte
	pfx  uint64
}

// loadKey caches the current entry's key and its prefix.
func (c *dataChunk) loadKey() {
	c.key = nil
	if e := c.ents[0]; e.keyLen() > 0 {
		off := e.offset() + entryHeaderSize
		c.key = c.buf[off : off+e.keyLen()]
	}
	c.pfx = keyPrefix(c.key)
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
	// A chunk is at least dataChunkSize, so the runtime hands it back
	// page-aligned, and entTop only ever moves by entryLocSize.
	return unsafe.Slice((*entryLoc)(unsafe.Pointer(&c.buf[c.entTop])), n)
}

type sortableBuffer struct {
	chunks []dataChunk
	// cur is &chunks[len-1]. nextChunk is the only place that appends to
	// chunks, and it re-takes the pointer; Prealloc leaves a filled buffer
	// alone for the same reason.
	cur  *dataChunk
	free int32 // bytes left in cur
	n    int

	// Sort orders each chunk on its own, so reading the buffer in key order is
	// a k-way merge over the chunks.
	heap    []int32 // chunk ids, ordered by their current entry
	at      int     // index of the entry the cursor sits on
	sortedN int     // n as of the last Sort; -1 while unsorted

	chunkBytes  int
	optimalSize int
}

// nextChunk starts a chunk able to hold an entry of n bytes and its index slot.
// An entry never straddles chunks, so Get can hand out direct references.
func (b *sortableBuffer) nextChunk(n int32) {
	if len(b.chunks) >= maxDataChunks {
		panic(fmt.Sprintf("etl: sortableBuffer exceeded %d chunks", maxDataChunks))
	}
	size := int(n) + entryLocSize
	var buf []byte
	if size > dataChunkSize {
		buf = make([]byte, (size+entryLocSize-1)&^(entryLocSize-1))
	} else {
		buf = getDataChunk()
	}
	b.chunks = append(b.chunks, dataChunk{buf: buf, entTop: int32(len(buf))}) //nolint:gosec
	b.cur = &b.chunks[len(b.chunks)-1]
	b.chunkBytes += len(buf)
	b.free = int32(len(buf)) //nolint:gosec
}

// Put adds key and value to the buffer. These slices will not be accessed later,
// so no copying is necessary
func (b *sortableBuffer) Put(k, v []byte) {
	if len(k) > maxKeyLen {
		panic(fmt.Sprintf("etl: key of %d bytes exceeds %d", len(k), maxKeyLen))
	}
	kLen, vLen := int32(len(k))+1, int32(len(v)) //nolint:gosec
	if k == nil {
		kLen = 0
	}
	if v == nil {
		vLen = -1
	}
	n := int32(entryHeaderSize + len(k) + len(v)) //nolint:gosec
	if n+entryLocSize > b.free {
		b.nextChunk(n)
	}
	c := b.cur
	off := c.dataEnd
	data := c.buf[off:]
	binary.NativeEndian.PutUint32(data, uint32(vLen)) //nolint:gosec
	copy(data[entryHeaderSize:], k)
	copy(data[entryHeaderSize+len(k):], v)
	c.dataEnd = off + n
	c.entTop -= entryLocSize
	binary.NativeEndian.PutUint32(c.buf[c.entTop:], uint32(makeEntryLoc(kLen, off)))
	b.free -= n + entryLocSize
	b.n++
}

// Size counts the bytes of every chunk taken, minus what is still free in the
// one being filled. The entry index lives inside the chunks, so it is counted.
func (b *sortableBuffer) Size() int { return b.chunkBytes - int(b.free) }

func (b *sortableBuffer) Len() int { return b.n }

func (b *sortableBuffer) Get(i int) ([]byte, []byte) {
	buf, e := b.entryAt(i)
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
	return key, val
}

// entryAt returns the i-th entry and the chunk holding it: key order once Sort
// has run over the current contents, insertion order before that. Walking i
// upward costs O(1) a step; going back restarts the merge.
func (b *sortableBuffer) entryAt(i int) ([]byte, entryLoc) {
	if b.sortedN != b.n {
		for k := range b.chunks {
			c := &b.chunks[k]
			m := c.len()
			if i < m {
				return c.buf, c.entries()[m-1-i]
			}
			i -= m
		}
		panic(fmt.Sprintf("etl: entry %d out of range", i))
	}
	if len(b.chunks) == 1 { // one run, nothing to merge
		c := &b.chunks[0]
		return c.buf, c.ents[i]
	}
	if i < b.at {
		b.rewind()
	}
	for b.at < i {
		b.advance()
	}
	c := &b.chunks[b.heap[0]]
	return c.buf, c.ents[0]
}

// Prealloc only reserves room for the chunk headers. The chunks themselves are
// still taken from their pool one at a time and carry the entry index with
// them, so an idle buffer holds nothing.
func (b *sortableBuffer) Prealloc(_, predictDataSize int) Buffer {
	if len(b.chunks) > 0 { // moving the slice would strand cur
		return b
	}
	if n := predictDataSize/dataChunkSize + 1; cap(b.chunks) < n {
		b.chunks = slices.Grow(b.chunks, n)
	}
	return b
}

func (b *sortableBuffer) Reset() {
	for i := range b.chunks {
		putDataChunk(b.chunks[i].buf)
	}
	clear(b.chunks)
	b.chunks, b.heap = b.chunks[:0], b.heap[:0]
	b.cur = nil
	b.free, b.n, b.at, b.chunkBytes = 0, 0, 0, 0
	b.sortedN = -1
}

func (b *sortableBuffer) SizeLimit() int { return b.optimalSize }

// Sort orders each chunk's index on its own. A chunk's keys are the only bytes
// it reads, so the sort stays inside 1MB however large the buffer is; reading
// the buffer back merges the runs.
func (b *sortableBuffer) Sort() {
	if b.sortedN == b.n {
		return
	}
	for i := range b.chunks {
		c := &b.chunks[i]
		ents := c.entries()
		if len(ents) < 2 {
			continue
		}
		buf := c.buf
		// Key extraction stays inside cmp: pdqsortCmpFunc calls the comparator
		// indirectly, so a separate closure never inlines and costs a call per key.
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
		// The index grows downward, so keys put in ascending order arrive reversed.
		desc := true
		for j := 1; j < len(ents); j++ {
			if cmp(ents[j-1], ents[j]) < 0 {
				desc = false
				break
			}
		}
		if desc {
			slices.Reverse(ents)
			continue
		}
		if slices.IsSortedFunc(ents, cmp) {
			continue
		}
		slices.SortFunc(ents, cmp)
	}
	b.sortedN = b.n
	b.rewind()
}

// rewind restarts the merge at the first entry in key order.
func (b *sortableBuffer) rewind() {
	b.heap = b.heap[:0]
	for i := range b.chunks {
		c := &b.chunks[i]
		c.ents = c.entries()
		if len(c.ents) == 0 {
			continue
		}
		c.loadKey()
		b.heap = append(b.heap, int32(i)) //nolint:gosec
	}
	for i := len(b.heap)/2 - 1; i >= 0; i-- {
		b.siftDown(i)
	}
	b.at = 0
}

// advance moves the cursor to the next entry in key order.
func (b *sortableBuffer) advance() {
	c := &b.chunks[b.heap[0]]
	c.ents = c.ents[1:]
	if len(c.ents) == 0 {
		last := len(b.heap) - 1
		b.heap[0] = b.heap[last]
		b.heap = b.heap[:last]
	} else {
		c.loadKey()
	}
	if len(b.heap) > 0 {
		b.siftDown(0)
	}
	b.at++
}

// less orders two chunks by their current entry. Chunks fill in insertion
// order, so the lower id wins a tie and equal keys come back in the order they
// went in.
func (b *sortableBuffer) less(x, y int32) bool {
	cx, cy := &b.chunks[x], &b.chunks[y]
	if cx.pfx != cy.pfx {
		return cx.pfx < cy.pfx
	}
	if r := bytes.Compare(cx.key, cy.key); r != 0 {
		return r < 0
	}
	return x < y
}

func (b *sortableBuffer) siftDown(i int) {
	for {
		m, l, r := i, 2*i+1, 2*i+2
		if l < len(b.heap) && b.less(b.heap[l], b.heap[m]) {
			m = l
		}
		if r < len(b.heap) && b.less(b.heap[r], b.heap[m]) {
			m = r
		}
		if m == i {
			return
		}
		b.heap[i], b.heap[m] = b.heap[m], b.heap[i]
		i = m
	}
}

func (b *sortableBuffer) CheckFlushSize() bool {
	return b.Size() >= b.optimalSize
}

func (b *sortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	for i := range b.n {
		buf, e := b.entryAt(i)
		data := buf[e.offset():]
		kLen32 := e.keyLen()
		vLen32 := int32(binary.NativeEndian.Uint32(data)) //nolint:gosec
		kLen, vLen := int(kLen32), int(vLen32)
		data = data[entryHeaderSize:]
		// write key
		n := binary.PutVarint(numBuf[:], int64(kLen32))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if kLen > 0 {
			if _, err := w.Write(data[:kLen]); err != nil {
				return err
			}
			data = data[kLen:]
		}
		// write value
		n = binary.PutVarint(numBuf[:], int64(vLen32))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if vLen > 0 {
			if _, err := w.Write(data[:vLen]); err != nil {
				return err
			}
		}
	}
	return nil
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
}

func (b *appendSortableBuffer) Size() int      { return b.size }
func (b *appendSortableBuffer) SizeLimit() int { return b.optimalSize }

func (b *appendSortableBuffer) Len() int {
	return len(b.entries)
}

func (b *appendSortableBuffer) Sort() {
	b.sortedBuf = b.sortedBuf[:0]
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

func (b *appendSortableBuffer) Get(i int) ([]byte, []byte) {
	return b.sortedBuf[i].key, b.sortedBuf[i].value
}

func (b *appendSortableBuffer) Reset() {
	b.sortedBuf = nil
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
}

func (b *oldestEntrySortableBuffer) Size() int      { return b.size }
func (b *oldestEntrySortableBuffer) SizeLimit() int { return b.optimalSize }

func (b *oldestEntrySortableBuffer) Len() int {
	return len(b.entries)
}

func (b *oldestEntrySortableBuffer) Sort() {
	b.sortedBuf = b.sortedBuf[:0]
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

func (b *oldestEntrySortableBuffer) Get(i int) ([]byte, []byte) {
	return b.sortedBuf[i].key, b.sortedBuf[i].value
}

func (b *oldestEntrySortableBuffer) Reset() {
	b.sortedBuf = nil
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
