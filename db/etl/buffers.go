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
	entryLocAlign   = 4 // what a chunk's length has to be a multiple of, so the index lands aligned
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
	// value may outgrow one. Exported because Put panics past it, so callers
	// that size their own keys have to check against it.
	MaxKeyLen = 1<<(32-dataChunkBits) - 2
)

// dataChunks are shared by all sortableBuffer instances: a buffer takes chunks as
// it fills and gives them back on Reset, instead of pinning its peak size forever.
var dataChunks = sync.Pool{New: func() any {
	c := make([]byte, dataChunkSize)
	return &c
}}

func getDataChunk() *[]byte { return dataChunks.Get().(*[]byte) }

// putDataChunk hands a chunk back, at no allocation since ref is the pointer
// the pool gave out. A chunk sized for one oversized entry has no ref, so it
// cannot reach the pool and be handed to a buffer expecting dataChunkSize.
func putDataChunk(ref *[]byte) {
	if ref != nil {
		dataChunks.Put(ref)
	}
}

type Buffer interface {
	// Put does copy `k` and `v`
	Put(k, v []byte)
	// Next returns the entries in key order, one goroutine at a time. Sort
	// must have run since the last Put; reading without it panics rather
	// than quietly returning a stale run. The slices point into the
	// buffer's own storage and must not be modified.
	Next() (k, v []byte, ok bool)
	// Rewind puts the read cursor back at the first entry. Write does it
	// too, so a read interleaved with a Write starts over rather than
	// resuming where the read left off.
	Rewind()
	Len() int
	Reset()
	// Size is what CheckFlushSize compares against SizeLimit. Each buffer
	// counts what it actually holds, which for the chunked one is whole
	// chunks - index and unfilled tail included.
	Size() int
	SizeLimit() int
	Prealloc(predictKeysAmount, predictDataAmount int) Buffer
	Write(io.Writer) error
	Sort()
	CheckFlushSize() bool
}

// mustBeSorted is what every Next calls: a buffer read after a Put and before
// a Sort would hand back the previous run, which duplicates rows silently.
func mustBeSorted(unsorted bool) {
	if unsorted {
		panic("etl: Next before Sort")
	}
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
	ref    *[]byte // what the pool gave out; nil means this chunk cannot go back
	entTop int32
}

func keyOf(buf []byte, e entryLoc) []byte {
	if kLen := e.keyLen(); kLen > 0 {
		off := e.offset() + entryHeaderSize
		return buf[off : off+kLen]
	}
	return nil
}

// entries views the chunk's index as entryLoc. Sort leaves it in key order;
// before that it runs newest-first, because it grows downward.
func (c *dataChunk) entries() []entryLoc {
	n := (len(c.buf) - int(c.entTop)) / entryLocSize
	if n == 0 {
		return nil
	}
	// Aligned: nextChunk checks len(buf), and entTop starts there and only
	// moves by entryLocSize, which is a multiple of entryLocAlign.
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
// and its index slot, or 0 if no chunk can. Bounding n first matters: the
// rounding below wraps for n near math.MaxInt, and a wrapped size would come
// back small enough to pass for a pooled chunk.
func chunkSizeFor(n int) int {
	if n > math.MaxInt32-entryLocSize-entryLocAlign {
		return 0
	}
	if size := n + entryLocSize; size > dataChunkSize {
		return (size + entryLocAlign - 1) &^ (entryLocAlign - 1)
	}
	return dataChunkSize
}

// nextChunk starts a chunk able to hold an entry of n bytes and its index
// slot. An entry never straddles chunks, so Next hands out direct references.
func (b *sortableBuffer) nextChunk(n int) {
	size := chunkSizeFor(n)
	if size == 0 {
		panic(fmt.Sprintf("etl: no chunk can hold an entry of %d bytes", n))
	}
	var buf []byte
	var ref *[]byte
	if size == dataChunkSize {
		ref = getDataChunk()
		buf = *ref
	} else {
		buf = make([]byte, size)
	}
	// entries() views the tail from entTop as []entryLoc. entTop starts at
	// len(buf) and only moves by entryLocSize, so this is what keeps it
	// aligned; the allocator already covers the pointer itself.
	if len(buf)%entryLocAlign != 0 {
		panic("etl: chunk length is not a multiple of the index slot")
	}
	b.syncCur()
	b.chunks = append(b.chunks, dataChunk{buf: buf, ref: ref, entTop: int32(len(buf))}) //nolint:gosec
	b.curBuf, b.curEnd, b.curTop = buf, 0, int32(len(buf))                              //nolint:gosec
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
	if lk > MaxKeyLen || off+n+entryLocSize > int(b.curTop) {
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
	if len(k) > MaxKeyLen {
		panic(fmt.Sprintf("etl: key of %d bytes exceeds %d", len(k), MaxKeyLen))
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
	mustBeSorted(b.sortedN != b.n)
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
	// The cursors and curBuf alias the chunks, so drop them before the pool
	// hands those chunks to another buffer.
	b.mrg.release()
	b.curBuf, b.curEnd, b.curTop = nil, 0, 0
	for i := range b.chunks {
		putDataChunk(b.chunks[i].ref)
	}
	clear(b.chunks)
	b.chunks = b.chunks[:0]
	b.n, b.chunkBytes = 0, 0
	b.sortedN = -1
}

func (b *sortableBuffer) SizeLimit() int { return b.optimalSize }

// Sort orders each chunk on its own, so it stays inside 1MB however large the
// buffer is; reading the buffer back merges the runs.
func (b *sortableBuffer) Sort() {
	if b.sortedN == b.n {
		return
	}
	b.syncCur()
	for i := range b.chunks {
		b.chunks[i].sort()
	}
	b.sortedN = b.n
	b.mrg.rewind(b.chunks)
}

// Rewind puts the read cursor back at the first entry in key order.
func (b *sortableBuffer) Rewind() {
	mustBeSorted(b.sortedN != b.n)
	b.mrg.rewind(b.chunks)
}

// sort orders the chunk's index by the keys it holds.
func (c *dataChunk) sort() {
	ents := c.entries()
	if len(ents) < 2 {
		return
	}
	buf := c.buf
	cmp := func(x, y entryLoc) int {
		if r := bytes.Compare(keyOf(buf, x), keyOf(buf, y)); r != 0 {
			return r
		}
		return int(x.offset() - y.offset()) // StableSort: offsets rise with insertion order
	}
	// The index grows downward, so ascending keys arrive reversed. pdqsort
	// spots that too, but only after sampling for a pivot. Equal keys leave
	// the offsets descending, which the byte compare alone already accepts.
	for j := 1; j < len(ents); j++ {
		if bytes.Compare(keyOf(buf, ents[j-1]), keyOf(buf, ents[j])) < 0 {
			slices.SortFunc(ents, cmp)
			return
		}
	}
	slices.Reverse(ents)
}

func (b *sortableBuffer) CheckFlushSize() bool {
	return b.Size() >= b.optimalSize
}

func (b *sortableBuffer) Write(w io.Writer) error {
	b.Sort()
	b.Rewind() // Write drives the read cursor, so it writes the whole buffer
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
	if !b.unsorted {
		return
	}
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

func (b *appendSortableBuffer) Rewind() {
	mustBeSorted(b.unsorted)
	b.at = 0
}

func (b *appendSortableBuffer) Next() ([]byte, []byte, bool) {
	mustBeSorted(b.unsorted)
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
	// The new map holds nothing, so neither does the run flattened out of the
	// old one, and neither does what Size reports.
	b.sortedBuf, b.at, b.unsorted, b.size = b.sortedBuf[:0], 0, false, 0
	if cap(b.sortedBuf) < predictKeysAmount {
		b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount)
	}
	return b
}

func (b *appendSortableBuffer) Write(w io.Writer) error {
	b.Sort()
	b.Rewind() // writeSortedEntries does not use the cursor, but Write resets it
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
	if !b.unsorted {
		return
	}
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

func (b *oldestEntrySortableBuffer) Rewind() {
	mustBeSorted(b.unsorted)
	b.at = 0
}

func (b *oldestEntrySortableBuffer) Next() ([]byte, []byte, bool) {
	mustBeSorted(b.unsorted)
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
	// The new map holds nothing, so neither does the run flattened out of the
	// old one, and neither does what Size reports.
	b.sortedBuf, b.at, b.unsorted, b.size = b.sortedBuf[:0], 0, false, 0
	if cap(b.sortedBuf) < predictKeysAmount {
		b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount)
	}
	return b
}

func (b *oldestEntrySortableBuffer) Write(w io.Writer) error {
	b.Sort()
	b.Rewind() // writeSortedEntries does not use the cursor, but Write resets it
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
