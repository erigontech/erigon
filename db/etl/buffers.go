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

// A spill file prefixes each field with its length. Fixed width rather than a
// varint: the file never leaves the machine that wrote it. A key is capped at
// MaxKeyLen so two bytes hold it, a value at MaxValLen so four do.
const (
	keyLenSize = 2
	valLenSize = 4
	nilKeyLen  = math.MaxUint16 // no key reaches this, so it can mean nil

	// MaxValLen: a longer value writes a length that reads back negative,
	// which the reader takes for nil and desyncs the rest of the file.
	// appendSortableBuffer grows a value across Puts, so the writers check it.
	MaxValLen = math.MaxInt32
)

func checkValLen(v []byte) error {
	if len(v) > MaxValLen {
		return fmt.Errorf("etl: value of %d bytes exceeds %d", len(v), MaxValLen)
	}
	return nil
}

func putKeyLen(dst []byte, keyLen int32) {
	n := uint16(keyLen) //nolint:gosec
	if keyLen < 0 {
		n = nilKeyLen
	}
	binary.NativeEndian.PutUint16(dst, n)
}

func putValLen(dst []byte, valLen int32) {
	binary.NativeEndian.PutUint32(dst, uint32(valLen)) //nolint:gosec
}

// writeSortedEntries writes the entries to w in the spill format above.
func writeSortedEntries(w io.Writer, entries []sortableBufferEntry) error {
	var numBuf [valLenSize]byte
	for _, entry := range entries {
		if err := checkValLen(entry.value); err != nil {
			return err
		}
		keyLen, valLen := int32(len(entry.key)), int32(len(entry.value)) //nolint:gosec
		if entry.key == nil {
			keyLen = -1
		}
		if entry.value == nil {
			valLen = -1
		}
		putKeyLen(numBuf[:], keyLen)
		if _, err := w.Write(numBuf[:keyLenSize]); err != nil {
			return err
		}
		if _, err := w.Write(entry.key); err != nil {
			return err
		}
		putValLen(numBuf[:], valLen)
		if _, err := w.Write(numBuf[:valLenSize]); err != nil {
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
	dataChunkBits = 20
	dataChunkSize = 1 << dataChunkBits // 1MB

	// MaxKeyLen is what is left of entryLoc once the offset takes
	// dataChunkBits, less one for the bias. Only a value may outgrow a chunk.
	MaxKeyLen = 1<<(32-dataChunkBits) - 2
)

// dataChunks are shared by all sortableBuffer instances: a buffer takes chunks as
// it fills and gives them back on Reset, instead of pinning its peak size forever.
var dataChunks = sync.Pool{New: func() any {
	c := make([]byte, dataChunkSize)
	return &c
}}

func getDataChunk() *[]byte { return dataChunks.Get().(*[]byte) }
func putDataChunk(ref *[]byte) {
	if ref != nil {
		dataChunks.Put(ref)
	}
}

type Buffer interface {
	// Put does copy `k` and `v`
	Put(k, v []byte)
	// Next returns the entries in key order, one goroutine at a time. Sort
	// positions the cursor, so reading before it panics and Sorting again is
	// how a buffer is read twice. The slices point into the buffer's storage.
	Next() (k, v []byte, ok bool)
	Len() int
	Reset()
	SizeLimit() int
	Prealloc(predictKeysAmount, predictDataAmount int) Buffer
	Write(io.Writer) error
	Sort()
	CheckFlushSize() bool
}

func panicIfUnsorted(unsorted bool) {
	if unsorted {
		panic("etl: buffer read before Sort")
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

// entryLoc packs an entry's offset inside its chunk with its key length, -1 for
// a nil key and biased by one so nil lands on an untouched slot's zero. Offsets
// rise with insertion order, which orders duplicate keys.
type entryLoc uint32

func makeEntryLoc(keyLen, offset int32) entryLoc {
	return entryLoc(uint32(keyLen+1))<<dataChunkBits | entryLoc(uint32(offset)) //nolint:gosec
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

// dataChunk holds entry bytes growing up from the front and their index growing
// down from the back, full when the two meet - so the index costs no allocation.
type dataChunk struct {
	buf []byte
	// Keeping the pool's own *[]byte lets Reset give the chunk back without
	// boxing. nil for a private chunk, which must never enter the pool.
	ref    *[]byte
	end    int32 // data grows up to here; only the chunk being filled moves it
	entTop int32 // the index grows down to here
}

func keyOf(buf []byte, e entryLoc) []byte {
	if kLen := e.keyLen(); kLen > 0 {
		off := e.offset() + entryHeaderSize
		return buf[off : off+kLen]
	}
	return nil
}

// entries runs newest-first until Sort reorders it, because it grows downward.
func (c *dataChunk) entries() []entryLoc {
	n := (len(c.buf) - int(c.entTop)) / entryLocSize
	if n == 0 {
		return nil
	}
	// Aligned: chunkSizeFor only returns multiples of entryLocAlign.
	return unsafe.Slice((*entryLoc)(unsafe.Pointer(&c.buf[c.entTop])), n)
}

type sortableBuffer struct {
	// A copy of the last of chunks, so Put reaches its bytes without indexing
	// the slice. completeCurrentChunk must put it back before anyone reads it.
	currentChunk dataChunk
	n            int

	chunks []dataChunk

	mrg     merger
	sortedN int // n as of the last Sort; -1 while unsorted

	chunkBytes  int
	optimalSize int

	// Write's length scratch. w.Write takes an io.Writer, so a local array
	// escapes and costs an allocation on every Write.
	numBuf [valLenSize]byte
}

// chunkSizeFor returns 0 if no chunk can hold n. n is bounded before the
// rounding, which wraps near math.MaxInt into a size that would pass for pooled.
func chunkSizeFor(n int) int {
	if n > math.MaxInt32-entryLocSize-entryLocAlign {
		return 0
	}
	if size := n + entryLocSize; size > dataChunkSize {
		return (size + entryLocAlign - 1) &^ (entryLocAlign - 1)
	}
	return dataChunkSize
}

// An entry never straddles chunks, so Next can hand out direct references.
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
	b.completeCurrentChunk()
	b.currentChunk = dataChunk{buf: buf, ref: ref, entTop: int32(len(buf))} //nolint:gosec
	b.chunks = append(b.chunks, b.currentChunk)
	b.chunkBytes += len(buf)
}

func (b *sortableBuffer) completeCurrentChunk() {
	if len(b.chunks) == 0 {
		return
	}
	b.chunks[len(b.chunks)-1] = b.currentChunk
}

// Put adds key and value to the buffer. These slices will not be accessed later,
// so no copying is necessary
func (b *sortableBuffer) Put(k, v []byte) {
	kLen, vLen := len(k), len(v)
	n := entryHeaderSize + kLen + vLen
	off := int(b.currentChunk.end)
	// One test for all three, so the fast path keeps its args in registers.
	if kLen > MaxKeyLen || off+n+entryLocSize > int(b.currentChunk.entTop) || b.sortedN == b.n {
		b.putSlow(k, v)
		return
	}
	keyLen, valLen := int32(-1), int32(-1)
	if k != nil {
		keyLen = int32(kLen) //nolint:gosec
	}
	if v != nil {
		valLen = int32(vLen) //nolint:gosec
	}
	// Capacity included: it lets the compiler drop each copy's min. Worth 10%.
	data := b.currentChunk.buf[off : off+n : off+n]
	binary.NativeEndian.PutUint32(data, uint32(valLen)) //nolint:gosec
	b.currentChunk.entTop -= entryLocSize
	binary.NativeEndian.PutUint32(b.currentChunk.buf[b.currentChunk.entTop:], uint32(makeEntryLoc(keyLen, int32(off)))) //nolint:gosec
	b.currentChunk.end = int32(off + n)                                                                                 //nolint:gosec
	b.n++
	copy(data[entryHeaderSize:entryHeaderSize+kLen], k)
	copy(data[entryHeaderSize+kLen:], v)
}

// nextChunk always leaves room, so the retry cannot come back here.
//
//go:noinline
func (b *sortableBuffer) putSlow(k, v []byte) {
	if b.sortedN == b.n {
		panic("etl: Put after Sort")
	}
	if len(k) > MaxKeyLen {
		panic(fmt.Sprintf("etl: key of %d bytes exceeds %d", len(k), MaxKeyLen))
	}
	b.nextChunk(entryHeaderSize + len(k) + len(v))
	b.Put(k, v)
}

func (b *sortableBuffer) Size() int {
	return b.chunkBytes - int(b.currentChunk.entTop-b.currentChunk.end)
}

func (b *sortableBuffer) Len() int { return b.n }

func (b *sortableBuffer) Next() ([]byte, []byte, bool) {
	panicIfUnsorted(b.sortedN != b.n)
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

// Prealloc reserves only the chunk headers - chunks come from the pool one at
// a time, so an idle buffer holds nothing.
func (b *sortableBuffer) Prealloc(_, predictDataSize int) Buffer {
	if n := predictDataSize/dataChunkSize + 1; cap(b.chunks) < n {
		b.chunks = slices.Grow(b.chunks, n)
	}
	return b
}

func (b *sortableBuffer) Reset() {
	// The cursors alias the chunks, so drop them before the pool hands those
	// chunks to another buffer.
	b.mrg.release()
	b.currentChunk = dataChunk{}
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
// buffer is; reading back merges the runs.
func (b *sortableBuffer) Sort() {
	if b.sortedN != b.n {
		b.completeCurrentChunk()
		for i := range b.chunks {
			b.chunks[i].sort()
		}
		b.sortedN = b.n
	}
	b.mrg.rewind(b.chunks)
}

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
	// The index grows downward, so ascending keys arrive reversed; equal keys
	// leave the offsets descending, which the byte compare accepts. pdqsort
	// finds the reversed run only after sampling a pivot - dropping this loop
	// costs ~2.4x on sorted input.
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
	panicIfUnsorted(b.sortedN != b.n)
	// Write drives the cursor Next does, where the map-backed buffers leave
	// theirs alone. Rewinding around the drain keeps the three alike.
	b.mrg.rewind(b.chunks)
	defer b.mrg.rewind(b.chunks)
	numBuf := b.numBuf[:]
	for {
		k, v, ok := b.Next()
		if !ok {
			return nil
		}
		keyLen, valLen := int32(len(k)), int32(len(v)) //nolint:gosec
		if k == nil {
			keyLen = -1
		}
		if v == nil {
			valLen = -1
		}
		putKeyLen(numBuf, keyLen)
		if _, err := w.Write(numBuf[:keyLenSize]); err != nil {
			return err
		}
		if len(k) > 0 {
			if _, err := w.Write(k); err != nil {
				return err
			}
		}
		putValLen(numBuf, valLen)
		if _, err := w.Write(numBuf[:valLenSize]); err != nil {
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
	panicIfUnsorted(b.unsorted)
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
	// The new map holds nothing, so neither does the old run, nor Size.
	b.sortedBuf, b.at, b.unsorted, b.size = b.sortedBuf[:0], 0, false, 0
	if cap(b.sortedBuf) < predictKeysAmount {
		b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount)
	}
	return b
}

func (b *appendSortableBuffer) Write(w io.Writer) error {
	panicIfUnsorted(b.unsorted)
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
	panicIfUnsorted(b.unsorted)
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
	b.sortedBuf, b.at, b.unsorted, b.size = b.sortedBuf[:0], 0, false, 0
	if cap(b.sortedBuf) < predictKeysAmount {
		b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount)
	}
	return b
}

func (b *oldestEntrySortableBuffer) Write(w io.Writer) error {
	panicIfUnsorted(b.unsorted)
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
