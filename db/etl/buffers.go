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

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
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

	entryLocSize    = 8 // sizeof(sortableBuffer.entries element): offset(4) + keyLen(2) + 2 spare
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

func entriesIn(bufBytes datasize.ByteSize) int {
	const etlAvgEntryBytes = 20
	return int(bufBytes) / (etlAvgEntryBytes + entryLocSize)
}

var (
	// etlSmallBufRAM (BufferOptimalSize/8) bounds the flush threshold:
	// 3_domains * 2 + 3_history * 1 + 4_indices * 2 = 17 etl collectors,
	// 17*(256Mb/8) = 512Mb for all collectors combined. Buffers pool their
	// chunks — see dataChunks below.
	etlSmallBufRAM       = dbg.EnvDataSize("ETL_SMALL", BufferOptimalSize/8)
	SmallSortableBuffers = NewAllocator(&sync.Pool{
		New: func() any {
			mxBufNew.Inc()
			log.Warn("[dbg] NewSortableBuffer")
			// Sortable Buffer now pre-allocs only metadata arrays not internal buffers for data-holding (they are-preallocated and have own sync.Pool)
			return NewSortableBuffer(etlSmallBufRAM) //.Prealloc(512, int(etlSmallBufRAM)/8)
		},
	})
)

var (
	etlLargeBufRAM       = BufferOptimalSize
	LargeSortableBuffers = NewAllocator(&sync.Pool{
		New: func() any {
			mxBufNew.Inc()
			return NewSortableBuffer(etlLargeBufRAM) //.Prealloc(entriesIn(etlLargeBufRAM), int(etlLargeBufRAM))
		},
	})
)

const (
	// sortableBuffer stores key/value bytes in chunks of a power-of-two size, so
	// entryLoc.offset can pack the chunk index with the offset inside the chunk
	// and splitting the two is a shift and a mask. 1MB is also the least a
	// collector can hold once it takes a chunk at all.
	dataChunkBits = 20
	dataChunkSize = 1 << dataChunkBits // 1MB

	// Sort slices a key straight out of its chunk, so only a value may outgrow one.
	maxKeyLen = 4096

	// The chunk index takes what is left of a positive int32, so one buffer
	// addresses at most maxDataChunks*dataChunkSize bytes (~2GB); nextChunk
	// panics past that. NewSortableBuffer's MaxInt32 bound on optimalSize
	// does not fully rule this out, since Put can grow the buffer past
	// optimalSize before CheckFlushSize is checked.
	maxDataChunks = math.MaxInt32>>dataChunkBits + 1
)

// dataChunks are shared by all sortableBuffer instances: a buffer takes chunks as
// it fills and gives them back on Reset, instead of pinning its peak size forever.
var dataChunks = sync.Pool{New: func() any {
	c := make([]byte, dataChunkSize)
	return &c
}}

func getDataChunk() *[]byte { return dataChunks.Get().(*[]byte) }

func putDataChunk(c *[]byte) {
	if len(*c) != dataChunkSize { // private chunk of an oversized entry
		log.Warn("[etl] dropping oversized buffer chunk", "size", len(*c), "chunkSize", dataChunkSize)
		return
	}
	dataChunks.Put(c)
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

// entryLoc packs, from the low bit up: the chunk index with the offset inside
// it (idx<<dataChunkBits | off), then keyLen, which maxKeyLen keeps inside 16
// bits, with -1 meaning nil. The top 16 bits are spare. Offsets rise with
// insertion order, which is what lets Sort order duplicate keys without a
// stable sort.
type entryLoc uint64

func makeEntryLoc(keyLen, offset int32) entryLoc {
	return entryLoc(uint16(keyLen))<<32 | entryLoc(uint32(offset)) //nolint:gosec
}
func (e entryLoc) keyLen() int32 { return int32(int16(uint16(e >> 32))) } //nolint:gosec
func (e entryLoc) offset() int32 { return int32(uint32(e)) }              //nolint:gosec

func NewSortableBuffer(bufferOptimalSize datasize.ByteSize) *sortableBuffer {
	if bufferOptimalSize.Bytes() > math.MaxInt32 {
		panic(fmt.Sprintf("etl: sortableBuffer size %d exceeds MaxInt32", bufferOptimalSize.Bytes()))
	}
	return &sortableBuffer{
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type sortableBuffer struct {
	entries []entryLoc
	// chunks hold the key/value bytes. Growing by chunk instead of by one big
	// slice keeps Put from re-allocating and copying everything collected so
	// far. All chunks are dataChunkSize, except the private chunk an entry
	// larger than that gets. cur is the chunk being filled.
	chunks []*[]byte
	// cur is the chunk being filled; curChunk is the pool's own pointer to it,
	// handed straight back on Reset so recycling allocates nothing.
	cur         []byte
	curChunk    *[]byte
	curBase     int32 // packed location of cur's first byte: curIdx<<dataChunkBits
	curOff      int32
	chunkBytes  int
	optimalSize int
}

// nextChunk starts a chunk able to hold n bytes. An entry never straddles
// chunks, so Get can hand out direct references.
func (b *sortableBuffer) nextChunk(n int) {
	if len(b.chunks) >= maxDataChunks {
		panic(fmt.Sprintf("etl: sortableBuffer exceeded %d chunks", maxDataChunks))
	}
	if n > dataChunkSize {
		private := make([]byte, n)
		b.curChunk = &private
	} else {
		b.curChunk = getDataChunk()
	}
	b.cur = *b.curChunk
	b.chunks = append(b.chunks, b.curChunk)
	b.curBase = int32(len(b.chunks)-1) << dataChunkBits //nolint:gosec
	b.curOff = 0
	b.chunkBytes += len(b.cur)
}

// entryData returns e's bytes: valLen, then the key, then the value.
func (b *sortableBuffer) entryData(e entryLoc) []byte {
	off := e.offset()
	return (*b.chunks[off>>dataChunkBits])[off&(dataChunkSize-1):]
}

// Put adds key and value to the buffer. These slices will not be accessed later,
// so no copying is necessary
func (b *sortableBuffer) Put(k, v []byte) {
	if len(k) > maxKeyLen {
		panic(fmt.Sprintf("etl: key of %d bytes exceeds %d", len(k), maxKeyLen))
	}
	kLen, vLen := int32(len(k)), int32(len(v)) //nolint:gosec
	if k == nil {
		kLen = -1
	}
	if v == nil {
		vLen = -1
	}
	n := entryHeaderSize + len(k) + len(v)
	off := b.curOff
	if int(off)+n > len(b.cur) {
		b.nextChunk(n)
		off = 0
	}
	data := b.cur[off:]
	binary.NativeEndian.PutUint32(data, uint32(vLen)) //nolint:gosec
	copy(data[entryHeaderSize:], k)
	copy(data[entryHeaderSize+len(k):], v)
	if len(b.entries) == cap(b.entries) {
		log.Warn("[dbg] entries grow", "len(b.entries)", len(b.entries))
		mxEntriesGrow.Inc()
	}
	b.entries = append(b.entries, makeEntryLoc(kLen, b.curBase|off))
	b.curOff = off + int32(n) //nolint:gosec
}

// Size counts the stored bytes, the tail wasted by the chunk still filling, and
// entryLocSize per entry. An entry's entryHeaderSize is already in chunkBytes.
func (b *sortableBuffer) Size() int {
	return b.chunkBytes - (len(b.cur) - int(b.curOff)) + len(b.entries)*entryLocSize
}

func (b *sortableBuffer) Len() int {
	return len(b.entries)
}

func (b *sortableBuffer) Get(i int) ([]byte, []byte) {
	e := b.entries[i]
	data := b.entryData(e)
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

func (b *sortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) Buffer {
	if cap(b.entries) < predictKeysAmount {
		b.entries = make([]entryLoc, 0, predictKeysAmount)
	}
	if n := predictDataSize/dataChunkSize + 1; cap(b.chunks) < n {
		b.chunks = slices.Grow(b.chunks, n)
	}
	return b
}

func (b *sortableBuffer) Reset() {
	b.entries = b.entries[:0]
	for i, c := range b.chunks {
		putDataChunk(c)
		b.chunks[i] = nil
	}
	b.chunks = b.chunks[:0]
	b.cur, b.curChunk, b.curBase, b.curOff = nil, nil, 0, 0
	b.chunkBytes = 0
}
func (b *sortableBuffer) SizeLimit() int { return b.optimalSize }
func (b *sortableBuffer) Sort() {
	chunks := b.chunks
	key := func(e entryLoc) []byte {
		kLen := e.keyLen()
		if kLen <= 0 {
			return nil
		}
		at := e.offset()
		off := at&(dataChunkSize-1) + entryHeaderSize
		return (*chunks[at>>dataChunkBits])[off : off+kLen]
	}
	cmp := func(x, y entryLoc) int {
		if c := bytes.Compare(key(x), key(y)); c != 0 {
			return c
		}
		return int(x.offset() - y.offset()) // StableSort: offsets rise with insertion order
	}
	if slices.IsSortedFunc(b.entries, cmp) {
		return
	}
	slices.SortFunc(b.entries, cmp)
}

func (b *sortableBuffer) CheckFlushSize() bool {
	return b.Size() >= b.optimalSize
}

func (b *sortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	for _, e := range b.entries {
		data := b.entryData(e)
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
