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

	entryLocSize = 16 // sizeof(entryLoc): insertionOrder(4) + offset(4) + keyLen(4) + valLen(4)
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
	// sortableBuffer stores key/value bytes in chunks of a power-of-two size, so
	// entryLoc.offset can pack the chunk index with the offset inside the chunk
	// and splitting the two is a shift and a mask. 1MB is also the least a
	// collector can hold once it takes a chunk at all.
	dataChunkBits = 20
	dataChunkSize = 1 << dataChunkBits // 1MB

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

func getDataChunk() []byte { return *dataChunks.Get().(*[]byte) }

// isPooledChunk reports whether c came from the pool. An oversized entry gets a
// private chunk instead, and handing that one back would let a later
// getDataChunk give an unrelated buffer a chunk of the wrong size.
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

// entryLoc stores the location of a key/value pair inside sortableBuffer.
// offset packs the chunk index and the offset inside that chunk:
// idx<<dataChunkBits | off. Key occupies chunk[off : off+keyLen], value follows
// right after it. keyLen/valLen of -1 indicates nil.
type entryLoc struct {
	insertionOrder int32 // enables stable sort via unstable SortFunc
	offset         int32
	keyLen         int32
	valLen         int32
}

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
	chunks      [][]byte
	cur         []byte
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
		b.cur = make([]byte, n)
	} else {
		b.cur = getDataChunk()
	}
	b.chunks = append(b.chunks, b.cur)
	b.curBase = int32(len(b.chunks)-1) << dataChunkBits //nolint:gosec
	b.curOff = 0
	b.chunkBytes += len(b.cur)
}

// entryData returns e's bytes: the key, immediately followed by the value.
func (b *sortableBuffer) entryData(e *entryLoc) []byte {
	return b.chunks[e.offset>>dataChunkBits][e.offset&(dataChunkSize-1):]
}

// Put adds key and value to the buffer. These slices will not be accessed later,
// so no copying is necessary
func (b *sortableBuffer) Put(k, v []byte) {
	e := entryLoc{
		keyLen:         int32(len(k)),         //nolint:gosec
		valLen:         int32(len(v)),         //nolint:gosec
		insertionOrder: int32(len(b.entries)), //nolint:gosec
	}
	if k == nil {
		e.keyLen = -1
	}
	if v == nil {
		e.valLen = -1
	}
	if n := len(k) + len(v); n > 0 {
		off := b.curOff
		if int(off)+n > len(b.cur) {
			b.nextChunk(n)
			off = 0
		}
		data := b.cur[off:]
		copy(data, k)
		copy(data[len(k):], v)
		e.offset = b.curBase | off
		b.curOff = off + int32(n) //nolint:gosec
	}
	b.entries = append(b.entries, e)
}

// Size counts the stored bytes, the tails wasted by the chunks already filled,
// and entryLocSize bytes of metadata per entry.
func (b *sortableBuffer) Size() int {
	return b.chunkBytes - (len(b.cur) - int(b.curOff)) + len(b.entries)*entryLocSize
}

func (b *sortableBuffer) Len() int {
	return len(b.entries)
}

func (b *sortableBuffer) Get(i int) ([]byte, []byte) {
	e := &b.entries[i]
	kLen, vLen := int(e.keyLen), int(e.valLen)
	var key, val []byte
	if kLen == 0 {
		key = []byte{}
	}
	if vLen == 0 {
		val = []byte{}
	}
	if kLen <= 0 && vLen <= 0 {
		return key, val
	}
	data := b.entryData(e)
	if kLen > 0 {
		key = data[:kLen:kLen]
		data = data[kLen:]
	}
	if vLen > 0 {
		val = data[:vLen:vLen]
	}
	return key, val
}

// Prealloc sizes the entries slice. predictDataSize only reserves room in the
// chunks slice for the chunk pointers; the chunks themselves are still taken
// one at a time, which is what keeps an idle buffer from holding its peak.
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
	b.cur, b.curBase, b.curOff = nil, 0, 0
	b.chunkBytes = 0
}
func (b *sortableBuffer) SizeLimit() int { return b.optimalSize }
func (b *sortableBuffer) Sort() {
	chunks := b.chunks
	// Key extraction stays inside cmp: pdqsortCmpFunc calls the comparator
	// indirectly, so a separate closure never inlines and costs a call per key.
	cmp := func(x, y entryLoc) int {
		var xk, yk []byte
		if x.keyLen > 0 {
			off := x.offset & (dataChunkSize - 1)
			xk = chunks[x.offset>>dataChunkBits][off : off+x.keyLen]
		}
		if y.keyLen > 0 {
			off := y.offset & (dataChunkSize - 1)
			yk = chunks[y.offset>>dataChunkBits][off : off+y.keyLen]
		}
		if c := bytes.Compare(xk, yk); c != 0 {
			return c
		}
		return int(x.insertionOrder - y.insertionOrder) // StableSort: preserve insertion order for duplicate keys
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
	for i := range b.entries {
		e := &b.entries[i]
		kLen, vLen := int(e.keyLen), int(e.valLen)
		var data []byte
		if kLen > 0 || vLen > 0 {
			data = b.entryData(e)
		}
		// write key
		n := binary.PutVarint(numBuf[:], int64(e.keyLen))
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
		n = binary.PutVarint(numBuf[:], int64(e.valLen))
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
