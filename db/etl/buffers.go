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
	//SliceBuffer - just simple slice w
	SortableSliceBuffer = iota
	//SortableAppendBuffer - map[k] [v1 v2 v3]
	SortableAppendBuffer
	// SortableOldestAppearedBuffer - buffer that keeps only the oldest entries.
	// if first v1 was added under key K, then v2; only v1 will stay
	SortableOldestAppearedBuffer

	//BufIOSize - 128 pages | default is 1 page | increasing over `64 * 4096` doesn't show speedup on SSD/NVMe, but show speedup in cloud drives
	BufIOSize = 128 * 4096

	entryLocSize = 24 // sizeof(entryLoc): insertionOrder(4) + offset(4) + keyLen(4) + valLen(4) + keyPrefix(8)
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

// etlSmallBufRAM (BufferOptimalSize/8) bounds the flush threshold so a full
// set of domain/history/index flush collectors (~17 per batch writer) stays
// around 512 MB when all run full. Pooled buffers start empty and grow with
// the data they actually see; grown capacity survives reuse (Reset preserves
// cap), so hot collectors amortize growth while never-full ones stay small.
var etlSmallBufRAM = dbg.EnvDataSize("ETL_SMALL", BufferOptimalSize/8)
var SmallSortableBuffers = NewAllocator(&sync.Pool{
	New: func() any {
		return NewSortableBuffer(etlSmallBufRAM)
	},
})
var etlLargeBufRAM = BufferOptimalSize
var LargeSortableBuffers = NewAllocator(&sync.Pool{
	New: func() any {
		return NewSortableBuffer(etlLargeBufRAM)
	},
})

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

var chunkSize = int(dbg.EnvDataSize("ETL_CHUNK", 1*datasize.MB))

var chunkPool = sync.Pool{
	New: func() any {
		c := make([]byte, chunkSize)
		return &c
	},
}

var chunkPoolWarm = dbg.EnvInt("ETL_CHUNK_POOL_WARM", 64)

func init() {
	for range chunkPoolWarm {
		c := make([]byte, chunkSize)
		chunkPool.Put(&c)
	}
}

func getChunk(size int) []byte {
	if size != chunkSize {
		return make([]byte, size)
	}
	return *chunkPool.Get().(*[]byte)
}

func putChunk(c []byte) {
	if len(c) == chunkSize {
		chunkPool.Put(&c)
	}
}

// entryLoc locates a key/value pair. keyLen/valLen of -1 means a nil slice.
type entryLoc struct {
	insertionOrder int32 // enables stable sort via unstable SortFunc
	offset         int32
	keyLen         int32
	valLen         int32
	keyPrefix      uint64 // see keyPrefixOf
}

// keyPrefixOf packs the first 8 key bytes big-endian, zero-padded, so comparing
// prefixes as uint64 orders them the same as comparing the bytes.
func keyPrefixOf(k []byte) uint64 {
	var buf [8]byte
	copy(buf[:], k)
	return binary.BigEndian.Uint64(buf[:])
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
	entries     []entryLoc
	chunks      [][]byte
	next        int // global offset of the next free byte
	dataLen     int
	optimalSize int
}

// reserve returns the offset of n contiguous free bytes. An entry never spans
// chunks, so the tail of a chunk that cannot hold it is skipped.
func (b *sortableBuffer) reserve(n int) int {
	if n > chunkSize {
		// Past every allocated chunk, so offset/chunkSize finds this wide one
		// and not a narrow chunk that cannot hold n.
		span := (n + chunkSize - 1) / chunkSize * chunkSize
		c := getChunk(span)
		off := len(b.chunks) * chunkSize
		for range span / chunkSize {
			b.chunks = append(b.chunks, c)
		}
		b.next = off + span
		return off
	}
	if pos := b.next % chunkSize; pos+n > chunkSize {
		b.next += chunkSize - pos
	}
	for len(b.chunks)*chunkSize < b.next+max(n, 1) {
		b.chunks = append(b.chunks, getChunk(chunkSize))
	}
	off := b.next
	b.next += n
	return off
}

func (b *sortableBuffer) at(offset, length int) []byte {
	c, pos := b.locate(offset)
	return c[pos : pos+length]
}

// A key and its value always share a chunk, so callers locate once and slice both.
func (b *sortableBuffer) locate(offset int) ([]byte, int) {
	return b.chunks[offset/chunkSize], offset % chunkSize
}

// Put adds key and value to the buffer. These slices will not be accessed later,
// so no copying is necessary
func (b *sortableBuffer) Put(k, v []byte) {
	off := b.reserve(len(k) + len(v))
	e := entryLoc{
		offset:         int32(off),            //nolint:gosec
		keyLen:         int32(len(k)),         //nolint:gosec
		valLen:         int32(len(v)),         //nolint:gosec
		insertionOrder: int32(len(b.entries)), //nolint:gosec
		keyPrefix:      keyPrefixOf(k),
	}
	if k == nil {
		e.keyLen = -1
	}
	if v == nil {
		e.valLen = -1
	}
	b.entries = append(b.entries, e)
	c, pos := b.locate(off)
	copy(c[pos:], k)
	copy(c[pos+len(k):], v)
	b.dataLen += len(k) + len(v)
}

func (b *sortableBuffer) Size() int { return b.dataLen + len(b.entries)*entryLocSize }

func (b *sortableBuffer) Len() int {
	return len(b.entries)
}

func (b *sortableBuffer) Get(i int) ([]byte, []byte) {
	e := &b.entries[i]
	kLen, vLen := int(e.keyLen), int(e.valLen)
	c, pos := b.locate(int(e.offset))
	valPos := pos
	if kLen > 0 {
		valPos += kLen
	}
	var key, val []byte
	if kLen > 0 {
		key = c[pos : pos+kLen]
	} else if kLen == 0 {
		key = []byte{}
	}
	if vLen > 0 {
		val = c[valPos : valPos+vLen]
	} else if vLen == 0 {
		val = []byte{}
	}
	return key, val
}

func (b *sortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) Buffer {
	if cap(b.entries) < predictKeysAmount {
		b.entries = make([]entryLoc, 0, predictKeysAmount)
	}
	for len(b.chunks)*chunkSize < predictDataSize {
		b.chunks = append(b.chunks, getChunk(chunkSize))
	}
	return b
}

func (b *sortableBuffer) Reset() {
	b.entries = b.entries[:0]
	for i, c := range b.chunks {
		putChunk(c)
		b.chunks[i] = nil
	}
	b.chunks = b.chunks[:0]
	b.next = 0
	b.dataLen = 0
}
func (b *sortableBuffer) SizeLimit() int { return b.optimalSize }
func (b *sortableBuffer) Sort() {
	cmp := func(x, y entryLoc) int {
		if x.keyPrefix != y.keyPrefix {
			if x.keyPrefix < y.keyPrefix {
				return -1
			}
			return 1
		}
		xc, xp := b.locate(int(x.offset))
		yc, yp := b.locate(int(y.offset))
		xKey := xc[xp : xp+int(max(x.keyLen, 0))]
		yKey := yc[yp : yp+int(max(y.keyLen, 0))]
		if c := bytes.Compare(xKey, yKey); c != 0 {
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
		keyOffset := int(e.offset)
		valOffset := keyOffset
		if kLen > 0 {
			valOffset += kLen
		}
		// write key
		n := binary.PutVarint(numBuf[:], int64(e.keyLen))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if kLen > 0 {
			if _, err := w.Write(b.at(keyOffset, kLen)); err != nil {
				return err
			}
		}
		// write value
		n = binary.PutVarint(numBuf[:], int64(e.valLen))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if vLen > 0 {
			if _, err := w.Write(b.at(valOffset, vLen)); err != nil {
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
