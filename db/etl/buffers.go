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
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
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
)

var BufferOptimalSize = dbg.EnvDataSize("ETL_OPTIMAL", 256*datasize.MB) /*  var because we want to sometimes change it from tests or command-line flags */

// 3_domains * 2 + 3_history * 1 + 4_indices * 2 = 17 etl collectors, 17*(256Mb/8) = 512Mb - for all collectros
var etlSmallBufRAM = dbg.EnvDataSize("ETL_SMALL", BufferOptimalSize/8)
var SmallSortableBuffers = NewAllocator(&sync.Pool{
	New: func() any {
		return NewSortableBuffer(etlSmallBufRAM).Prealloc(1_024, int(etlSmallBufRAM/32))
	},
})
var etlLargeBufRAM = BufferOptimalSize
var LargeSortableBuffers = NewAllocator(&sync.Pool{
	New: func() any {
		return NewSortableBuffer(etlLargeBufRAM).Prealloc(1_024, int(etlLargeBufRAM/32))
	},
})

type Buffer interface {
	// Put does copy `k` and `v`
	Put(k, v []byte)
	Get(i int, keyBuf, valBuf []byte) ([]byte, []byte)
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

// entryLoc stores the location of a key/value pair within sortableBuffer.data.
// Key occupies data[offset : offset+keyLen], value follows at data[offset+max(0,keyLen) : ...+valLen].
// keyLen/valLen of -1 indicates nil.
type entryLoc struct {
	offset int
	keyLen int
	valLen int
}

func NewSortableBuffer(bufferOptimalSize datasize.ByteSize) *sortableBuffer {
	return &sortableBuffer{
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type sortableBuffer struct {
	entries     []entryLoc
	data        []byte
	size        int
	optimalSize int
}

// Put adds key and value to the buffer. These slices will not be accessed later,
// so no copying is necessary
func (b *sortableBuffer) Put(k, v []byte) {
	lk, lv := len(k), len(v)
	if k == nil {
		lk = -1
	}
	if v == nil {
		lv = -1
	}
	b.entries = append(b.entries, entryLoc{
		offset: len(b.data),
		keyLen: lk,
		valLen: lv,
	})
	b.data = append(b.data, k...)
	b.data = append(b.data, v...)
	b.size += len(k) + len(v) + 24 // 24 = sizeof(entryLoc): 3 ints
}

func (b *sortableBuffer) Size() int { return b.size }

func (b *sortableBuffer) Len() int {
	return len(b.entries)
}

func (b *sortableBuffer) Get(i int, keyBuf, valBuf []byte) ([]byte, []byte) {
	e := &b.entries[i]
	keyLen, valLen := e.keyLen, e.valLen
	keyOffset := e.offset
	valOffset := keyOffset
	if keyLen > 0 {
		valOffset += keyLen
	}
	if keyLen > 0 {
		keyBuf = append(keyBuf, b.data[keyOffset:keyOffset+keyLen]...)
	} else if keyLen == 0 {
		if keyBuf != nil {
			keyBuf = keyBuf[:0]
		} else {
			keyBuf = []byte{}
		}
	} else {
		keyBuf = nil
	}
	if valLen > 0 {
		valBuf = append(valBuf, b.data[valOffset:valOffset+valLen]...)
	} else if valLen == 0 {
		if valBuf != nil {
			valBuf = valBuf[:0]
		} else {
			valBuf = []byte{}
		}
	} else {
		valBuf = nil
	}
	return keyBuf, valBuf
}

func (b *sortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) Buffer {
	b.entries = make([]entryLoc, 0, predictKeysAmount)
	b.data = make([]byte, 0, predictDataSize)
	b.size = 0
	return b
}

func (b *sortableBuffer) Reset() {
	b.entries = b.entries[:0]
	b.data = b.data[:0]
	b.size = 0
}
func (b *sortableBuffer) SizeLimit() int { return b.optimalSize }
func (b *sortableBuffer) Sort() {
	data := b.data
	cmp := func(a, b entryLoc) int {
		return bytes.Compare(data[a.offset:a.offset+a.keyLen], data[b.offset:b.offset+b.keyLen])
	}
	if slices.IsSortedFunc(b.entries, cmp) {
		return
	}
	slices.SortStableFunc(b.entries, cmp) // Stable: this buffer type can have duplicate keys and must preserve their insertion order
}

func (b *sortableBuffer) CheckFlushSize() bool {
	return b.Size() >= b.optimalSize
}

func (b *sortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	for i := range b.entries {
		e := &b.entries[i]
		keyOffset := e.offset
		valOffset := keyOffset
		if e.keyLen > 0 {
			valOffset += e.keyLen
		}
		// write key
		n := binary.PutVarint(numBuf[:], int64(e.keyLen))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if e.keyLen > 0 {
			if _, err := w.Write(b.data[keyOffset : keyOffset+e.keyLen]); err != nil {
				return err
			}
		}
		// write value
		n = binary.PutVarint(numBuf[:], int64(e.valLen))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if e.valLen > 0 {
			if _, err := w.Write(b.data[valOffset : valOffset+e.valLen]); err != nil {
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

func (b *appendSortableBuffer) Get(i int, keyBuf, valBuf []byte) ([]byte, []byte) {
	keyBuf = append(keyBuf, b.sortedBuf[i].key...)
	valBuf = append(valBuf, b.sortedBuf[i].value...)
	return keyBuf, valBuf
}
func (b *appendSortableBuffer) Reset() {
	b.sortedBuf = nil
	b.entries = make(map[string][]byte)
	b.size = 0
}
func (b *appendSortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) Buffer {
	b.entries = make(map[string][]byte, predictKeysAmount)
	b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount)
	return b
}

func (b *appendSortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	entries := b.sortedBuf
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
	b.entries[string(k)] = common.Copy(v)
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

func (b *oldestEntrySortableBuffer) Get(i int, keyBuf, valBuf []byte) ([]byte, []byte) {
	keyBuf = append(keyBuf, b.sortedBuf[i].key...)
	valBuf = append(valBuf, b.sortedBuf[i].value...)
	return keyBuf, valBuf
}
func (b *oldestEntrySortableBuffer) Reset() {
	b.sortedBuf = nil
	b.entries = make(map[string][]byte)
	b.size = 0
}
func (b *oldestEntrySortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) Buffer {
	b.entries = make(map[string][]byte, predictKeysAmount)
	b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount)
	return b
}

func (b *oldestEntrySortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	entries := b.sortedBuf
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
