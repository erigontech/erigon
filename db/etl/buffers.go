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
	"sort"
	"strconv"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/common/dbg"

	"github.com/erigontech/erigon-lib/common"
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
)

var BufferOptimalSize = dbg.EnvDataSize("ETL_OPTIMAL", 256*datasize.MB) /*  var because we want to sometimes change it from tests or command-line flags */

// 3_domains * 2 + 3_history * 1 + 4_indices * 2 = 17 etl collectors, 17*(256Mb/8) = 512Mb - for all collectros
var etlSmallBufRAM = dbg.EnvDataSize("ETL_SMALL", BufferOptimalSize/8)
var SmallSortableBuffers = NewAllocator(&sync.Pool{
	New: func() interface{} {
		return NewSortableBuffer(etlSmallBufRAM).Prealloc(1_024, int(etlSmallBufRAM/32))
	},
})
var etlLargeBufRAM = BufferOptimalSize
var LargeSortableBuffers = NewAllocator(&sync.Pool{
	New: func() interface{} {
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

func NewSortableBuffer(bufferOptimalSize datasize.ByteSize) *sortableBuffer {
	return &sortableBuffer{
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type sortableBuffer struct {
	offsets     []int
	lens        []int
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
	b.lens = append(b.lens, lk, lv)

	b.offsets = append(b.offsets, len(b.data))
	b.data = append(b.data, k...)
	b.offsets = append(b.offsets, len(b.data))
	b.data = append(b.data, v...)
	b.size += lk + lv + 32 // size = len(b.data) + 8*len(b.offsets) + 8*len(b.lens)
}

func (b *sortableBuffer) Size() int { return b.size }

func (b *sortableBuffer) Len() int {
	return len(b.offsets) / 2
}

func (b *sortableBuffer) Less(i, j int) bool {
	i2, j2 := i*2, j*2
	ki := b.data[b.offsets[i2] : b.offsets[i2]+b.lens[i2]]
	kj := b.data[b.offsets[j2] : b.offsets[j2]+b.lens[j2]]
	return bytes.Compare(ki, kj) < 0
}

func (b *sortableBuffer) Swap(i, j int) {
	i2, j2 := i*2, j*2
	b.offsets[i2], b.offsets[j2] = b.offsets[j2], b.offsets[i2]
	b.offsets[i2+1], b.offsets[j2+1] = b.offsets[j2+1], b.offsets[i2+1]
	b.lens[i2], b.lens[j2] = b.lens[j2], b.lens[i2]
	b.lens[i2+1], b.lens[j2+1] = b.lens[j2+1], b.lens[i2+1]
}

func (b *sortableBuffer) Get(i int, keyBuf, valBuf []byte) ([]byte, []byte) {
	i2 := i * 2
	keyOffset, valOffset := b.offsets[i2], b.offsets[i2+1]
	keyLen, valLen := b.lens[i2], b.lens[i2+1]
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
	b.lens = make([]int, 0, predictKeysAmount)
	b.offsets = make([]int, 0, predictKeysAmount)
	b.data = make([]byte, 0, predictDataSize)
	b.size = 0
	return b
}

func (b *sortableBuffer) Reset() {
	b.offsets = b.offsets[:0]
	b.lens = b.lens[:0]
	b.data = b.data[:0]
	b.size = 0
}
func (b *sortableBuffer) SizeLimit() int { return b.optimalSize }
func (b *sortableBuffer) Sort() {
	if sort.IsSorted(b) {
		return
	}
	sort.Stable(b)
}

func (b *sortableBuffer) CheckFlushSize() bool {
	return b.Size() >= b.optimalSize
}

func (b *sortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	for i, offset := range b.offsets {
		l := b.lens[i]
		n := binary.PutVarint(numBuf[:], int64(l))
		if _, err := w.Write(numBuf[:n]); err != nil {
			return err
		}
		if l <= 0 {
			continue
		}
		if _, err := w.Write(b.data[offset : offset+l]); err != nil {
			return err
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
	for key, val := range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(key), value: val})
	}
	sort.Stable(b)
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
	b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount*2)
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
	for k, v := range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(k), value: v})
	}
	sort.Stable(b)
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
	b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount*2)
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
