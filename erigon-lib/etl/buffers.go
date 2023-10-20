/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package etl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
)

const (
	//SliceBuffer - just simple slice w
	SortableSliceBuffer = iota
	//SortableAppendBuffer - map[k] [v1 v2 v3]
	SortableAppendBuffer
	// SortableOldestAppearedBuffer - buffer that keeps only the oldest entries.
	// if first v1 was added under key K, then v2; only v1 will stay
	SortableOldestAppearedBuffer
	SortableMergeBuffer

	//BufIOSize - 128 pages | default is 1 page | increasing over `64 * 4096` doesn't show speedup on SSD/NVMe, but show speedup in cloud drives
	BufIOSize = 128 * 4096
)

var BufferOptimalSize = 256 * datasize.MB /*  var because we want to sometimes change it from tests or command-line flags */

type Buffer interface {
	Put(k, v []byte)
	Get(i int, keyBuf, valBuf []byte) ([]byte, []byte)
	Len() int
	Reset()
	SizeLimit() int
	Prealloc(predictKeysAmount, predictDataAmount int)
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
}

func (b *sortableBuffer) Size() int {
	return len(b.data) + 8*len(b.offsets) + 8*len(b.lens)
}

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

func (b *sortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) {
	b.lens = make([]int, 0, predictKeysAmount)
	b.offsets = make([]int, 0, predictKeysAmount)
	b.data = make([]byte, 0, predictDataSize)
}

func (b *sortableBuffer) Reset() {
	b.offsets = b.offsets[:0]
	b.lens = b.lens[:0]
	b.data = b.data[:0]
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
	fmt.Printf("put: %d, %x, %x . %x\n", b.size, k, stored, v)
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
func (b *appendSortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) {
	b.entries = make(map[string][]byte, predictKeysAmount)
	b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount*2)
}

func (b *appendSortableBuffer) Write(w io.Writer) error {
	var numBuf [binary.MaxVarintLen64]byte
	entries := b.sortedBuf
	for _, entry := range entries {
		fmt.Printf("write: %x, %x\n", entry.key, entry.value)
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
func (b *oldestEntrySortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) {
	b.entries = make(map[string][]byte, predictKeysAmount)
	b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount*2)
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

func getBufferByType(tp int, size datasize.ByteSize, prevBuf Buffer) Buffer {
	switch tp {
	case SortableSliceBuffer:
		return NewSortableBuffer(size)
	case SortableAppendBuffer:
		return NewAppendBuffer(size)
	case SortableOldestAppearedBuffer:
		return NewOldestEntryBuffer(size)
	case SortableMergeBuffer:
		return NewLatestMergedEntryMergedBuffer(size, prevBuf.(*oldestMergedEntrySortableBuffer).merge)
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
	case *oldestMergedEntrySortableBuffer:
		return SortableMergeBuffer
	default:
		panic(fmt.Sprintf("unknown buffer type: %T ", b))
	}
}

func NewLatestMergedEntryMergedBuffer(bufferOptimalSize datasize.ByteSize, merger func([]byte, []byte) []byte) *oldestMergedEntrySortableBuffer {
	if merger == nil {
		panic("nil merge func")
	}
	return &oldestMergedEntrySortableBuffer{
		entries:     make(map[string][]byte),
		size:        0,
		merge:       merger,
		optimalSize: int(bufferOptimalSize.Bytes()),
	}
}

type oldestMergedEntrySortableBuffer struct {
	entries     map[string][]byte
	merge       func([]byte, []byte) []byte
	sortedBuf   []sortableBufferEntry
	size        int
	optimalSize int
}

func (b *oldestMergedEntrySortableBuffer) Put(k, v []byte) {
	prev, ok := b.entries[string(k)]
	if ok {
		b.size -= len(v)
		// if we already had this entry, we are going to keep it and ignore new value
		v = b.merge(prev, v)
		b.size += len(v)
	} else {
		b.size += len(k) + len(v)
	}
	b.entries[string(k)] = common.Copy(v)
}

func (b *oldestMergedEntrySortableBuffer) Size() int      { return b.size }
func (b *oldestMergedEntrySortableBuffer) SizeLimit() int { return b.optimalSize }

func (b *oldestMergedEntrySortableBuffer) Len() int {
	return len(b.entries)
}

func (b *oldestMergedEntrySortableBuffer) Sort() {
	for k, v := range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(k), value: v})
	}
	sort.Stable(b)
}

func (b *oldestMergedEntrySortableBuffer) Less(i, j int) bool {
	return bytes.Compare(b.sortedBuf[i].key, b.sortedBuf[j].key) < 0
}

func (b *oldestMergedEntrySortableBuffer) Swap(i, j int) {
	b.sortedBuf[i], b.sortedBuf[j] = b.sortedBuf[j], b.sortedBuf[i]
}

func (b *oldestMergedEntrySortableBuffer) Get(i int, keyBuf, valBuf []byte) ([]byte, []byte) {
	keyBuf = append(keyBuf, b.sortedBuf[i].key...)
	valBuf = append(valBuf, b.sortedBuf[i].value...)
	return keyBuf, valBuf
}
func (b *oldestMergedEntrySortableBuffer) Reset() {
	b.sortedBuf = nil
	b.entries = make(map[string][]byte)
	b.size = 0
}
func (b *oldestMergedEntrySortableBuffer) Prealloc(predictKeysAmount, predictDataSize int) {
	b.entries = make(map[string][]byte, predictKeysAmount)
	b.sortedBuf = make([]sortableBufferEntry, 0, predictKeysAmount*2)
}

func (b *oldestMergedEntrySortableBuffer) Write(w io.Writer) error {
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
func (b *oldestMergedEntrySortableBuffer) CheckFlushSize() bool {
	return b.size >= b.optimalSize
}
