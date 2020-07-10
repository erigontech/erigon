package etl

import (
	"bytes"
	"sort"
	"strconv"
	"sync"
)

const (
	//SliceBuffer - just simple slice w
	SortableSliceBuffer = iota
	//SortableAppendBuffer - map[k] [v1 v2 v3]
	SortableAppendBuffer
	// SortableOldestAppearedBuffer - buffer that keeps only the oldest entries.
	// if first v1 was added under key K, then v2; only v1 will stay
	SortableOldestAppearedBuffer

	BufferOptimalSize = 256 * 1024 * 1024 /* 256 mb | var because we want to sometimes change it from tests */
	BufIOSize         = 64 * 4096         // 64 pages | default is 1 page | increasing further doesn't show speedup on SSD
)

type Buffer interface {
	Put(k, v []byte)
	Get(i int) sortableBufferEntry
	Len() int
	Reset()
	GetEntries() []sortableBufferEntry
	Sort()
	CheckFlushSize() bool
}

type bytesPoolT struct{ sync.Pool }

func (p *bytesPoolT) Get() []byte  { return p.Pool.Get().([]byte)[:0] }
func (p *bytesPoolT) Put(b []byte) { p.Pool.Put(b) }

var bytesPool = &bytesPoolT{sync.Pool{New: func() interface{} { return make([]byte, 64) }}}

type sortableBufferEntry struct {
	key   []byte
	value []byte
}

var (
	_ Buffer = &sortableBuffer{}
	_ Buffer = &appendSortableBuffer{}
	_ Buffer = &oldestEntrySortableBuffer{}
)

func NewSortableBuffer(bufferOptimalSize int) *sortableBuffer {
	return &sortableBuffer{
		entries:     make([]sortableBufferEntry, 0),
		size:        0,
		optimalSize: bufferOptimalSize,
	}
}

type sortableBuffer struct {
	entries     []sortableBufferEntry
	size        int
	optimalSize int
}

func (b *sortableBuffer) Put(k, v []byte) {
	b.size += len(k)
	b.size += len(v)
	b.entries = append(b.entries, sortableBufferEntry{
		append(bytesPool.Get(), k...),
		append(bytesPool.Get(), v...),
	})
}

func (b *sortableBuffer) Size() int {
	return b.size
}

func (b *sortableBuffer) Len() int {
	return len(b.entries)
}

func (b *sortableBuffer) Less(i, j int) bool {
	return bytes.Compare(b.entries[i].key, b.entries[j].key) < 0
}

func (b *sortableBuffer) Swap(i, j int) {
	b.entries[i], b.entries[j] = b.entries[j], b.entries[i]
}

func (b *sortableBuffer) Get(i int) sortableBufferEntry {
	return b.entries[i]
}

func (b *sortableBuffer) Reset() {
	for _, e := range b.entries {
		bytesPool.Put(e.key)
		bytesPool.Put(e.value)
	}
	b.entries = b.entries[:0] // keep the capacity
	b.size = 0
}
func (b *sortableBuffer) Sort() {
	sort.Stable(b)
}

func (b *sortableBuffer) GetEntries() []sortableBufferEntry {
	return b.entries
}

func (b *sortableBuffer) CheckFlushSize() bool {
	return b.size >= b.optimalSize
}

func NewAppendBuffer(bufferOptimalSize int) *appendSortableBuffer {
	return &appendSortableBuffer{
		entries:     make(map[string][]byte),
		size:        0,
		optimalSize: bufferOptimalSize,
	}
}

type appendSortableBuffer struct {
	entries     map[string][]byte
	size        int
	optimalSize int
	sortedBuf   []sortableBufferEntry
}

func (b *appendSortableBuffer) Put(k, v []byte) {
	ks := string(k)
	stored, ok := b.entries[ks]
	if !ok {
		b.size += len(k)
		k = append(bytesPool.Get(), k...)
		stored = bytesPool.Get()
	}

	b.size += len(v)
	stored = append(stored, v...)
	b.entries[ks] = stored
	bytesPool.Put(k)
}

func (b *appendSortableBuffer) Size() int {
	return b.size
}

func (b *appendSortableBuffer) Len() int {
	return len(b.entries)
}
func (b *appendSortableBuffer) Sort() {
	for i := range b.entries {
		b.sortedBuf = append(b.sortedBuf, sortableBufferEntry{key: []byte(i), value: b.entries[i]})
	}
	sort.Stable(b)
}

func (b *appendSortableBuffer) Less(i, j int) bool {
	return bytes.Compare(b.sortedBuf[i].key, b.sortedBuf[j].key) < 0
}

func (b *appendSortableBuffer) Swap(i, j int) {
	b.sortedBuf[i], b.sortedBuf[j] = b.sortedBuf[j], b.sortedBuf[i]
}

func (b *appendSortableBuffer) Get(i int) sortableBufferEntry {
	return b.sortedBuf[i]
}
func (b *appendSortableBuffer) Reset() {
	b.sortedBuf = b.sortedBuf[:0]
	for _, e := range b.entries {
		bytesPool.Put(e)
	}
	b.entries = make(map[string][]byte)
	b.size = 0
}

func (b *appendSortableBuffer) GetEntries() []sortableBufferEntry {
	return b.sortedBuf
}

func (b *appendSortableBuffer) CheckFlushSize() bool {
	return b.size >= b.optimalSize
}

func NewOldestEntryBuffer(bufferOptimalSize int) *oldestEntrySortableBuffer {
	return &oldestEntrySortableBuffer{
		entries:     make(map[string][]byte),
		size:        0,
		optimalSize: bufferOptimalSize,
	}
}

type oldestEntrySortableBuffer struct {
	entries     map[string][]byte
	size        int
	optimalSize int
	sortedBuf   []sortableBufferEntry
}

func (b *oldestEntrySortableBuffer) Put(k, v []byte) {
	ks := string(k)
	_, ok := b.entries[ks]
	if ok {
		// if we already had this entry, we are going to keep it and ignore new value
		return
	}

	b.size += len(k)
	b.size += len(v)

	k = append(bytesPool.Get(), k...)
	if v != nil {
		v = append(bytesPool.Get(), v...)
	}
	b.entries[ks] = v
	bytesPool.Put(k)
}

func (b *oldestEntrySortableBuffer) Size() int {
	return b.size
}

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

func (b *oldestEntrySortableBuffer) Get(i int) sortableBufferEntry {
	return b.sortedBuf[i]
}
func (b *oldestEntrySortableBuffer) Reset() {
	b.sortedBuf = b.sortedBuf[:0]
	for _, e := range b.sortedBuf {
		bytesPool.Put(e.key)
		bytesPool.Put(e.value)
	}
	b.entries = make(map[string][]byte)
	b.size = 0
}

func (b *oldestEntrySortableBuffer) GetEntries() []sortableBufferEntry {
	return b.sortedBuf
}

func (b *oldestEntrySortableBuffer) CheckFlushSize() bool {
	return b.size >= b.optimalSize
}

func getBufferByType(tp int, size int) Buffer {
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
