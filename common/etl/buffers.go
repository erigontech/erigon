package etl

import (
	"bytes"
	"github.com/ugorji/go/codec"
	"io"
)


func newSortableBuffer() *sortableBuffer {
	return &sortableBuffer{
		entries:     make([]sortableBufferEntry, 0),
		size:        0,
		OptimalSize: bufferOptimalSize,
		encoder:     codec.NewEncoder(nil, &cbor),
	}
}


type sortableBufferEntry struct {
	key   []byte
	value []byte
}

type sortableBuffer struct {
	entries     []sortableBufferEntry
	size        int
	OptimalSize int
	encoder     *codec.Encoder
}

func (b *sortableBuffer) Put(k, v []byte) {
	b.size += len(k)
	b.size += len(v)
	b.entries = append(b.entries, sortableBufferEntry{k, v})
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

func (b *sortableBuffer) EncoderReset(w io.Writer) {
	b.encoder.Reset(w)
}
func (b *sortableBuffer) Reset() {
	b.entries = b.entries[:0] // keep the capacity
	b.size = 0
}
func (b *sortableBuffer) GetEnt() []sortableBufferEntry {
	return b.entries
}
func (b *sortableBuffer) GetEncoder() Encoder {
	return b.encoder
}
