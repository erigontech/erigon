package simpleseq

import (
	"encoding/binary"
	"sort"

	"github.com/erigontech/erigon/db/kv/stream"
)

// SimpleSequence is a simpler representation of number sequences meant to be a drop-in
// replacement for EliasFano for short sequences.
//
// It stores sequences as deltas of a base number. We assume base number and element values
// as uint64, while the deltas are uint32, reinforcing the assumption that this implementation
// is optimized for short sequences of close values.
type SimpleSequence struct {
	baseNum uint64
	raw     []byte
	pos     int
}

func NewSimpleSequence(baseNum uint64, count uint64) *SimpleSequence {
	return &SimpleSequence{
		baseNum: baseNum,
		raw:     make([]byte, count*4),
		pos:     0,
	}
}

// Construct a SimpleSequence from a serialized representation.
//
// Returned object can be reused by calling Reset() on it.
func ReadSimpleSequence(baseNum uint64, raw []byte) *SimpleSequence {
	seq := SimpleSequence{}
	seq.Reset(baseNum, raw)
	return &seq
}

func (s *SimpleSequence) Get(i uint64) uint64 {
	idx := i * 4
	delta := binary.BigEndian.Uint32(s.raw[idx : idx+4])
	return s.baseNum + uint64(delta)
}

func (s *SimpleSequence) Min() uint64 {
	return s.Get(0)
}

func (s *SimpleSequence) Max() uint64 {
	return s.Get(s.Count() - 1)
}

func (s *SimpleSequence) Count() uint64 {
	return uint64(len(s.raw) / 4)
}

func (s *SimpleSequence) AddOffset(offset uint64) {
	binary.BigEndian.PutUint32(s.raw[s.pos*4:(s.pos+1)*4], uint32(offset-s.baseNum))
	s.pos++
}

func (s *SimpleSequence) Reset(baseNum uint64, raw []byte) {
	s.baseNum = baseNum
	s.raw = raw
	s.pos = len(raw) / 4
}

func (s *SimpleSequence) AppendBytes(buf []byte) []byte {
	return append(buf, s.raw...)
}

func (s *SimpleSequence) search(v uint64) (int, bool) {
	c := s.Count()
	idx := sort.Search(int(c), func(i int) bool {
		return s.Get(uint64(i)) >= v
	})

	if idx >= int(c) {
		return 0, false
	}
	return idx, true
}

func (s *SimpleSequence) reverseSearch(v uint64) (int, bool) {
	c := s.Count()
	idx := sort.Search(int(c), func(i int) bool {
		return s.Get(c-uint64(i)-1) <= v
	})

	if idx >= int(c) {
		return 0, false
	}
	return int(c) - idx - 1, true
}

func (s *SimpleSequence) Seek(v uint64) (uint64, bool) {
	idx, found := s.search(v)
	if !found {
		return 0, false
	}
	return s.Get(uint64(idx)), true
}

func (s *SimpleSequence) Iterator() *SimpleSequenceIterator {
	return &SimpleSequenceIterator{
		seq: s,
		pos: 0,
	}
}

func (s *SimpleSequence) ReverseIterator() *ReverseSimpleSequenceIterator {
	return &ReverseSimpleSequenceIterator{
		seq: s,
		pos: int(s.Count()) - 1,
	}
}

type SimpleSequenceIterator struct {
	seq *SimpleSequence
	pos int
}

func (it *SimpleSequenceIterator) Next() (uint64, error) {
	if !it.HasNext() {
		return 0, stream.ErrIteratorExhausted
	}

	v := it.seq.Get(uint64(it.pos))
	it.pos++
	return v, nil
}

func (it *SimpleSequenceIterator) HasNext() bool {
	return it.pos < int(it.seq.Count())
}

func (it *SimpleSequenceIterator) Close() {
	// noop
}

func (it *SimpleSequenceIterator) Seek(v uint64) {
	idx, found := it.seq.search(v)
	if !found {
		it.pos = int(it.seq.Count())
		return
	}

	it.pos = idx
}

type ReverseSimpleSequenceIterator struct {
	seq *SimpleSequence
	pos int
}

func (it *ReverseSimpleSequenceIterator) Next() (uint64, error) {
	if !it.HasNext() {
		return 0, stream.ErrIteratorExhausted
	}

	v := it.seq.Get(uint64(it.pos))
	it.pos--
	return v, nil
}

func (it *ReverseSimpleSequenceIterator) HasNext() bool {
	return it.pos >= 0
}

func (it *ReverseSimpleSequenceIterator) Close() {
	// noop
}

func (it *ReverseSimpleSequenceIterator) Seek(v uint64) {
	idx, found := it.seq.reverseSearch(v)
	if !found {
		it.pos = -1
		return
	}

	it.pos = idx
}
