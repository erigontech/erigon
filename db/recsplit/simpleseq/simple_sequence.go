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
	delta := binary.BigEndian.Uint32(s.raw[i*4:])
	return s.baseNum + uint64(delta)
}

func (s *SimpleSequence) Min() uint64 {
	return s.baseNum + uint64(binary.BigEndian.Uint32(s.raw))
}

func (s *SimpleSequence) Max() uint64 {
	return s.baseNum + uint64(binary.BigEndian.Uint32(s.raw[len(s.raw)-4:]))
}

func (s *SimpleSequence) Count() uint64 {
	return uint64(len(s.raw) / 4)
}
func (s *SimpleSequence) Empty() bool { return len(s.raw) == 0 }

// isCount1 - sequence has only 1 element
func (s *SimpleSequence) isCount1() bool { return len(s.raw) == 4 }

func (s *SimpleSequence) AddOffset(offset uint64) {
	binary.BigEndian.PutUint32(s.raw[s.pos*4:], uint32(offset-s.baseNum))
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

func (s *SimpleSequence) search(seek uint64) (int, bool) {
	// Real data lengths:
	//   - 70% len=1
	//   - 15% len=2
	//   - ...
	//
	// Real data return `idx`:
	//   - 85% return idx=0 (first element)
	//   - 10% return "not found"
	//   - 5% other lengths
	c := s.Count()
	if c == 0 {
		return 0, false
	}
	if seek <= s.Min() {
		return 0, true
	}
	if s.isCount1() {
		return 0, false
	}
	// c >= 2, Get(0) < seek; answer is in [1, c-1] if it exists
	i := sort.Search(int(c-1), func(i int) bool {
		return s.Get(uint64(i+1)) >= seek
	})
	if i == int(c-1) {
		return 0, false
	}
	return 1 + i, true
}

func (s *SimpleSequence) reverseSearch(seek uint64) (int, bool) {
	// Find the rightmost index where Get(i) <= seek.
	c := s.Count()
	if c == 0 {
		return 0, false
	}

	if seek < s.Min() {
		return 0, false
	}
	if seek >= s.Max() {
		return int(c) - 1, true
	}

	// c >= 2, Get(0) <= seek < Get(c-1); answer is in [0, c-2]
	idx := sort.Search(int(c-1), func(i int) bool {
		return s.Get(c-uint64(i)-2) <= seek
	})
	return int(c) - idx - 2, true
}

func (s *SimpleSequence) Seek(seek uint64) (uint64, bool) {
	idx, found := s.search(seek)
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
