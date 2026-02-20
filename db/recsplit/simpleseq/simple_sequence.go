package simpleseq

import (
	"encoding/binary"

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
	return s.Get(0)
}

func (s *SimpleSequence) Max() uint64 {
	delta := binary.BigEndian.Uint32(s.raw[len(s.raw)-4:])
	return s.baseNum + uint64(delta)
}

func (s *SimpleSequence) Count() uint64 {
	return uint64(len(s.raw) / 4)
}

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

func (s *SimpleSequence) search(seek uint64) (idx int, v uint64, ok bool) {
	// Real data lengths:
	//   - 70% len=1
	//   - 15% len=2
	//   - ...
	//
	// Real data return `idx`:
	//   - 85% return idx=0 (first element)
	//   - 10% return "not found"
	//   - 5% other lengths
	//
	// As a result: check first element before max, reuse max value in scan.

	if len(s.raw) == 0 {
		return 0, 0, false
	}
	// Fast path: 85% of real data returns idx=0. Check first element before
	// reading max, saving one memory read on the dominant path.
	v = s.baseNum + uint64(binary.BigEndian.Uint32(s.raw))
	if v >= seek {
		return 0, v, true
	}
	// Single-element sequence: first element didn't match, nothing else to check.
	if len(s.raw) == 4 {
		return 0, 0, false
	}
	// Read max once; reuse it to avoid re-reading the last element in the loop.
	maxV := s.Max()
	if seek > maxV {
		return 0, 0, false
	}
	for i := 4; i < len(s.raw)-4; i += 4 {
		v = s.baseNum + uint64(binary.BigEndian.Uint32(s.raw[i:]))
		if v >= seek {
			return i / 4, v, true
		}
	}
	// Last element is guaranteed to satisfy v >= seek (seek <= maxV).
	return len(s.raw)/4 - 1, maxV, true
}

func (s *SimpleSequence) reverseSearch(seek uint64) (idx int, v uint64, ok bool) {
	if len(s.raw) == 0 || seek < s.Min() {
		return 0, 0, false
	}
	for i := len(s.raw) - 4; i >= 0; i -= 4 {
		v = s.baseNum + uint64(binary.BigEndian.Uint32(s.raw[i:]))
		if v <= seek {
			return i / 4, v, true
		}
	}
	return 0, 0, false
}

func (s *SimpleSequence) Seek(v uint64) (uint64, bool) {
	_, val, found := s.search(v)
	return val, found
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
	idx, _, found := it.seq.search(v)
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
	idx, _, found := it.seq.reverseSearch(v)
	if !found {
		it.pos = -1
		return
	}

	it.pos = idx
}
