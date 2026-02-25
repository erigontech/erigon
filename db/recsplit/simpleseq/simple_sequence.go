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
	count   uint64 //u64-typed pre-calculated `len(raw)/4`
}

func NewSimpleSequence(baseNum uint64, count uint64) *SimpleSequence {
	return &SimpleSequence{
		baseNum: baseNum,
		raw:     make([]byte, count*4),
		pos:     0,
		count:   count,
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
	delta := uint64(binary.BigEndian.Uint32(s.raw[i*4:]))
	return s.baseNum + delta
}

func (s *SimpleSequence) Min() uint64 {
	delta := uint64(binary.BigEndian.Uint32(s.raw))
	return s.baseNum + delta
}

func (s *SimpleSequence) Max() uint64 {
	delta := uint64(binary.BigEndian.Uint32(s.raw[len(s.raw)-4:]))
	return s.baseNum + delta
}

func (s *SimpleSequence) Count() uint64 {
	return s.count
}
func (s *SimpleSequence) Empty() bool { return len(s.raw) == 0 }

func (s *SimpleSequence) AddOffset(offset uint64) {
	binary.BigEndian.PutUint32(s.raw[s.pos*4:], uint32(offset-s.baseNum))
	s.pos++
}

func (s *SimpleSequence) Reset(baseNum uint64, raw []byte) { // no `return parameter` to avoid heap-allocation of `s` object
	s.baseNum = baseNum
	s.raw = raw
	s.pos = len(raw) / 4
	s.count = uint64(len(raw) / 4)
}

func (s *SimpleSequence) AppendBytes(buf []byte) []byte {
	return append(buf, s.raw...)
}

func (s *SimpleSequence) search(seek uint64) (idx int, ok bool) {
	// Real data lengths:
	//   - 70% len=1
	//   - 15% len=2
	//   - ...
	//
	// Real data return `idx`:
	//   - 85% return idx=0 (first element)
	//   - 10% return "not found"
	//   - 5% other lengths
	if seek <= s.Min() { // fast-path for 1-st element hit
		return 0, true
	}
	if s.count == 1 { // if len=1 then nothing left to search
		return 0, false
	}
	idx = sort.Search(int(s.count), func(i int) bool {
		return s.Get(uint64(i)) >= seek
	})
	return idx, idx < int(s.count)
}

func (s *SimpleSequence) reverseSearch(seek uint64) (idx int, ok bool) {
	if seek >= s.Max() { // fast-path for last element hit
		return int(s.count) - 1, true
	}
	if s.count == 1 { // if len=1 then nothing left to search
		return 0, false
	}
	idx = sort.Search(int(s.count), func(i int) bool {
		return s.Get(uint64(i)) > seek
	}) - 1
	return idx, idx >= 0
}

func (s *SimpleSequence) Seek(v uint64) (uint64, bool) {
	idx, found := s.search(v)
	if !found {
		return 0, false
	}
	return s.Get(uint64(idx)), true
}

func (s *SimpleSequence) Has(v uint64) bool {
	n, ok := s.Seek(v)
	return ok && n == v
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

func (it *SimpleSequenceIterator) Reset(seq *SimpleSequence) {
	it.seq = seq
	it.pos = 0
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

func (it *ReverseSimpleSequenceIterator) Reset(seq *SimpleSequence) {
	it.seq = seq
	it.pos = int(seq.Count()) - 1
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
