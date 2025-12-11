package multiencseq

import (
	"fmt"

	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/recsplit/simpleseq"
)

type EncodingType uint8

const (
	// TODO: remove PlainEliasFano reader support once all snapshots are migrated to
	// optimized .ef and E3 alpha/beta version which writes ONLY new format is published.
	// until then keep this constant which will be used ONLY for reading legacy .ef support.
	PlainEliasFano   EncodingType = 0b0
	SimpleEncoding   EncodingType = 0b10000000
	RebasedEliasFano EncodingType = 0b10010000

	PlainEliasFanoMask     byte = 0b10000000
	SimpleEncodingMask     byte = 0b11110000
	SimpleEncodingSizeMask byte = ^SimpleEncodingMask
)

// SequenceReader is used to read serialized number sequences.
//
// It is aware of the different encoding types and can read them transparently.
//
// This is the "reader" counterpart of SequenceBuilder.
type SequenceReader struct {
	currentEnc EncodingType
	ref        eliasfano32.RebasedEliasFano
	sseq       simpleseq.SimpleSequence
}

func ReadMultiEncSeq(baseNum uint64, raw []byte) *SequenceReader {
	var s SequenceReader
	s.Reset(baseNum, raw)
	return &s
}

// This is a specialized "fast" Count method that shouldn't allocate new objects, but read the count directly
// from raw data
func Count(baseNum uint64, data []byte) uint64 {
	// plain elias fano (legacy)
	if data[0]&PlainEliasFanoMask == 0 {
		return eliasfano32.Count(data)
	}

	// rebased elias fano
	if EncodingType(data[0]) == RebasedEliasFano {
		return eliasfano32.Count(data[1:])
	}

	// simple encoding
	if EncodingType(data[0]&SimpleEncodingMask) == SimpleEncoding {
		return uint64(data[0]&SimpleEncodingSizeMask) + 1
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", data[0]))
}

// TODO: optimize me - to avoid object allocation (this TODO was inherited from elias_fano.go)
func Seek(baseNum uint64, data []byte, n uint64) (uint64, bool) {
	seq := ReadMultiEncSeq(baseNum, data)
	return seq.seek(n)
}

func (s *SequenceReader) EncodingType() EncodingType {
	return s.currentEnc
}

func (s *SequenceReader) Get(i uint64) uint64 {
	switch s.currentEnc {
	case SimpleEncoding:
		return s.sseq.Get(i)
	case PlainEliasFano, RebasedEliasFano:
		return s.ref.Get(i)
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", s.currentEnc))
}

func (s *SequenceReader) Min() uint64 {
	switch s.currentEnc {
	case SimpleEncoding:
		return s.sseq.Min()
	case PlainEliasFano, RebasedEliasFano:
		return s.ref.Min()
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", s.currentEnc))
}

func (s *SequenceReader) Max() uint64 {
	switch s.currentEnc {
	case SimpleEncoding:
		return s.sseq.Max()
	case PlainEliasFano, RebasedEliasFano:
		return s.ref.Max()
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", s.currentEnc))
}

func (s *SequenceReader) Count() uint64 {
	switch s.currentEnc {
	case SimpleEncoding:
		return s.sseq.Count()
	case PlainEliasFano, RebasedEliasFano:
		return s.ref.Count()
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", s.currentEnc))
}

func (s *SequenceReader) Reset(baseNum uint64, raw []byte) {
	// plain elias fano (legacy)
	if raw[0]&PlainEliasFanoMask == 0 {
		s.currentEnc = PlainEliasFano
		s.ref.Reset(0, raw)
		return
	}

	// rebased elias fano
	if EncodingType(raw[0]) == RebasedEliasFano {
		s.currentEnc = RebasedEliasFano
		s.ref.Reset(baseNum, raw[1:])
		return
	}

	// simple encoding
	if EncodingType(raw[0]&SimpleEncodingMask) == SimpleEncoding {
		s.currentEnc = SimpleEncoding
		s.sseq.Reset(baseNum, raw[1:])
		return
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", raw[0]))
}

func (s *SequenceReader) seek(v uint64) (uint64, bool) {
	switch s.currentEnc {
	case SimpleEncoding:
		return s.sseq.Seek(v)
	case PlainEliasFano, RebasedEliasFano:
		return s.ref.Seek(v)
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", s.currentEnc))
}

func (s *SequenceReader) Iterator(from int) stream.U64 {
	switch s.currentEnc {
	case SimpleEncoding:
		it := s.sseq.Iterator()
		if from > 0 {
			it.Seek(uint64(from))
		}
		return it
	case PlainEliasFano, RebasedEliasFano:
		it := s.ref.Iterator()
		if from > 0 {
			it.Seek(uint64(from))
		}
		return it
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", s.currentEnc))
}

func (s *SequenceReader) ReverseIterator(v int) stream.U64 {
	switch s.currentEnc {
	case SimpleEncoding:
		it := s.sseq.ReverseIterator()
		if v > 0 {
			it.Seek(uint64(v))
		}
		return it
	case PlainEliasFano, RebasedEliasFano:
		it := s.ref.ReverseIterator()
		if v > 0 {
			it.Seek(uint64(v))
		}
		return it
	}

	panic(fmt.Sprintf("unknown sequence encoding: %d", s.currentEnc))
}
