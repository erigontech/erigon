package multiencseq

import (
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/recsplit/simpleseq"
)

type EncodingType uint8

const (
	PlainEliasFano   EncodingType = 0b0
	SimpleEncoding   EncodingType = 0b10000000
	RebasedEliasFano EncodingType = 0b10010000
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

func ReadMultiEncSeq(baseNum uint64, b []byte) *SequenceReader {
	var s SequenceReader
	s.Reset(baseNum, b)
	return &s
}

// This is a specialized "fast" Count method that shouldn't allocate new objects, but read the count directly
// from raw data
func Count(baseNum uint64, data []byte) uint64 {
	// plain elias fano (legacy)
	if data[0]&0b10000000 == 0 {
		return eliasfano32.Count(data)
	}

	// rebased elias fano
	if data[0] == 0x90 {
		return eliasfano32.Count(data[1:])
	}

	// simple encoding
	if data[0]&0b11110000 == 0b10000000 {
		return uint64(data[0]&0b00001111) + 1
	}

	panic("unknown encoding")
}

// TODO: optimize me - to avoid object allocation (this TODO was inherited from elias_fano.go)
func Seek(baseNum uint64, data []byte, n uint64) (uint64, bool) {
	seq := ReadMultiEncSeq(baseNum, data)
	return seq.search(n)
}

func (s *SequenceReader) Get(i uint64) uint64 {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Get(i)
	} else if s.currentEnc == PlainEliasFano || s.currentEnc == RebasedEliasFano {
		return s.ref.Get(i)
	}

	panic("unknown encoding")
}

func (s *SequenceReader) Min() uint64 {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Min()
	} else if s.currentEnc == PlainEliasFano || s.currentEnc == RebasedEliasFano {
		return s.ref.Min()
	}

	panic("unknown encoding")
}

func (s *SequenceReader) Max() uint64 {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Max()
	} else if s.currentEnc == PlainEliasFano || s.currentEnc == RebasedEliasFano {
		return s.ref.Max()
	}

	panic("unknown encoding")
}

func (s *SequenceReader) Count() uint64 {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Count()
	} else if s.currentEnc == PlainEliasFano || s.currentEnc == RebasedEliasFano {
		return s.ref.Count()
	}

	panic("unknown encoding")
}

func (s *SequenceReader) Reset(baseNum uint64, data []byte) {
	// plain elias fano (legacy)
	if data[0]&0b10000000 == 0 {
		s.currentEnc = PlainEliasFano
		s.ref.Reset(0, data)
		return
	}

	// rebased elias fano
	if data[0] == 0x90 {
		s.currentEnc = RebasedEliasFano
		s.ref.Reset(baseNum, data[1:])
		return
	}

	// simple encoding
	if data[0]&0b11110000 == 0b10000000 {
		s.currentEnc = SimpleEncoding
		s.sseq.Reset(baseNum, data[1:])
		return
	}

	panic("unknown encoding")
}

func (s *SequenceReader) search(v uint64) (uint64, bool) {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Search(v)
	} else if s.currentEnc == PlainEliasFano || s.currentEnc == RebasedEliasFano {
		return s.ref.Search(v)
	}

	panic("unknown encoding")
}

func (s *SequenceReader) Iterator(v int) stream.U64 {
	if s.currentEnc == SimpleEncoding {
		it := s.sseq.Iterator()
		if v > 0 {
			it.Seek(uint64(v))
		}
		return it
	} else if s.currentEnc == PlainEliasFano || s.currentEnc == RebasedEliasFano {
		it := s.ref.Iterator()
		if v > 0 {
			it.Seek(uint64(v))
		}
		return it
	}

	panic("unknown encoding")
}

func (s *SequenceReader) ReverseIterator(v int) stream.U64 {
	if s.currentEnc == SimpleEncoding {
		it := s.sseq.ReverseIterator()
		if v > 0 {
			it.Seek(uint64(v))
		}
		return it
	} else if s.currentEnc == PlainEliasFano || s.currentEnc == RebasedEliasFano {
		it := s.ref.ReverseIterator()
		if v > 0 {
			it.Seek(uint64(v))
		}
		return it
	}

	panic("unknown encoding")
}
