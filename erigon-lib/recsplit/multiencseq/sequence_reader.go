package multiencseq

import (
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/recsplit/simpleseq"
)

type NumberSequence interface {
	Get(i uint64) uint64
	Min() uint64
	Max() uint64
	Count() uint64

	Reset(r []byte)

	// Search returns the value in the sequence, equal or greater than given value
	Search(v uint64) (uint64, bool)

	Iterator() stream.Uno[uint64]
	ReverseIterator() stream.Uno[uint64]
}

type EncodingType uint8

const (
	PlainEliasFano   EncodingType = 0b0
	SimpleEncoding   EncodingType = 0b10000000
	RebasedEliasFano EncodingType = 0b10010000
)

type MultiEncodingSequence struct {
	ef   eliasfano32.EliasFano
	sseq simpleseq.SimpleSequence

	currentEnc EncodingType
}

func ReadMultiEncSeq(baseNum uint64, b []byte) *MultiEncodingSequence {
	var s MultiEncodingSequence
	s.Reset(baseNum, b)
	return &s
}

func Count(baseNum uint64, data []byte) uint64 {
	// TODO: implement multiseqenc support
	return eliasfano32.Count(data)
}

// TODO: optimize me - to avoid object allocation (this TODO was inherited from elias_fano.go)
func Seek(baseNum uint64, data []byte, n uint64) (uint64, bool) {
	seq := ReadMultiEncSeq(baseNum, data)
	return seq.Search(n)
}

func (s *MultiEncodingSequence) Get(i uint64) uint64 {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Get(i)
	} else if s.currentEnc == PlainEliasFano {
		return s.ef.Get(i)
	}
	panic("unknown encoding")
}

func (s *MultiEncodingSequence) Min() uint64 {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Min()
	} else if s.currentEnc == PlainEliasFano {
		return s.ef.Min()
	}
	panic("unknown encoding")
}

func (s *MultiEncodingSequence) Max() uint64 {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Max()
	} else if s.currentEnc == PlainEliasFano {
		return s.ef.Max()
	}
	panic("unknown encoding")
}

func (s *MultiEncodingSequence) Count() uint64 {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Count()
	} else if s.currentEnc == PlainEliasFano {
		return s.ef.Count()
	}
	panic("unknown encoding")
}

func (s *MultiEncodingSequence) Reset(baseNum uint64, r []byte) {
	if r[0]&0b10000000 == 0 {
		s.currentEnc = PlainEliasFano
		s.ef.Reset(r)
		return
	}

	s.currentEnc = SimpleEncoding
	s.sseq.Reset(baseNum, r[1:])
}

func (s *MultiEncodingSequence) Search(v uint64) (uint64, bool) {
	if s.currentEnc == SimpleEncoding {
		return s.sseq.Search(v)
	} else if s.currentEnc == PlainEliasFano {
		return s.ef.Search(v)
	}
	panic("unknown encoding")
}

func (s *MultiEncodingSequence) Iterator(v uint64) stream.U64 {
	if s.currentEnc == SimpleEncoding {
		it := s.sseq.Iterator()
		if v > 0 {
			it.Seek(v)
		}
		return it
	} else if s.currentEnc == PlainEliasFano {
		it := s.ef.Iterator()
		if v > 0 {
			it.Seek(v)
		}
		return it
	}
	panic("unknown encoding")
}

func (s *MultiEncodingSequence) ReverseIterator(v uint64) stream.U64 {
	if s.currentEnc == SimpleEncoding {
		it := s.sseq.ReverseIterator()
		it.Seek(v)
		return it
	} else if s.currentEnc == PlainEliasFano {
		it := s.ef.ReverseIterator()
		it.Seek(v)
		return it
	}
	panic("unknown encoding")
}
