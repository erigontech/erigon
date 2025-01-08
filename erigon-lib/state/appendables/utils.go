package appendables

import (
	"io"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv/stream"
)

var zeroByte = hexutility.EncodeTs(0)

type SequentialStream struct {
	from    uint64
	to      uint64
	current uint64
	closed  bool
}

func (s *SequentialStream) Next() (VKType, error) {
	if s.closed {
		return 0, io.EOF
	}
	if s.current < s.to {
		s.current++
		return VKType(s.current), nil
	}
	return 0, io.EOF
}

func (s *SequentialStream) HasNext() bool {
	if s.closed {
		return false
	}
	return s.current < s.to
}

func (s *SequentialStream) Close() {
	s.closed = true
}

// plains

type NoopTransformer struct{}

func (p *NoopTransformer) Transform(sourceKey VKType, value VVType) (data VVType, shouldSkip bool, err error) {
	return value, false, nil
}

func NewSequentialStream(from uint64, to uint64) stream.Uno[VKType] {
	return &SequentialStream{
		from:    from,
		to:      to,
		current: from,
	}
}

// take values from valsTbl and dump it in snapshot files.
func NewPlainFreezer(valsTable string, gen SourceKeyGenerator) Freezer {
	return &BaseFreezer{
		gen:     gen,
		proc:    &NoopTransformer{},
		valsTbl: valsTable,
	}
}
