package appendables

import (
	"io"

	"github.com/erigontech/erigon-lib/common/hexutility"
)

var zeroByte = hexutility.EncodeTs(0)

type SequentialStream struct {
	from    uint64
	to      uint64
	current uint64
	closed  bool
}

func (s *SequentialStream) Next() ([]byte, error) {
	if s.closed {
		return zeroByte, io.EOF
	}
	if s.current < s.to {
		s.current++
		return hexutility.EncodeTs(s.current), nil
	}
	return zeroByte, io.EOF
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
