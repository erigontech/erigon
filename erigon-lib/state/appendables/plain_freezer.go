package appendables

import (
	"context"
	"io"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
)

type ValueKeyFetcher interface {
	// stream of valsTbl keys
	GetKeys(baseNumFrom, baseNumTo Num, tx kv.Tx) stream.Uno[VKType]
}

type PlainFreezer struct {
	fetcher ValueKeyFetcher
	valsTbl string
	coll    Collector
}

// what does this do?
func (sf *PlainFreezer) Freeze(ctx context.Context, baseNumFrom, baseNumTo Num, tx kv.Tx) error {
	can_stream := sf.fetcher.GetKeys(baseNumFrom, baseNumTo, tx)
	for can_stream.HasNext() {
		key, err := can_stream.Next()
		if err != nil {
			return err
		}

		data, err := tx.GetOne(sf.valsTbl, key)
		if err != nil {
			return err
		}

		if err := sf.coll(data); err != nil {
			return err
		}
	}

	return nil
}

func (sf *PlainFreezer) SetCollector(coll Collector) {
	sf.coll = coll
}

// sequential stream

var zeroByte = hexutility.EncodeTs(0)

type SequentialStream struct {
	from    uint64
	to      uint64
	current uint64
	closed  bool
}

func (s *SequentialStream) Next() (VKType, error) {
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

func NewSequentialStream(from uint64, to uint64) stream.Uno[VKType] {
	return &SequentialStream{
		from:    from,
		to:      to,
		current: from,
	}
}
