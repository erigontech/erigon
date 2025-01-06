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

// plains

type PlainProcessor struct{}

func (p *PlainProcessor) Process(sourceKey VKType, value VVType) (data VVType, shouldSkip bool, err error) {
	return value, false, nil
}

func NewSequentialStream(from uint64, to uint64) stream.Uno[VKType] {
	return &SequentialStream{
		from:    from,
		to:      to,
		current: from,
	}
}

type PlainFreezer struct {
	*BaseFreezer
	valsTable string
}

func NewPlainFreezer(valsTable string, gen SourceKeyGenerator) *PlainFreezer {
	f := PlainFreezer{
		valsTable: valsTable,
	}

	f.BaseFreezer = &BaseFreezer{
		gen:  gen,
		fet:  &PlainFetcher{valsTable},
		proc: &PlainProcessor{},
	}
}

// type PlainFetcher struct {
// 	valsTable string
// }

// func NewPlainFetcher(valsTable string) *PlainFetcher {
// 	return &PlainFetcher{valsTable}
// }

// func (f *PlainFetcher) GetValues(sourceKey VKType, tx kv.Tx) (value VVType, shouldSkip bool, found bool, err error) {
// 	found = true
// 	shouldSkip = false
// 	value, err = tx.GetOne(f.valsTable, sourceKey)
// 	if err != nil {
// 		return nil, false, false, err
// 	}
// 	return value, shouldSkip, found, nil
// }

// type PlainPutter struct {
// 	valsTable string
// }

// func NewPlainPutter(valsTable string) *PlainPutter {
// 	return &PlainPutter{valsTable}
// }

// func (p *PlainPutter) Put(tsId uint64, forkId []byte, value VVType, tx kv.RwTx) error {
// 	return tx.Put(p.valsTable, hexutility.EncodeTs(tsId), value)
// }
