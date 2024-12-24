package appendables

import (
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon/polygon/heimdall"
)

type SpanSourceKeyGenerator struct{}

func (s *SpanSourceKeyGenerator) FromStepKey(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) stream.Uno[[]byte] {
	spanFrom := heimdall.SpanIdAt(stepKeyFrom)
	spanTo := heimdall.SpanIdAt(stepKeyTo)
	return NewSequentialStream(uint64(spanFrom), uint64(spanTo))
}

func (s *SpanSourceKeyGenerator) FromTsNum(tsNum uint64, tx kv.Tx) []byte {
	return hexutility.EncodeTs(tsNum)
}

func (s *SpanSourceKeyGenerator) FromTsId(tsId uint64, forkId []byte, tx kv.Tx) []byte {
	return hexutility.EncodeTs(tsId)
}
