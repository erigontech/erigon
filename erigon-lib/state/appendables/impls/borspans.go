package impls

import (
	"context"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	ca "github.com/erigontech/erigon-lib/state/appendables"
	"github.com/erigontech/erigon/polygon/heimdall"
)

// appendables which don't store non-canonical data
// 1. or which tsId = tsNum always
// 2. bor spans/milestones/checkpoints
// 3. two level lookup: stepKey -> tsId/tsNum -> value (non-range lookup)
/// forget about blobs right now

// 1. a valsTable stores the value simply

const (
	BorSpans ca.ApEnum = "borspans.appe"
)

// func init() {
// 	ca.RegisterAppendable(BorSpans, NewSpanAppendable(kv.BorSpans))
// 	// even agg is initialized, it'll pick appendables from registry
// 	// and set it in itself.
// }

type SpanAppendable struct {
	*ca.BaseAppendable
	valsTable string
}

func NewSpanAppendable(valsTable string) *SpanAppendable {
	ap := &SpanAppendable{
		BaseAppendable: ca.NewBaseAppendable(BorSpans),
		valsTable:      valsTable,
	}

	gen := &SpanSourceKeyGenerator{}
	ap.SetSourceKeyGenerator(gen)
	ap.SetValueFetcher(ca.NewPlainFetcher(valsTable))
	ap.SetValuePutter(ca.NewPlainPutter(valsTable))
	ap.SetFreezer(ca.NewPlainFreezer(valsTable, gen))

	salt := uint32(4343) // load from salt-blocks.txt etc.

	indexb := ca.NewSimpleAccessorBuilder(ca.NewAccessorArgs(true, false, false, salt), BorSpans, string(BorSpans))
	ap.SetIndexBuilders([]ca.AccessorIndexBuilder{indexb})
	return ap
}

// prune function
func (ap *SpanAppendable) Prune(ctx context.Context, limit uint64, rwTx kv.RwTx) error {
	// TODO
	return nil
}

func (ap *SpanAppendable) Unwind(ctx context.Context, stepKeyFrom uint64, rwTx kv.RwTx) {
	cursor, err := rwTx.Cursor(ap.valsTable)
	if err != nil {
		return
	}

	defer cursor.Close()

}

type SpanSourceKeyGenerator struct{}

func (s *SpanSourceKeyGenerator) FromStepKey(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) stream.Uno[ca.VKType] {
	spanFrom := heimdall.SpanIdAt(stepKeyFrom)
	spanTo := heimdall.SpanIdAt(stepKeyTo)
	return ca.NewSequentialStream(uint64(spanFrom), uint64(spanTo))
}

func (s *SpanSourceKeyGenerator) FromTsNum(tsNum uint64, tx kv.Tx) ca.VKType {
	return hexutility.EncodeTs(tsNum)
}

func (s *SpanSourceKeyGenerator) FromTsId(tsId uint64, forkId []byte, tx kv.Tx) ca.VKType {
	return hexutility.EncodeTs(tsId)
}

// two things to set
// 1. source key generator
// 2. Can freeze
