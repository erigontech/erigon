package impls

import (
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	ca "github.com/erigontech/erigon-lib/state/appendables"
	"github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/snapshotsync"
)

const (
	BorSpans ca.ApEnum = "borspans.appe"
)

// func init() {
// 	ca.RegisterAppendable(BorSpans, NewSpanAppendable(kv.BorSpans))
// 	// even agg is initialized, it'll pick appendables from registry
// 	// and set it in itself.
// }

// type SpanAppendable struct {
// 	*ca.BaseAppendable
// }

func NewSpanAppendable(valsTable string, blockSnapshot *snapshotsync.RoSnapshots) ca.Appendable {
	ap := ca.NewBaseAppendable(BorSpans, kv.BorSpans)

	gen := &SpanSourceKeyGenerator{}
	ap.SetSourceKeyGenerator(gen)
	ap.SetFreezer(ca.NewPlainFreezer(valsTable, gen))

	salt := uint32(4343) // load from salt-blocks.txt etc.

	indexb := ca.NewSimpleAccessorBuilder(ca.NewAccessorArgs(true, false, false, salt), BorSpans, string(BorSpans))
	ap.SetIndexBuilders([]ca.AccessorIndexBuilder{indexb})

	ap.SetCanFreeze(&followBlock{blockSnapshot})
	return ap
}

type SpanSourceKeyGenerator struct{}

func (s *SpanSourceKeyGenerator) FromStepKey(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) stream.Uno[ca.VKType] {
	spanFrom := heimdall.SpanIdAt(stepKeyFrom)
	spanTo := heimdall.SpanIdAt(stepKeyTo)
	return ca.NewSequentialStream(uint64(spanFrom), uint64(spanTo))
}

type followBlock struct {
	blockSnapshot *snapshotsync.RoSnapshots
}

func (f *followBlock) Evaluate(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) (bool, error) {
	return f.blockSnapshot.DirtyBlocksAvailable(snaptype.Enums.Bodies) > stepKeyTo, nil
}
