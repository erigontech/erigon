package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
)

// 3 appendable categories:
// 1. CanonicalAppendable: only valsTbl; only canonical data stored (unwind removes all non-canonical data)
// 2. MarkedAppendables: canonicalMarkerTbl and valsTbl
// 3. MixedAppendable: tsId always increments...provenance coming from some other structure;

// --------------------------------------------------------------------

// vals table k,v types
type VKType []byte
type VVType []byte

type Collector func(values []byte) error

// pattern is SetCollector ; and then call Freeze
type Freezer interface {
	// stepKeyFrom/To represent tsNum which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, stepKeyFrom uint64, stepKeyTo uint64, tx kv.Tx) error
	SetCollector(coll Collector)
}

type Appendable interface {
	SetFreezer(Freezer)
	SetTsNumMapper(TsNumMapper)
	DirtySegmentsMaxTsNum() uint64
	VisibleSegmentsMaxTsNum() uint64
}

// converts baseTsNum to tsId of the appendable
// this should only refer to hot db, and not the snapshot files.
type TsNumMapper interface {
	Get(baseTsNumFrom, baseTsNumTo uint64, tx kv.Tx) stream.Uno[VKType]
}
