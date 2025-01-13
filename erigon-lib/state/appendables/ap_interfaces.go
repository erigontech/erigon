package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
)

// 3 appendable categories:
// 1. CanonicalAppendable: only valsTbl; only canonical data stored (unwind removes all non-canonical data)
// 2. MarkedAppendables: canonicalMarkerTbl and valsTbl
// 3. IncrementingAppendable: tsId always increments...provenance coming from some other structure;
//            (useful when you don't want data to be lost on unwind..e.g. transactions)

// --------------------------------------------------------------------

// vals table k,v types
type VKType []byte
type VVType []byte
type TsNum uint64
type TsId uint64

type Collector func(values []byte) error

// pattern is SetCollector ; and then call Freeze
type Freezer interface {
	// stepKeyFrom/To represent tsNum which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, baseTsNumFrom, baseTsNumTo TsNum, tx kv.Tx) error
	SetCollector(coll Collector)
}

type Appendable interface {
	SetFreezer(Freezer)
	SetIndexBuilders([]AccessorIndexBuilder)
	DirtySegmentsMaxTsNum() TsNum
	VisibleSegmentsMaxTsNum() TsNum
	Prune(ctx context.Context, baseKeyTo TsNum, limit uint64, rwTx kv.RwTx) error
	Unwind(ctx context.Context, baseKeyFrom TsNum, rwTx kv.RwTx) error
}

// appendable extensions...providing different query patterns
// idea is that aggregator "return" a *Queries interface, and user can do Get/Put/Range on that.
// alternate is to expose eveything, but that means exposing tsId/forkId etc. even for appendables
// for which it is not relevant.
type CanonicalQueries interface {
	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
	Put(tsNum TsNum, value VVType, tx kv.RwTx) error
}

type MarkedQueries interface {
	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
	GetNc(tsId TsId, forkId []byte, tx kv.Tx) (VVType, error)
	Put(tsId TsId, forkId []byte, value VVType, tx kv.RwTx) error
}

type IncrementalQueries interface {
	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
	GetNc(tsId TsId, tx kv.Tx) (VVType, error)
	Put(tsId TsId, value VVType, tx kv.RwTx) error
}

// type CanonicalAppendableI interface {
// 	Appendable
// 	CanonicalQueries
// }

// type MixedAppendableI interface {
// 	Appendable
// 	MixedQueries
// }

// type MarkedAppendableI interface {
// 	Appendable
// 	MarkedQueries
// }

// converts baseTsNum to tsId of the appendable
// this should only refer to hot db, and not the snapshot files.
type TsNumMapper interface {
	Get(baseTsNumFrom, baseTsNumTo TsNum, tx kv.Tx) stream.Uno[VKType]
}
