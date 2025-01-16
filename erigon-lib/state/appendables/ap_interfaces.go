package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
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
	RecalcVisibleFiles(baseTsNumTo TsNum)
	Prune(ctx context.Context, baseTsNumTo TsNum, limit uint64, rwTx kv.RwTx) error
	Unwind(ctx context.Context, baseTsNumFrom TsNum, rwTx kv.RwTx) error

	// don't put BeginFilesRo here, since it returns PointQueries, RangedQueries etc.
	// so anyway aggregator has to recover concrete type, and then it can 
	// call BeginFilesRo on that
}

// appendableRoTx extensions...providing different query patterns
// idea is that aggregator "return" a *Queries interface, and user can do Get/Put/Range on that.
// alternate is to expose eveything, but that means exposing tsId/forkId etc. even for appendables
// for which it is not relevant. Plus, sometimes base appendable tsNum should also be managed...
// type PointQueries interface {
// 	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
// 	Put(tsNum TsNum, value VVType, tx kv.RwTx) error
// }

// type RangedQueries interface {
// 	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
// 	Put(tsNum TsNum, value VVType, tx kv.RwTx) error
// 	PutEntityEnd(tsNum TsNum, startBaseTsNum TsNum, tx kv.RwTx) error
// }

// type MarkedQueries interface {
// 	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
// 	GetNc(tsId TsId, forkId []byte, tx kv.Tx) (VVType, error)
// 	Put(tsId TsId, forkId []byte, value VVType, tx kv.RwTx) error
// }
