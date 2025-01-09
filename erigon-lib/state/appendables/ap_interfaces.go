package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"golang.org/x/sync/errgroup"
)

// why this is needed?
// commenting this out because: appendable only checks if enough "stuff" is present in db
type CanFreeze interface {
	Evaluate(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) (bool, error)
}

type VKType uint64 // value table key type
type VVType []byte // value table value type

// we now think of appendable as simply a mapping from incremental tsIds to entity-value.
// no forkId.
// also unwind does remove data, so valsTbl only stores canonical values.
// this means blocks/headers/txs; caplin blocks/blobs are not supported
type Appendable interface {
	SetSourceKeyGenerator(gen SourceKeyGenerator)
	SetFreezer(freezer Freezer)
	SetIndexBuilders(ib []AccessorIndexBuilder)
	SetCanFreeze(canFreeze CanFreeze)

	// freeze
	BuildFiles(ctx context.Context, stepKeyFrom, stepKeyTo uint64, db kv.RoDB, ps *background.ProgressSet) error

	// Collate(ctx context.Context, stepKeyTo uint64, tx kv.Tx) (ap *AppendableCollation, created bool, err error)
	// BuildFiles(ctx context.Context, step uint64, coll AppendableCollation, ps *background.ProgressSet) (AppendableFiles, error)
	BuildMissedIndexes(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet)

	Prune(ctx context.Context, stepKeyTo, limit uint64, rwTx kv.RwTx) error // prune 0 to stepKeyTo
	Unwind(ctx context.Context, stepKeyFrom uint64, rwTx kv.RwTx) error     // stepKey or tsId/tsNum -- stepKey with SourceKeyGenerator is fine.

	// queries and put
	Get(tsNum uint64, roTx kv.Tx) (VVType, error)
	//NCGet(tsId uint64, forkId []byte, roTx kv.Tx) (VVType, error)
	Put(tsNum uint64, value VVType, rwTx kv.RwTx) error
}

type Collector func(values []byte) error

// NOTE: Freezer should be agnostic of any appendable stuff
// pattern is SetCollector ; and then call Freeze
type Freezer interface {
	// stepKeyFrom/To represent tsNum which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, stepKeyFrom uint64, stepKeyTo uint64, tx kv.Tx) (lastKeyValue uint64, err error)
	SetCollector(coll Collector)
}
