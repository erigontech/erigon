package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"golang.org/x/sync/errgroup"
)

type CanFreeze interface {
	Evaluate(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) (bool, error)
}

type VKType []byte // value table key type
type VVType []byte // value table value type

type Appendable interface {
	// SetSourceKeyGenerator(gen SourceKeyGenerator[SKey])
	// SetValueFetcher(fet ValueFetcher[SKey, SVal])
	// SetValuePutter(put ValuePutter[SVal])

	SetFreezer(freezer Freezer)
	SetIndexBuilders(ib []AccessorIndexBuilder)
	SetCanFreeze(canFreeze CanFreeze)
	GetRoSnapshots() *RoSnapshots

	// freeze
	BuildFiles(ctx context.Context, stepKeyFrom, stepKeyTo uint64, db kv.RoDB, ps *background.ProgressSet) error

	// Collate(ctx context.Context, stepKeyTo uint64, tx kv.Tx) (ap *AppendableCollation, created bool, err error)
	// BuildFiles(ctx context.Context, step uint64, coll AppendableCollation, ps *background.ProgressSet) (AppendableFiles, error)
	BuildMissedIndexes(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet)

	Prune(ctx context.Context, limit uint64, rwTx kv.RwTx) error
	Unwind(ctx context.Context, stepKeyFrom uint64, rwTx kv.RwTx) // stepKey or tsId/tsNum

	// queries and put
	Get(tsNum uint64, roTx kv.Tx) (VVType, bool, error)
	NCGet(tsId uint64, forkId []byte, roTx kv.Tx) (VVType, bool, error)

	Put(tsId uint64, forkId []byte, value VVType, rwTx kv.RwTx) error
}

type Collector func(values []byte) error

// NOTE: Freezer should be agnostic of any appendable stuff
// pattern is SetCollector, (maybe) CompressorWorkers; and then call Freeze
// TODO: can Freeze accept step? Depends on if it's only used to create the minimal snapshot...
type Freezer interface {
	// stepKeyFrom/To represent tsNum which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, stepKeyFrom uint64, stepKeyTo uint64, tx kv.Tx) (lastKeyValue uint64, err error)
	SetCollector(coll Collector)
	GetCompressorWorkers() uint64
	SetCompressorWorkers(uint64)
}

type SnapshotConfig struct {
	StepSize         uint64 // range width (#stepKeys) of snapshot file i.e. #stepKeys per step
	LeaveStepKeyInDb uint64 // number of element to leave in db
	// note that both of these are in terms of the "step key" rather than tsNum.
	// e.g. for txs, we decide to leave x number of blocks in db, and as a result,
	// the transactions of those x blocks will also be left in db.
}
