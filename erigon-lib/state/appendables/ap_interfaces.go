package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/seg"
	"golang.org/x/sync/errgroup"
)

type SourceKeyGenerator[SKey any] interface {
	FromStepKey(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) stream.Uno[SKey]
	FromTsNum(tsNum uint64, tx kv.Tx) SKey
	FromTsId(tsId uint64, forkId []byte, tx kv.Tx) SKey
}

type ValueFetcher[SKey any, SVal any] interface {
	GetValues(sourceKey SKey, tx kv.Tx) (value SVal, shouldSkip bool, found bool, err error)
}

type ValueProcessor[SKey any, SVal any] interface {
	Process(sourceKey SKey, value SVal) (data SVal, shouldSkip bool, err error)
}

type ValuePutter[SVal any] interface {
	Put(tsId uint64, forkId []byte, value SVal, tx kv.RwTx) error
}

type CanFreeze interface {
	Evaluate(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) (bool, error)
}

type Appendable[SKey any, SVal any] interface {
	SetSourceKeyGenerator(gen SourceKeyGenerator[SKey])
	SetValueFetcher(fet ValueFetcher[SKey, SVal])
	SetValueProcessor(proc ValueProcessor[SKey, SVal])
	SetValuePutter(put ValuePutter[SVal])

	SetFreezer(freezer Freezer[*AppendableCollation])
	SetIndexBuilders(ib []AccessorIndexBuilder)
	SetCanFreeze(canFreeze CanFreeze)
	SetRoSnapshots(rosnapshots *RoSnapshots[*AppendableCollation])

	// freeze
	BuildFiles(ctx context.Context, stepKeyFrom, stepKeyTo uint64, db kv.RoDB, ps *background.ProgressSet) error

	// Collate(ctx context.Context, stepKeyTo uint64, tx kv.Tx) (ap *AppendableCollation, created bool, err error)
	// BuildFiles(ctx context.Context, step uint64, coll AppendableCollation, ps *background.ProgressSet) (AppendableFiles, error)
	BuildMissedIndexes(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet)

	Prune(ctx context.Context, limit uint64, rwTx kv.RwTx) error

	// queries and put
	Get(tsNum uint64, roTx kv.Tx) (SVal, error)
	NCGet(tsId uint64, forkId []byte, roTx kv.Tx) (SVal, bool, error)

	Put(tsId uint64, forkId []byte, value SVal, rwTx kv.RwTx) error
}

type Collector func(values []byte) error

// NOTE: Freezer should be agnostic of any appendable stuff
// pattern is SetCollector, (maybe) CompressorWorkers; and then call Freeze
type Freezer[CollationType MinimalCollation] interface {
	// stepKeyFrom/To represent tsNum which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, stepKeyFrom uint64, stepKeyTo uint64, tx kv.Tx) (lastKeyValue uint64, err error)
	SetCollector(coll Collector)
	GetCompressorWorkers() uint64
	SetCompressorWorkers(uint64)
}

type SnapshotConfig struct {
	StepSize         uint64 // range width (#stepKeys) of snapshot file
	LeaveStepKeyInDb uint64 // number of element to leave in db
	// note that both of these are in terms of the "step key" rather than tsNum.
	// e.g. for txs, we decide to leave x number of blocks in db, and as a result,
	// the transactions of those x blocks will also be left in db.
}

type MinimalCollation interface {
	SetValuesComp(*seg.Compressor)
	GetValuesComp() *seg.Compressor
	SetValuesPath(string)
	GetValuesPath() string
	SetValuesCount(int)
	GetValuesCount() int
}

type AppendableCollation struct {
	valuesComp  *seg.Compressor
	valuesPath  string // TODO: should be a struct which contains version, step, appendable name/type
	valuesCount int
}

// AppendableCollation implements MinimalCollation

func (ac *AppendableCollation) SetValuesComp(comp *seg.Compressor) {
	ac.valuesComp = comp
}

func (ac *AppendableCollation) GetValuesComp() *seg.Compressor {
	return ac.valuesComp
}

func (ac *AppendableCollation) SetValuesPath(path string) {
	ac.valuesPath = path
}

func (ac *AppendableCollation) GetValuesPath() string {
	return ac.valuesPath
}

func (ac *AppendableCollation) SetValuesCount(count int) {
	ac.valuesCount = count
}

func (ac *AppendableCollation) GetValuesCount() int {
	return ac.valuesCount
}
