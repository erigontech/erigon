package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/seg"
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
	Process(sourceKey SKey, value SVal) (data any, shouldSkip bool, err error)
}

type ValuePutter[SVal any] interface {
	Put(tsId uint64, forkId []byte, value SVal, tx kv.RwTx) error
}

type Appendable[SKey any, SVal any] interface {
	SetSourceKeyGenerator(gen SourceKeyGenerator[SKey])
	SetValueFetcher(fet ValueFetcher[SKey, SVal])
	SetValueProcessor(proc ValueProcessor[SKey, SVal])
	SetValuePutter(put ValuePutter[SVal])

	SetFreezer(freezer Freezer[*AppendableCollation])
	SetIndexBuilders(ib []IndexBuilder)

	// freeze
	Collate(ctx context.Context, stepKeyTo uint64, tx kv.Tx) (AppendableCollation, created bool, err error)

	Prune(ctx context.Context, limit uint64, rwTx kv.RwTx) error

	// queries and put
	Get(tsNum uint64, roTx kv.Tx) (SVal, error)
	NCGet(tsId uint64, forkId []byte, roTx kv.Tx) (SVal, bool, error)

	Put(tsId uint64, forkId []byte, value SVal, rwTx kv.RwTx) error
}

// NOTE: Freezer & FreezeConfig should be agnostic of any appendable stuff
type Freezer[CollationType MinimalCollation] interface {
	// stepKeyFrom/To represent tsNum which the snapshot should range
	Freeze(ctx context.Context, stepKeyTo uint64, roDB kv.RoDB) (collation CollationType, lastKeyValue uint64, err error)
	FreezeConfig() *FreezeConfig
	GetCompressorWorkers() uint64
	SetCompressorWorkers(uint64)
}

type FreezeConfig struct {
	stepSize     uint64 // range width (#stepKeys) of snapshot file
	stepSizeInDb uint64 // number of element to leave in db
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
	valuesPath  string  // TODO: should be a struct which contains version, step, appendable name/type
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
