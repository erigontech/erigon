package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

type BaseAppendable[SKey any, SVal any] struct {
	gen  SourceKeyGenerator[SKey]
	fet  ValueFetcher[SKey, SVal]
	proc ValueProcessor[SKey, SVal]
	put  ValuePutter[SVal]

	freezer       Freezer[*AppendableCollation]
	indexBuilders []IndexBuilder
}

// setters

func (ap *BaseAppendable[SKey, SVal]) Get(tsNum uint64, tx kv.Tx) (SVal, bool, error) {
	vkey := ap.gen.FromTsNum(tsNum, tx)
	val, _, found, err := ap.fet.GetValues(vkey, tx)
	return val, found, err
}

func (ap *BaseAppendable[SKey, SVal]) NCGet(tsId uint64, forkId []byte, tx kv.Tx) (SVal, bool, error) {
	vkey := ap.gen.FromTsId(tsId, forkId, tx)
	val, _, found, err := ap.fet.GetValues(vkey, tx)
	return val, found, err
}

func (ap *BaseAppendable[SKey, SVal]) Put(tsId uint64, forkId []byte, value SVal, tx kv.RwTx) error {
	return ap.put.Put(tsId, forkId, value, tx)
}

// collate
// TODO: doubt, passing tx to freeze rather than db...
// was passing db earlier as wanted to do bigchunks, but alex said it doesn't
// matter now as build files is separate step.
func (ap *BaseAppendable[SKey, SVal]) Collate(ctx context.Context, stepKeyTo uint64, roDb kv.RoDB) (ac *AppendableCollation, created bool, err error) {
	// 1. this calls freezer, which can freeze if enough data and based on config etc.
	// 2. TODO: this should be delegated to rosnapshots actually, which then delegates to freezer
	coll, _, err := ap.freezer.Freeze(ctx, stepKeyTo, roDb)
	if err != nil {
		return ac, false, err
	}
	return coll, true, nil
}

func (ap *BaseAppendable[SKey, SVal]) BuildFiles(ctx context.Context, step uint64, coll AppendableCollation) (AppendableFiles, error) {
	if err := coll.valuesComp.Compress(); err != nil {
		return AppendableFiles{}, err
	}
	coll.valuesComp.Close()
	coll.valuesComp = nil
	var valuesDecomp *seg.Decompressor
	var err error
	if valuesDecomp, err = seg.NewDecompressor(coll.valuesPath); err != nil {
		return AppendableFiles{}, err
	}

	// build index
	// TODO: this should be delegated to rosnapshots
	for _, ib := range ap.indexBuilders {
	 	//ib.Build(ctx, )
	}

	// GetIndexInputDataQueryFor()

	return AppendableFiles{
		valuesDecomp: valuesDecomp,
		// nil indexes
		indexes: []*recsplit.Index{},
	}, nil
}

// set index builders
func (ap *BaseAppendable[SKey, SVal]) SetIndexBuilders(ib []IndexBuilder) {
	ap.indexBuilders = ib
}

type AppendableFiles struct {
	valuesDecomp *seg.Decompressor
	indexes      []*recsplit.Index
}
