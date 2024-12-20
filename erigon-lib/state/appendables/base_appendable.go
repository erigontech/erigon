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

	canFreeze CanFreeze

	rosnapshot *RoSnapshots[*AppendableCollation]
	enum       EENum
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

func (ap *BaseAppendable[SKey, SVal]) UnbuiltStepsTill(stepKeyTo uint64) (startStep, endStep uint64) {
	// this method should actually reside in rosnapshots
	// with the caveat that stepKeyTo sent has considered the leaveInDb info
	startStep, _ = ap.rosnapshot.LastStepInSnapshot(ap.enum)
	startStep++
	config := ap.rosnapshot.GetSnapshotConfig(ap.enum)
	endStep = startStep + (1+stepKeyTo-startStep*config.StepSize)/config.StepSize
	return
}

// collate
// TODO: doubt, passing tx to freeze rather than db...
// was passing db earlier as wanted to do bigchunks, but alex said it doesn't
// matter now as build files is separate step.
func (ap *BaseAppendable[SKey, SVal]) Collate(ctx context.Context, step uint64, roDb kv.RoDB) (ac *AppendableCollation, created bool, err error) {

	// coll, _, err := ap.freezer.Freeze(ctx, stepKeyTo, roDb)
	// if err != nil {
	// 	return ac, false, err
	// }
	// return coll, true, nil

	// 0. find stepKeyFrom
	// 1. check if collation can happen at stepKeyTo
	// 2. if yes, proceed with the freeze.
	config := ap.rosnapshot.GetSnapshotConfig(ap.enum)
	stepKeyFrom, stepKeyTo := step*config.StepSize, (step+1)*config.StepSize
	tx, err := roDb.BeginRo(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	if ok, err := ap.canFreeze.Evaluate(stepKeyFrom, stepKeyTo, tx); !ok {
		// can't freeze
		return nil, ok, err
	}
	tx.Rollback()

	collation, err := ap.rosnapshot.Freeze(ctx, ap.enum, stepKeyFrom, stepKeyTo, roDb)
	if err != nil {
		return nil, false, err
	}

	return collation, true, nil

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

func (ap *BaseAppendable[SKey, SVal]) SetCanFreeze(canFreeze CanFreeze) {
	ap.canFreeze = canFreeze
}

func (ap *BaseAppendable[SKey, SVal]) SetRoSnapshots(rs *RoSnapshots[*AppendableCollation]) {
	ap.rosnapshot = rs
}

type AppendableFiles struct {
	valuesDecomp *seg.Decompressor
	indexes      []*recsplit.Index
}
