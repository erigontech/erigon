package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
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

type ValuePutter[SVal any] interface {
	Put(tsId uint64, forkId []byte, value SVal, tx kv.RwTx) error
}

type BaseAppendable[SKey any, SVal any] struct {
	gen SourceKeyGenerator[SKey]
	fet ValueFetcher[SKey, SVal]
	put ValuePutter[SVal]

	freezer       Freezer
	indexBuilders []AccessorIndexBuilder

	canFreeze CanFreeze

	rosnapshot *RoSnapshots
	enum       ApEnum
}

// setters

func (ap *BaseAppendable[SKey, SVal]) Get(tsNum uint64, tx kv.Tx) (SVal, bool, error) {
	vkey := ap.gen.FromTsNum(tsNum, tx)
	val, _, found, err := ap.fet.GetValues(vkey, tx)
	// var ba Appendable[uint64, []byte]
	// ba = &BaseAppendable[uint64, []byte]{}
	//_, _ = ba.UnbuiltStepsTill(tsNum)
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

// this can also be done in rosnapshots. The (supposed) advantage is that the same logic works for domain/history/ii
// but need to check further.
func (ap *BaseAppendable[SKey, SVal]) BuildFiles(ctx context.Context, stepKeyFrom, stepKeyTo uint64, db kv.RoDB, ps *background.ProgressSet) error {
	// find if can freeze
	// freeze
	// build indexes
	// register to rosnapshot

	config := ap.rosnapshot.GetSnapshotConfig(ap.enum)
	step := stepKeyTo / config.StepSize
	stepKeyFrom, stepKeyTo := step*config.StepSize, (step+1)*config.StepSize
	canDo := false
	if err := db.View(ctx, func(tx kv.Tx) error {
		if ok, err := ap.canFreeze.Evaluate(stepKeyFrom, stepKeyTo, tx); !ok {
			// can't freeze
			return err
		}
		canDo = true
		return nil
	}); err != nil {
		return nil
	}
	if !canDo {
		return nil
	}

	name := string(ap.enum)
	path := ap.enum.GetSnapshotName(stepKeyFrom, stepKeyTo)

	sn, err := seg.NewCompressor(ctx, "Snapshot "+name, path, tmpDir, seg.DefaultCfg, log.LvlTrace, logger)

	// freeze
	ap.freezer.SetCollector(func(values []byte) error {
		return sn.AddWord(values)
	})
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = ap.freezer.Freeze(ctx, stepKeyFrom, stepKeyTo, tx)
	if err != nil {
		return err
	}
	tx.Rollback()
	if err := sn.Compress(); err != nil {
		return err
	}
	sn.Close()
	sn = nil

	valuesDecomp, err := seg.NewDecompressor(path)
	if err != nil {
		return err
	}

	dseg := &DirtySegment{
		Range:        Range{stepKeyFrom, stepKeyTo},
		Decompressor: valuesDecomp,
		filePath:     path,
		enum:         ap.enum,
	}

	// register to rosnapshot first, so it's available to components
	// like index builder which (often) might rely on rosnapshot to retrieve the
	// decompressor
	ap.rosnapshot.RegisterSegment(ap.enum, dseg)

	indexes := make([]*recsplit.Index, len(ap.indexBuilders))
	for _, ib := range ap.indexBuilders {
		recsplitIdx, err := ib.Build(ctx, stepKeyFrom, stepKeyTo, "sometmpdir", ps, log.LvlInfo, nil)
		if err != nil {
			return err
		}

		indexes = append(dseg.indexes, recsplitIdx)
	}

	// should this use a lock? or is it ok?
	dseg.indexes = indexes
	return nil
}

func (ap *BaseAppendable[SKey, SVal]) BuildMissedIndexes(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {

}

func (ap *BaseAppendable[SKey, SVal]) Prune(ctx context.Context, limit uint64, rwTx kv.RwTx) error {
	return nil
}

// collate
// TODO: doubt, passing tx to freeze rather than db...
// was passing db earlier as wanted to do bigchunks, but alex said it doesn't
// matter now as build files is separate step.
// func (ap *BaseAppendable[SKey, SVal]) Collate(ctx context.Context, step uint64, tx kv.Tx) (*AppendableCollation, bool, error) {

// 	// coll, _, err := ap.freezer.Freeze(ctx, stepKeyTo, roDb)
// 	// if err != nil {
// 	// 	return ac, false, err
// 	// }
// 	// return coll, true, nil

// 	// 0. find stepKeyFrom
// 	// 1. check if collation can happen at stepKeyTo
// 	// 2. if yes, proceed with the freeze.
// 	config := ap.rosnapshot.GetSnapshotConfig(ap.enum)
// 	stepKeyFrom, stepKeyTo := step*config.StepSize, (step+1)*config.StepSize
// 	if ok, err := ap.canFreeze.Evaluate(stepKeyFrom, stepKeyTo, tx); !ok {
// 		// can't freeze
// 		return nil, ok, err
// 	}

// 	//collation, err := ap.rosnapshot.Freeze(ctx, ap.enum, stepKeyFrom, stepKeyTo, roDb)
// 	coll, _, err := ap.freezer.Freeze(ctx, stepKeyFrom, stepKeyTo, tx)
// 	if err != nil {
// 		return nil, false, err
// 	}

// 	return coll, true, nil
// }

// func (ap *BaseAppendable[SKey, SVal]) BuildFiles(ctx context.Context, step uint64, coll AppendableCollation, ps *background.ProgressSet) (AppendableFiles, error) {
// 	if err := coll.valuesComp.Compress(); err != nil {
// 		return AppendableFiles{}, err
// 	}
// 	coll.valuesComp.Close()
// 	coll.valuesComp = nil
// 	var valuesDecomp *seg.Decompressor
// 	var err error
// 	if valuesDecomp, err = seg.NewDecompressor(coll.valuesPath); err != nil {
// 		return AppendableFiles{}, err
// 	}

// 	// build index
// 	// TODO: this should be delegated to rosnapshots. Should it? why...
// 	indexes := make([]*recsplit.Index, len(ap.indexBuilders))
// 	for _, ib := range ap.indexBuilders {
// 		iidq := ib.GetInputDataQuery(step)
// 		desc := IndexDescriptor{
// 			IndexPath:  "something",
// 			BaseDataId: iidq.GetBaseDataId(),
// 		}
// 		recsplitIdx, err := ib.Build(ctx, desc, "sometmpdir", coll, ps, log.LvlInfo, logger)
// 		if err != nil {
// 			return AppendableFiles{}, err
// 		}

// 		indexes = append(indexes, recsplitIdx)
// 	}

// 	// GetIndexInputDataQueryFor()

// 	return AppendableFiles{
// 		valuesDecomp: valuesDecomp,
// 		// nil indexes
// 		indexes: indexes,
// 	}, nil
// }

// set index builders
func (ap *BaseAppendable[SKey, SVal]) SetIndexBuilders(ib []AccessorIndexBuilder) {
	ap.indexBuilders = ib
}

func (ap *BaseAppendable[SKey, SVal]) SetCanFreeze(canFreeze CanFreeze) {
	ap.canFreeze = canFreeze
}

func (ap *BaseAppendable[SKey, SVal]) SetFreezer(freezer Freezer) {
	ap.freezer = freezer
}

func (ap *BaseAppendable[SKey, SVal]) SetRoSnapshots(rs *RoSnapshots) {
	ap.rosnapshot = rs
}

func (ap *BaseAppendable[SKey, SVal]) SetSourceKeyGenerator(gen SourceKeyGenerator[SKey]) {
	ap.gen = gen
}

func (ap *BaseAppendable[SKey, SVal]) SetValueFetcher(fet ValueFetcher[SKey, SVal]) {
	ap.fet = fet
}

func (ap *BaseAppendable[SKey, SVal]) SetValuePutter(put ValuePutter[SVal]) {
	ap.put = put
}

type AppendableFiles struct {
	valuesDecomp *seg.Decompressor
	indexes      []*recsplit.Index
}
