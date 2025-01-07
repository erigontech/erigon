package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"
)

// this generates keys for valsTable.
type SourceKeyGenerator interface {
	// this is needed in freezing, for which the input is stepKey
	FromStepKey(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) stream.Uno[VKType]

	// for canonical get
	FromTsNum(tsNum uint64, tx kv.Tx) VKType

	// for non-canonical get
	FromTsId(tsId uint64, forkId []byte, tx kv.Tx) VKType
}

// type ValueFetcher interface {
// 	// used in:
// 	// 1. freezing process, after SourceKeyGenerator#FromStepKey
// 	// 2. for get operations.
// 	GetValues(sourceKey VKType, tx kv.Tx) (value VVType, shouldSkip bool, found bool, err error)
// }

// removing ValuePutter, as in some cases (like polygon entities), putting means
// accepting blockNum as well..which is not possible here. So canonicalTbl stuff
// has to happen outside...
// type ValuePutter interface {
// 	Put(tsId uint64, forkId []byte, value VVType, tx kv.RwTx) error
// }

type BaseAppendable struct {
	gen     SourceKeyGenerator
	valsTbl string
	//put     ValuePutter

	freezer       Freezer
	indexBuilders []AccessorIndexBuilder

	canFreeze CanFreeze

	//rosnapshot *RoSnapshots
	enum ApEnum

	dirtyFiles *btree.BTreeG[*DirtySegment]
	_visible   []VisibleSegment

	// config stuff
	doesUnwind bool
	stepSize   uint64
}

func NewBaseAppendable(enum ApEnum, valsTbl string) *BaseAppendable {
	ap := &BaseAppendable{
		enum:    enum,
		valsTbl: valsTbl,
	}
	return ap
}

// setters

func (ap *BaseAppendable) Get(tsNum uint64, tx kv.Tx) (VVType, error) {
	vkey := ap.gen.FromTsNum(tsNum, tx)
	return tx.GetOne(ap.valsTbl, vkey)
	// val, _, found, err := ap.fet.GetValues(vkey, tx)

	// // var ba2 Appendable
	// // ba2 = &BaseAppendable{}

	// //_, _ = ba.UnbuiltStepsTill(tsNum)
	// return val, found, err
}

// ideally only db search
func (ap *BaseAppendable) NCGet(tsId uint64, forkId []byte, tx kv.Tx) (VVType, error) {
	vkey := ap.gen.FromTsId(tsId, forkId, tx)
	return tx.GetOne(ap.valsTbl, vkey)
}

func (ap *BaseAppendable) Put(tsId uint64, forkId []byte, value VVType, tx kv.RwTx) error {
	vkey := ap.gen.FromTsId(tsId, forkId, tx)
	if len(forkId) == 0 { // incremental assumed
		return tx.Append(ap.valsTbl, vkey, value)
	}
	return tx.Put(ap.valsTbl, vkey, value)
}

// func (ap *BaseAppendable) UnbuiltStepsTill(stepKeyTo uint64) (startStep, endStep uint64) {
// 	// this method should actually reside in rosnapshots
// 	// with the caveat that stepKeyTo sent has considered the leaveInDb info
// 	startStep, _ = ap.rosnapshot.LastStepInSnapshot(ap.enum)
// 	startStep++
// 	config := ap.rosnapshot.GetSnapshotConfig(ap.enum)
// 	endStep = startStep + (1+stepKeyTo-startStep*config.StepSize)/config.StepSize
// 	return
// }

// it takes `stepKeyFrom` because "snapshots retire" command takes arbitrary stepKeyFrom...
// If we don't want this command...stepKeyFrom can be removed, and assumed to be lastFrozen stepKey in func.
// "leaveInDb" or "never exceed block snapshots" is ensured by the caller to aggregator...not managed here...
func (ap *BaseAppendable) BuildFiles(ctx context.Context, stepKeyFrom, stepKeyTo uint64, db kv.RoDB, ps *background.ProgressSet) error {
	// this can also be done in rosnapshots. The (supposed) advantage is that the same logic works for domain/history/ii
	// but need to check further.
	//config := ap.rosnapshot.GetSnapshotConfig(ap.enum)
	stepFrom := stepKeyFrom / ap.stepSize
	stepTo := stepKeyTo / ap.stepSize

	for step := stepFrom; step < stepTo; step++ {
		stepKeyFrom, stepKeyTo := step*ap.stepSize, (step+1)*ap.stepSize // TODO: more complicated logic can be there e.g.. snapcfg.MergeLimit()

		// can freeze?
		canDo := false
		if err := db.View(ctx, func(tx kv.Tx) error {
			if ok, err := ap.canFreeze.Evaluate(stepKeyFrom, stepKeyTo, tx); !ok {
				// can't freeze
				return err
			}
			canDo = true
			return nil
		}); err != nil {
			return err
		}
		if !canDo {
			return nil
		}

		path := AppeSegName(ap.enum, 1, step, step+1)
		sn, err := seg.NewCompressor(ctx, "Snapshot "+string(ap.enum), path, tmpDir, seg.DefaultCfg, log.LvlTrace, logger)
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

		indexes := make([]*recsplit.Index, len(ap.indexBuilders))
		for _, ib := range ap.indexBuilders {
			var progresset *background.Progress //TODO
			recsplitIdx, err := ib.Build(ctx, stepKeyFrom, stepKeyTo, "sometmpdir", progresset, log.LvlInfo, nil)
			if err != nil {
				return err
			}

			indexes = append(dseg.indexes, recsplitIdx)
		}

		// should this use a lock?
		dseg.indexes = indexes
		ap.dirtyFiles.Set(dseg)
	}

	return nil
}

func (ap *BaseAppendable) BuildMissedIndexes(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {

}

func (ap *BaseAppendable) Prune(ctx context.Context, stepKeyTo, limit uint64, rwTx kv.RwTx) error {
	// similar to unwind, but with a limit and going reverse
	return nil
}

func (ap *BaseAppendable) Unwind(ctx context.Context, stepKeyFrom uint64, rwTx kv.RwTx) error {
	if !ap.doesUnwind {
		return nil
	}

	c, err := rwTx.Cursor(ap.valsTbl)
	if err != nil {
		return err
	}
	defer c.Close()

	stre := ap.gen.FromStepKey(stepKeyFrom, stepKeyFrom+1, rwTx)
	defer stre.Close()
	vkey, err := stre.Next()
	if err != nil {
		return err
	}

	stre.Close()

	firstK, _, err := c.Seek(vkey)
	if err != nil {
		return err
	}
	if firstK == nil {
		return nil
	}

	for k, _, err := c.Current(); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}

		if err := rwTx.Delete(ap.valsTbl, k); err != nil {
			return err
		}
	}

	return nil
}

func (ap *BaseAppendable) SetIndexBuilders(ib []AccessorIndexBuilder) {
	ap.indexBuilders = ib
}
func (ap *BaseAppendable) SetFreezer(freezer Freezer) {
	ap.freezer = freezer
}

func (ap *BaseAppendable) SetSourceKeyGenerator(gen SourceKeyGenerator) {
	ap.gen = gen
}

// func (ap *BaseAppendable) SetValueFetcher(fet ValueFetcher) {
// 	ap.fet = fet
// }

// func (ap *BaseAppendable) SetValuePutter(put ValuePutter) {
// 	ap.put = put
// }

func (ap *BaseAppendable) SetCanFreeze(canFreeze CanFreeze) {
	ap.canFreeze = canFreeze
}

type AppendableFiles struct {
	valuesDecomp *seg.Decompressor
	indexes      []*recsplit.Index
}
