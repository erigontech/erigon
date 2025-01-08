package appendables

import (
	"context"
	"encoding/binary"

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
	// this is needed in unwind, for which the input is stepKey
	// this can also be used by Freezer
	FromStepKey(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) stream.Uno[VKType]
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
	doesUnwind             bool
	stepSize               uint64
	stepKeySameAsTsNum     bool
	hasCustomIndexBuilders bool // use
}

func NewBaseAppendable(enum ApEnum, valsTbl string) *BaseAppendable {
	ap := &BaseAppendable{
		enum:    enum,
		valsTbl: valsTbl,
	}
	return ap
}

// setters

func (ap *BaseAppendable) getLastTsNumInSnapshot() uint64 {
	last := ap._visible[len(ap._visible)-1].indexes[0]
	return last.BaseDataID() + last.KeyCount() - 1
}

func (ap *BaseAppendable) Get(tsNum uint64, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	lastTsNum := ap.getLastTsNumInSnapshot()
	if tsNum <= lastTsNum {
		if ap.stepKeySameAsTsNum {
			// can do binary search or loop over visible segments and find which segment contains tsNum
			// and then get from there
			var v *VisibleSegment
			return v.Get(tsNum)
		} else {
			// loop over all visible segments and find which segment contains tsNum
		}
	}

	// then db
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, tsNum)
	return tx.GetOne(ap.valsTbl, key)
}

func (ap *BaseAppendable) Put(tsNum uint64, value VVType, tx kv.RwTx) error {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, tsNum)
	return tx.Append(ap.valsTbl, key, value)
}

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

func (ap *BaseAppendable) getBaseDataId(stepKeyFrom uint64, tx kv.Tx) (VKType, error) {
	// can_stream := ap.gen.FromStepKey(stepKeyFrom, stepKeyFrom+1, tx)
	// if !can_stream.HasNext() {
	// 	return 0, nil
	// }
	// key, err := can_stream.Next()
	// if err != nil {
	// 	return 0, err
	// }
	// return key, nil

	// the above doesn't work because:
	// 1. freezing might skip certain keys, so first key in stream might not be the first key in the snapshot
	// 2. gen.FromStepKey() might rely on db, which might be pruned.

	// Discuss with Alex:
	// the best option is to have baseDataId stored in the snapshot (change in snapshot format); and be retrievable from there.
	// this is fine. Because the freezer knows the first key to go in snapshot (see BaseFreezer.Freeze()), and so
	// can report that.
	// snapshot store firstKey as []byte (can't be uint64 generally, because snapshots like blocks have key as blockNum+hash, so 
	// it can't be uint64 generally in compressor/decompressor/getter interfaces. So byte is fine.).
	return 0, nil
}

func (ap *BaseAppendable) BuildMissedIndexes(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	// use indexbuilders
	// caller must "refresh" like OpenFolder to refresh the dirty files
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

func (ap *BaseAppendable) SetCanFreeze(canFreeze CanFreeze) {
	ap.canFreeze = canFreeze
}

type AppendableFiles struct {
	valuesDecomp *seg.Decompressor
	indexes      []*recsplit.Index
}
