package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	"golang.org/x/sync/errgroup"

	"github.com/tidwall/btree"
)

// common members/functions for appendables impls. Embed this.
type ProtoAppendable struct {
	freezer Freezer

	enum          ApEnum
	indexBuilders []AccessorIndexBuilder

	dirtyFiles *btree.BTreeG[*DirtySegment]
	_visible   VisibleSegments

	baseAppendable Appendable

	// put these in config
	stepSize           uint64
	baseKeySameAsTsNum bool // if the tsNum of this appendable is the same as the tsNum of the base appendable

	dirs   datadir.Dirs
	logger log.Logger
}

func NewProtoAppendable(enum ApEnum, stepSize uint64) *ProtoAppendable {
	return &ProtoAppendable{
		enum:     enum,
		stepSize: stepSize,
	}
}

func (a *ProtoAppendable) SetFreezer(freezer Freezer) {
	a.freezer = freezer
}

func (a *ProtoAppendable) SetIndexBuilders(indexBuilders []AccessorIndexBuilder) {
	a.indexBuilders = indexBuilders
}

func (a *ProtoAppendable) BaseKeySameAsTsNum() {
	a.baseKeySameAsTsNum = true
}

func (a *ProtoAppendable) VisibleSegmentsMaxTsNum() TsNum {
	// if snapshots store the last tsNum
	latest := a._visible[len(a._visible)-1]
	return TsNum(latest.Src().GetLastTsNum())
}

func (a *ProtoAppendable) DirtySegmentsMaxTsNum() TsNum {
	// if snapshots store the last tsNum
	latest, ok := a.dirtyFiles.Max()
	if !ok {
		return 0
	}
	return TsNum(latest.GetLastTsNum())
}

func (a *ProtoAppendable) BuildFiles(ctx context.Context, baseTsNumFrom, baseTsNumTo TsNum, db kv.RoDB, ps *background.ProgressSet) error {
	stepFrom, stepTo := uint64(baseTsNumFrom)/a.stepSize, uint64(baseTsNumTo)/a.stepSize
	for step := stepFrom; step <= stepTo; step++ {
		from, to := TsNum(step*a.stepSize), TsNum((step+1)*a.stepSize)

		// can it freeze? just follow base appendable
		if to > a.baseAppendable.DirtySegmentsMaxTsNum() {
			break
		}
		// maybe also check if segment is already built

		path := AppeSegName(a.enum, 1, step, step+1)
		sn, err := seg.NewCompressor(ctx, "Snapshot "+string(a.enum), path, a.dirs.Tmp, seg.DefaultCfg, log.LvlTrace, a.logger)
		if err != nil {
			return err
		}
		// freeze
		a.freezer.SetCollector(func(values []byte) error {
			return sn.AddWord(values)
		})
		tx, err := db.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err = a.freezer.Freeze(ctx, from, to, tx); err != nil {
			return nil
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
			Range:                  Range{uint64(from), uint64(to)},
			Decompressor:           valuesDecomp,
			filePath:               path,
			enum:                   a.enum,
			expectedCountOfIndexes: len(a.indexBuilders),
		}

		indexes := make([]*recsplit.Index, len(a.indexBuilders))
		for _, ib := range a.indexBuilders {
			var progresset *background.Progress //TODO
			recsplitIdx, err := ib.Build(ctx, from, to, "sometmpdir", progresset, log.LvlInfo, nil)
			if err != nil {
				return err
			}

			indexes = append(dseg.indexes, recsplitIdx)
		}

		// should this use a lock?
		dseg.indexes = indexes
		a.dirtyFiles.Set(dseg)
	}

	return nil
}

func (a *ProtoAppendable) BuildMissedIndexes(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	// use indexbuilders
	// caller must "refresh" like OpenFolder to refresh the dirty files
}

func (a *ProtoAppendable) RecalcVisibleFiles(baseTsNumTo TsNum) {
	a._visible = calcVisibleFiles(a.dirtyFiles, baseTsNumTo)
}

func (a *ProtoAppendable) Close() {
	if a == nil {
		return
	}
	a.closeWhatNotInList([]string{})
}

func (a *ProtoAppendable) Prune(ctx context.Context, baseTsNumTo TsNum, limit uint64, rwTx kv.RwTx) error {
	return nil
}
func (a *ProtoAppendable) Unwind(ctx context.Context, baseTsNumFrom TsNum, rwTx kv.RwTx) error {
	return nil
}

func (a *ProtoAppendable) closeWhatNotInList(fNames []string) {
	protectFiles := make(map[string]struct{}, len(fNames))
	for _, f := range fNames {
		protectFiles[f] = struct{}{}
	}
	var toClose []*DirtySegment
	a.dirtyFiles.Walk(func(items []*DirtySegment) bool {
		for _, item := range items {
			if item.Decompressor != nil {
				if _, ok := protectFiles[item.Decompressor.FileName()]; ok {
					continue
				}
			}
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		item.closeFiles()
		a.dirtyFiles.Delete(item)
	}
}

func (a *ProtoAppendable) IsBaseAppendable() bool {
	return a == a.baseAppendable
}

/// appendableRoTx
// preserve certain visiblefiles (inc refcount)
// AppendableRoTx and AppendableRwTx

// temporalTx.GetAsOf(name, k, ts) --> aggTx.GetAsOf(name, tx, k, ts)
// -> domaintx.GetAsOf(tx, k, ts)
//
/*

// on forkchoice update, temporalrwtx created, and then passed used for
// everything, block stuff and state stuff.
// need to follow same pattern here

// temporaldb has multiple aggregators. Today there's a single "state aggregator";
// but also need a "block aggregator"
 temporalDb.BeginRwNosync() -> tempRwTx (creates rotx for all aggregators;)
 tempRwTx.AppAggTx(base_appendable_eum) -> appendable_AggTx
 appendable_aggtx.Prune() etc.... : agg level ops
 appendable_aggtx.Get(app_enum) : appendable rotx level ops (this way because each get can return a different appendable rotx)

 queries can be PointQueries, RangedQueries, MarkedQueries
*/

type AppendableRoTx struct {
	files VisibleSegments
	a     Appendable
	name  ApEnum

	getters    []*seg.Reader
	idxReaders [][]*recsplit.IndexReader
}

func (a *AppendableRoTx) Unwind(ctx context.Context, baseTsNumFrom TsNum, rwTx kv.RwTx) error {
	return a.a.Unwind(ctx, baseTsNumFrom, rwTx)
}

///

func calcVisibleFiles(files *btree.BTreeG[*DirtySegment], toTsNum TsNum) VisibleSegments {
	newVisibleFiles := make([]VisibleSegment, 0, files.Len())
	iToTsNum := uint64(toTsNum)
	files.Walk(func(items []*DirtySegment) bool {
		for _, item := range items {
			if item.To() > iToTsNum {
				continue
			}
			if item.canDelete.Load() {
				continue
			}
			if item.Decompressor == nil {
				continue
			}
			if len(item.indexes) != item.expectedCountOfIndexes {
				continue
			}

			if len(newVisibleFiles) > 0 && newVisibleFiles[len(newVisibleFiles)-1].src.isSubsetOf(item) {
				newVisibleFiles[len(newVisibleFiles)-1].src = nil
				newVisibleFiles = newVisibleFiles[:len(newVisibleFiles)-1]
			}

			newVisibleFiles = append(newVisibleFiles, VisibleSegment{
				src: item,
			})
		}
		return true
	})
	if newVisibleFiles == nil {
		newVisibleFiles = []VisibleSegment{}
	}
	return newVisibleFiles
}

// proto_appendable_rotx

type ProtoAppendableRoTx struct {
	enum  ApEnum
	files VisibleSegments
	a     *ProtoAppendable
}

func (a *ProtoAppendable) BeginFilesRo() *ProtoAppendableRoTx {
	for i := 0; i < len(a._visible); i++ {
		if a._visible[i].src.frozen {
			a._visible[i].src.refcount.Add(1)
		}
	}

	return &ProtoAppendableRoTx{
		enum:  a.enum,
		files: a._visible,
		a:     a,
	}
}

func (a *ProtoAppendableRoTx) Close() {
	if a.files == nil {
		return
	}
	files := a.files
	a.files = nil
	for i := range files {
		src := files[i].src
		if src == nil || src.frozen {
			continue
		}
		refCnt := src.refcount.Add(-1)
		if refCnt == 0 && src.canDelete.Load() {
			src.CloseFilesAndRemove()
		}
	}
}

func (a *ProtoAppendableRoTx) Garbage(merged *DirtySegment) (outs []*DirtySegment) {
	if merged == nil {
		return
	}

	a.a.dirtyFiles.Walk(func(item []*DirtySegment) bool {
		for _, item := range item {
			if item.frozen {
				continue
			}
			if item.isSubsetOf(merged) {
				outs = append(outs, item)
			}
			if item.isBefore(merged) && hasCoverVisibleFile(a.files, item) {
				outs = append(outs, item)
			}
		}
		return true
	})
	return outs
}

func hasCoverVisibleFile(visibleFiles VisibleSegments, item *DirtySegment) bool {
	for _, f := range visibleFiles {
		if item.isSubsetOf(f.src) {
			return true
		}
	}
	return false
}

//func deleteMergeFile(dirtyFiles *btree2.BTreeG[*DirtySegment], outs []*DirtySegment)