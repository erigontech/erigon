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

type ProtoAppendable struct {
	freezer Freezer

	enum          ApEnum
	indexBuilders []AccessorIndexBuilder

	dirtyFiles *btree.BTreeG[*DirtySegment]
	_visible   []*VisibleSegment

	baseAppendable Appendable

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
	return TsNum(latest.GetLastTsNum())
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
			Range:        Range{uint64(from), uint64(to)},
			Decompressor: valuesDecomp,
			filePath:     path,
			enum:         a.enum,
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
