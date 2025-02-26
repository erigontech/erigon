package state

import (
	"context"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"

	btree2 "github.com/tidwall/btree"
)

// ProtoEntity with basic functionality it's not intended to be used directly.
// Can be embedded in other marker/relational/appendable entities
type ProtoEntity struct {
	freezer Freezer

	a          ae.EntityId
	builders   []AccessorIndexBuilder
	dirtyFiles *btree2.BTreeG[*filesItem]
	_visible   visibleFiles

	sameKeyAsRoot bool
	strategy      CanonicityStrategy

	logger log.Logger
}

func NewProto(a ae.EntityId, builders []AccessorIndexBuilder, freezer Freezer, logger log.Logger) *ProtoEntity {
	return &ProtoEntity{
		a:          a,
		builders:   builders,
		freezer:    freezer,
		dirtyFiles: btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		logger:     logger,
	}
}

func (a *ProtoEntity) VisibleFilesMaxRootNum() ae.RootNum {
	lasti := len(a._visible) - 1
	if lasti < 0 {
		return 0
	}

	return RootNum(a._visible[lasti].src.endTxNum)
}

func (a *ProtoEntity) DirtyFilesMaxRootNum() ae.RootNum {
	latest, found := a.dirtyFiles.Max()
	if latest == nil || !found {
		return 0
	}
	return ae.RootNum(latest.endTxNum)
}

func (a *ProtoEntity) VisibleFilesMaxNum() Num {
	//latest := a._visible[len(a._visible)-1]
	// need to store first entity num in snapshots for this
	// TODO: just sending max root num now; so it won't work if rootnum!=num;
	// maybe we should just test bodies and headers till then.

	return Num(a.VisibleFilesMaxRootNum())
}

func (a *ProtoEntity) BuildFiles(ctx context.Context, from, to RootNum, db kv.RoDB, ps *background.ProgressSet) (filesBuilt uint, err error) {
	log.Debug("freezing %s from %d to %d", a.a.Name(), from, to)
	calcFrom, calcTo := from, to
	var canFreeze bool
	cfg := a.a.SnapshotConfig()
	for {
		calcFrom, calcTo, canFreeze = ae.GetFreezingRange(calcFrom, calcTo, a.a)
		if !canFreeze {
			break
		}

		log.Debug("freezing %s from %d to %d", a.a.Name(), calcFrom, calcTo)
		path := ae.SegName(a.a, snaptype.Version(1), calcFrom, calcTo)
		sn, err := seg.NewCompressor(ctx, "Snapshot "+a.a.Name(), path, a.a.Dirs().Tmp, seg.DefaultCfg, log.LvlTrace, a.logger)
		if err != nil {
			return filesBuilt, err
		}
		defer sn.Close()

		{
			a.freezer.SetCollector(func(values []byte) error {
				return sn.AddWord(values)
			})
			if err = a.freezer.Freeze(ctx, calcFrom, calcTo, db); err != nil {
				return filesBuilt, err
			}
		}

		{
			p := ps.AddNew(path, 1)
			defer ps.Delete(p)

			if err := sn.Compress(); err != nil {
				return filesBuilt, err
			}
			sn.Close()
			sn = nil
			ps.Delete(p)

		}

		valuesDecomp, err := seg.NewDecompressor(path)
		if err != nil {
			return filesBuilt, err
		}

		df := newFilesItemWithFrozenSteps(uint64(calcFrom), uint64(calcTo), cfg.MinimumSize, cfg.StepsInFrozenFile())
		df.decompressor = valuesDecomp

		indexes := make([]*recsplit.Index, len(a.builders))
		for i, ib := range a.builders {
			p := &background.Progress{}
			ps.Add(p)
			recsplitIdx, err := ib.Build(ctx, calcFrom, calcTo, p)
			if err != nil {
				return filesBuilt, err
			}

			indexes[i] = recsplitIdx
		}
		// TODO: add support for multiple indexes in filesItem.
		df.index = indexes[0]

		calcFrom = calcTo
		calcTo = to
		sn.Close()
		filesBuilt++
	}

	return filesBuilt, nil
}

// proto_appendable_rotx

type ProtoEntityTx struct {
	id    EntityId
	files visibleFiles
	a     *ProtoEntity
}

func (a *ProtoEntity) BeginFilesRo() *ProtoEntityTx {
	for i := range a._visible {
		if a._visible[i].src.frozen {
			a._visible[i].src.refcount.Add(1)
		}
	}

	return &ProtoEntityTx{
		id:    a.a,
		files: a._visible,
		a:     a,
	}
}

func (a *ProtoEntityTx) Close() {
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
			src.closeFilesAndRemove()
		}
	}
}

func (a *ProtoEntityTx) Type() CanonicityStrategy {
	return a.a.strategy
}

func (a *ProtoEntityTx) Garbage(merged *filesItem) (outs []*filesItem) {
	if merged == nil {
		return
	}

	a.a.dirtyFiles.Walk(func(item []*filesItem) bool {
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
