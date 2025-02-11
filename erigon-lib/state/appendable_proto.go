package state

import (
	"context"
	"sync"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	ae "github.com/erigontech/erigon-lib/state/appendables_extras"

	"github.com/tidwall/btree"
	btree2 "github.com/tidwall/btree"
)

// appendable struct with basic functionality it's not intended to be used directly.
// Can be embedded in other concrete appendable structs
type ProtoAppendable struct {
	freezer Freezer

	a          ae.AppendableId
	builders   []AccessorIndexBuilder
	dirtyFiles *btree.BTreeG[*filesItem]
	_visible   visibleFiles

	visibleLock   sync.RWMutex
	sameKeyAsRoot bool

	logger log.Logger
}

func NewProto(a ae.AppendableId, builders []AccessorIndexBuilder, freezer Freezer, logger log.Logger) *ProtoAppendable {
	return &ProtoAppendable{
		a:          a,
		builders:   builders,
		freezer:    freezer,
		dirtyFiles: btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		logger:     logger,
	}
}

func (a *ProtoAppendable) VisibleFilesMaxRootNum() ae.RootNum {
	latest := a._visible[len(a._visible)-1]
	return ae.RootNum(latest.src.endTxNum)
}

func (a *ProtoAppendable) DirtyFilesMaxRootNum() ae.RootNum {
	latest, found := a.dirtyFiles.Max()
	if latest == nil || !found {
		return 0
	}
	return ae.RootNum(latest.endTxNum)
}

func (a *ProtoAppendable) VisibleFilesMaxNum() RootNum {
	//latest := a._visible[len(a._visible)-1]
	// need to store first entity num in snapshots for this
	// TODO: just sending max root num now; so it won't work if rootnum!=num;
	// maybe we should just test bodies and headers till then.

	return a.VisibleFilesMaxRootNum()
}

func (a *ProtoAppendable) BuildFiles(ctx context.Context, from, to RootNum, db kv.RoDB, ps *background.ProgressSet) error {
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
			return err
		}

		{
			a.freezer.SetCollector(func(values []byte) error {
				return sn.AddWord(values)
			})
			tx, err := db.BeginRo(ctx)
			if err != nil {
				return err
			}

			defer tx.Rollback()
			if err = a.freezer.Freeze(ctx, calcFrom, calcTo, tx); err != nil {
				return err
			}
			tx.Rollback()
		}

		{
			p := ps.AddNew(path, 1)
			defer ps.Delete(p)
			if err := sn.Compress(); err != nil {
				return err
			}
			sn.Close()
			sn = nil
			ps.Delete(p)
		}

		valuesDecomp, err := seg.NewDecompressor(path)
		if err != nil {
			return err
		}

		df := newFilesItemWithFrozenSteps(uint64(calcFrom), uint64(calcTo), cfg.MinimumSize, cfg.StepsInFrozenFile())
		df.decompressor = valuesDecomp

		indexes := make([]*recsplit.Index, len(a.builders))
		for i, ib := range a.builders {
			recsplitIdx, err := ib.Build(ctx, from, to, "sometmpdir", ps, log.LvlInfo, nil)
			if err != nil {
				return err
			}

			indexes[i] = recsplitIdx
		}
		// TODO: add support for multiple indexes in filesItem.
		df.index = indexes[0]

		calcFrom = calcTo
		calcTo = to
	}

	return nil
}
