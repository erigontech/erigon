package state

import (
	"context"
	"fmt"
	"sort"

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

// func (a *ProtoEntity) DirtyFilesMaxRootNum() ae.RootNum {
// 	latest, found := a.dirtyFiles.Max()
// 	if latest == nil || !found {
// 		return 0
// 	}
// 	return ae.RootNum(latest.endTxNum)
// }

func (a *ProtoEntity) RecalcVisibleFiles(toRootNum RootNum) {
	a._visible = calcVisibleFiles(a.dirtyFiles, AccessorHashMap, false, uint64(toRootNum))
}

func (a *ProtoEntity) IntegrateDirtyFiles(files []*filesItem) {
	for _, item := range files {
		a.dirtyFiles.Set(item)
	}
}

func (a *ProtoEntity) BuildFiles(ctx context.Context, from, to RootNum, db kv.RoDB, ps *background.ProgressSet) (dirtyFiles []*filesItem, err error) {
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
		path := ae.SnapFilePath(a.a, snaptype.Version(1), calcFrom, calcTo)
		sn, err := seg.NewCompressor(ctx, "Snapshot "+a.a.Name(), path, a.a.Dirs().Tmp, seg.DefaultCfg, log.LvlTrace, a.logger)
		if err != nil {
			return dirtyFiles, err
		}
		defer sn.Close()

		{
			a.freezer.SetCollector(func(values []byte) error {
				// TODO: look at block_Snapshots.go#dumpRange
				// when snapshot is non-frozen range, it AddsUncompressedword (fast creation)
				// else AddWord.
				return sn.AddUncompressedWord(values)
			})
			if err = a.freezer.Freeze(ctx, calcFrom, calcTo, db); err != nil {
				return dirtyFiles, err
			}
		}

		{
			p := ps.AddNew(path, 1)
			defer ps.Delete(p)

			if err := sn.Compress(); err != nil {
				return dirtyFiles, err
			}
			sn.Close()
			sn = nil
			ps.Delete(p)

		}

		valuesDecomp, err := seg.NewDecompressor(path)
		if err != nil {
			return dirtyFiles, err
		}

		df := newFilesItemWithFrozenSteps(uint64(calcFrom), uint64(calcTo), cfg.MinimumSize, cfg.StepsInFrozenFile())
		df.decompressor = valuesDecomp

		indexes := make([]*recsplit.Index, len(a.builders))
		for i, ib := range a.builders {
			p := &background.Progress{}
			ps.Add(p)
			recsplitIdx, err := ib.Build(ctx, calcFrom, calcTo, p)
			if err != nil {
				return dirtyFiles, err
			}

			indexes[i] = recsplitIdx
		}
		// TODO: add support for multiple indexes in filesItem.
		df.index = indexes[0]
		dirtyFiles = append(dirtyFiles, df)

		calcFrom = calcTo
		calcTo = to
	}

	return dirtyFiles, nil
}

// proto_appendable_rotx

type ProtoEntityTx struct {
	id    EntityId
	files visibleFiles
	a     *ProtoEntity

	readers []*recsplit.IndexReader
	getters []*seg.Reader
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

func (a *ProtoEntityTx) StatelessGetter(i int) *seg.Reader {
	if a.getters == nil {
		a.getters = make([]*seg.Reader, len(a.files))
	}

	r := a.getters[i]
	if r == nil {
		g := a.files[i].src.decompressor.MakeGetter()
		r = seg.NewReader(g, seg.CompressVals) // TODO: add compression support
		a.getters[i] = r
	}

	return r
}

func (a *ProtoEntityTx) StatelessIdxReader(i int) *recsplit.IndexReader {
	if a.readers == nil {
		a.readers = make([]*recsplit.IndexReader, len(a.files))
	}

	r := a.readers[i]
	if r == nil {
		r = a.files[i].src.index.GetReaderFromPool()
		a.readers[i] = r
	}

	return r
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

func (a *ProtoEntityTx) VisibleFilesMaxRootNum() RootNum {
	lasti := len(a.files) - 1
	if lasti < 0 {
		return 0
	}

	return RootNum(a.files[lasti].src.endTxNum)
}

func (a *ProtoEntityTx) VisibleFilesMaxNum() Num {
	// need to store first entity num in snapshots for this
	// TODO: just sending max root num now; so it won't work if rootnum!=num;
	// maybe we should just test bodies and headers till then.

	return Num(a.VisibleFilesMaxRootNum())
}

func (a *ProtoEntityTx) LookupFile(entityNum Num, tx kv.Tx) (b Bytes, found bool, err error) {
	ap := a.a
	lastNum := a.VisibleFilesMaxNum()
	if entityNum <= lastNum && ap.builders[0].AllowsOrdinalLookupByNum() {
		var word []byte
		index := sort.Search(len(ap._visible), func(i int) bool {
			return ap._visible[i].src.LastEntityNum() > uint64(entityNum)
		})

		if index == -1 {
			return nil, false, fmt.Errorf("entity get error: snapshot expected but now found: (%s, %d)", ap.a.Name(), entityNum)
		}
		offset := a.StatelessIdxReader(index).OrdinalLookup(uint64(entityNum))
		g := a.StatelessGetter(index)
		g.Reset(offset)
		if g.HasNext() {
			word, _ = g.Next(word[:0])
			return word, true, nil
		}
		return nil, false, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", ap.a.Name(), entityNum, ap._visible[index].src.decompressor.FileName1)
	}

	return nil, false, nil

}
