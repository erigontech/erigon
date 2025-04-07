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
	ae "github.com/erigontech/erigon-lib/state/appendable_extras"

	btree2 "github.com/tidwall/btree"
)

/*
ProtoAppendable with basic functionality it's not intended to be used directly.
Can be embedded in other marker/relational/appendable entities.
*/
type ProtoAppendable struct {
	freezer Freezer

	a          ae.AppendableId
	builders   []AccessorIndexBuilder
	dirtyFiles *btree2.BTreeG[*filesItem]
	_visible   visibleFiles

	strategy CanonicityStrategy

	logger log.Logger
}

func NewProto(a ae.AppendableId, builders []AccessorIndexBuilder, freezer Freezer, logger log.Logger) *ProtoAppendable {
	return &ProtoAppendable{
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

func (a *ProtoAppendable) RecalcVisibleFiles(toRootNum RootNum) {
	a._visible = calcVisibleFiles(a.dirtyFiles, AccessorHashMap, false, uint64(toRootNum))
}

func (a *ProtoAppendable) IntegrateDirtyFiles(files []*filesItem) {
	for _, item := range files {
		a.dirtyFiles.Set(item)
	}
}

func (a *ProtoAppendable) BuildFiles(ctx context.Context, from, to RootNum, db kv.RoDB, ps *background.ProgressSet) (dirtyFiles []*filesItem, err error) {
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
			if err = a.freezer.Freeze(ctx, calcFrom, calcTo, func(values []byte) error {
				// TODO: look at block_Snapshots.go#dumpRange
				// when snapshot is non-frozen range, it AddsUncompressedword (fast creation)
				// else AddWord.
				// But BuildFiles perhaps only used for fast builds...and merge is for slow builds.
				return sn.AddUncompressedWord(values)
			}, db); err != nil {
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

func (a *ProtoAppendable) Close() {
	var toClose []*filesItem
	a.dirtyFiles.Walk(func(items []*filesItem) bool {
		toClose = append(toClose, items...)
		return true
	})
	for _, item := range toClose {
		item.closeFiles()
		a.dirtyFiles.Delete(item)
	}
}

// proto_appendable_rotx

type ProtoAppendableTx struct {
	id      AppendableId
	files   visibleFiles
	a       *ProtoAppendable
	noFiles bool

	readers []*recsplit.IndexReader
}

func (a *ProtoAppendable) BeginFilesRo() *ProtoAppendableTx {
	for i := range a._visible {
		if a._visible[i].src.frozen {
			a._visible[i].src.refcount.Add(1)
		}
	}

	return &ProtoAppendableTx{
		id:    a.a,
		files: a._visible,
		a:     a,
	}
}

func (a *ProtoAppendable) BeginNoFilesRo() *ProtoAppendableTx {
	return &ProtoAppendableTx{
		id:      a.a,
		files:   nil,
		a:       a,
		noFiles: true,
	}
}

func (a *ProtoAppendableTx) Close() {
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

	for i := range a.readers {
		a.readers[i].Close()
	}
	a.readers = nil
}

func (a *ProtoAppendableTx) StatelessIdxReader(i int) *recsplit.IndexReader {
	a.NoFilesCheck()
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

func (a *ProtoAppendableTx) Type() CanonicityStrategy {
	return a.a.strategy
}

func (a *ProtoAppendableTx) Garbage(merged *filesItem) (outs []*filesItem) {
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

func (a *ProtoAppendableTx) VisibleFilesMaxRootNum() RootNum {
	a.NoFilesCheck()
	lasti := len(a.files) - 1
	if lasti < 0 {
		return 0
	}

	return RootNum(a.files[lasti].src.endTxNum)
}

func (a *ProtoAppendableTx) VisibleFilesMaxNum() Num {
	a.NoFilesCheck()
	lasti := len(a.files) - 1
	if lasti < 0 {
		return 0
	}
	idx := a.files[lasti].src.index
	return Num(idx.BaseDataID() + idx.KeyCount())
}

// if either found=false or err != nil, then fileIdx = -1
// can get FileItem on which entityNum was found by a.Files()[fileIdx] etc.
func (a *ProtoAppendableTx) GetFromFiles(entityNum Num) (b Bytes, found bool, fileIdx int, err error) {
	a.NoFilesCheck()
	ap := a.a
	lastNum := a.VisibleFilesMaxNum()
	if entityNum < lastNum && ap.builders[0].AllowsOrdinalLookupByNum() {
		index := sort.Search(len(ap._visible), func(i int) bool {
			idx := ap._visible[i].src.index
			return idx.BaseDataID()+idx.KeyCount() > uint64(entityNum)
		})
		if index == -1 {
			return nil, false, -1, fmt.Errorf("entity get error: snapshot expected but not found: (%s, %d)", ap.a.Name(), entityNum)
		}

		v, f, err := a.GetFromFile(entityNum, index)
		return v, f, index, err
	}

	return nil, false, -1, nil
}

func (a *ProtoAppendableTx) Files() []FilesItem {
	a.NoFilesCheck()
	v := a.a._visible
	fi := make([]FilesItem, len(v))
	for i, f := range v {
		fi[i] = f.src
	}
	return fi
}

func (a *ProtoAppendableTx) GetFromFile(entityNum Num, idx int) (v Bytes, found bool, err error) {
	a.NoFilesCheck()
	if idx >= len(a.files) {
		return nil, false, fmt.Errorf("index out of range: %d >= %d", idx, len(a.files))
	}

	indexR := a.StatelessIdxReader(idx)
	id := int64(entityNum) - int64(indexR.BaseDataID())
	if id < 0 {
		a.a.logger.Error("ordinal lookup by negative num", "entityNum", entityNum, "index", idx, "indexR.BaseDataID()", indexR.BaseDataID())
		panic("ordinal lookup by negative num")
	}
	offset := indexR.OrdinalLookup(uint64(id))
	g := a.files[idx].src.decompressor.MakeGetter()
	g.Reset(offset)
	var word []byte
	if g.HasNext() {
		word, _ = g.Next(word[:0])
		return word, true, nil
	}
	ap := a.a
	return nil, false, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", ap.a.Name(), entityNum, ap._visible[idx].src.decompressor.FileName1)
}

func (a *ProtoAppendableTx) NoFilesCheck() {
	if a.noFiles {
		panic("snapshot read attempt on noFiles mode")
	}
}
