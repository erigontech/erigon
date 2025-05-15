package state

import (
	"context"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	ee "github.com/erigontech/erigon-lib/state/entity_extras"
	"github.com/erigontech/erigon-lib/version"
)

/*
ProtoForkable with basic functionality it's not intended to be used directly.
Can be embedded in other marker/relational/appending entities.
*/
type ProtoForkable struct {
	freezer Freezer

	a        ee.ForkableId
	cfg      *ee.SnapshotConfig
	parser   ee.SnapNameSchema
	builders []AccessorIndexBuilder
	snaps    *SnapshotRepo

	strategy  CanonicityStrategy
	unaligned bool

	logger log.Logger
}

func NewProto(a ee.ForkableId, builders []AccessorIndexBuilder, freezer Freezer, logger log.Logger) *ProtoForkable {
	return &ProtoForkable{
		a:        a,
		cfg:      a.SnapshotConfig(),
		parser:   a.SnapshotConfig().Schema,
		builders: builders,
		freezer:  freezer,
		snaps:    NewSnapshotRepoForForkable(a, logger),
		logger:   logger,
	}
}

func (a *ProtoForkable) RecalcVisibleFiles(toRootNum RootNum) {
	a.snaps.RecalcVisibleFiles(toRootNum)
}

func (a *ProtoForkable) IntegrateDirtyFile(file *filesItem) {
	a.snaps.IntegrateDirtyFile(file)
}

func (a *ProtoForkable) IntegrateDirtyFiles(files []*filesItem) {
	a.snaps.IntegrateDirtyFiles(files)
}

func (a *ProtoForkable) IntegrateDirtyFiles2(files []FilesItem) {
	cfiles := make([]*filesItem, len(files))
	for i := range files {
		cfiles[i] = files[i].(*filesItem)
	}
	a.snaps.IntegrateDirtyFiles(cfiles)
}

// BuildFile builds a single file for the given range, respecting the snapshot config.
//  1. typically this would be used to built a single step or "minimum sized snapshot", but can
//     be used to build bigger files too.
//  2. The caller is responsible for ensuring that data is available in db to freeze.
func (a *ProtoForkable) BuildFile(ctx context.Context, from, to RootNum, db kv.RoDB, ps *background.ProgressSet) (builtFile *filesItem, built bool, err error) {
	log.Debug("freezing %s from %d to %d", a.a.Name(), from, to)
	calcFrom, calcTo := from, to
	var canFreeze bool
	cfg := a.a.SnapshotConfig()
	calcFrom, calcTo, canFreeze = a.snaps.GetFreezingRange(calcFrom, calcTo)
	if !canFreeze {
		return nil, false, nil
	}

	log.Debug("freezing %s from %d to %d", a.a.Name(), calcFrom, calcTo)
	path := a.parser.DataFile(version.V1_0, calcFrom, calcTo)
	sn, err := seg.NewCompressor(ctx, "Snapshot "+a.a.Name(), path, a.a.Dirs().Tmp, seg.DefaultCfg, log.LvlTrace, a.logger)
	if err != nil {
		return nil, false, err
	}
	defer sn.Close()

	{
		if err = a.freezer.Freeze(ctx, calcFrom, calcTo, func(values []byte) error {
			// TODO: look at block_Snapshots.go#dumpRange
			// when snapshot is non-frozen range, it AddsUncompressedword (fast creation)
			// else AddWord.
			// BuildFiles perhaps only used for fast builds...and merge is for slow builds.
			// so using uncompressed here
			return sn.AddUncompressedWord(values)
		}, db); err != nil {
			return nil, false, err
		}
	}

	{
		p := ps.AddNew(path, 1)
		defer ps.Delete(p)

		if err := sn.Compress(); err != nil {
			return nil, false, err
		}
		sn.Close()
		sn = nil
		ps.Delete(p)

	}

	valuesDecomp, err := seg.NewDecompressor(path)
	if err != nil {
		return nil, false, err
	}

	df := newFilesItemWithSnapConfig(uint64(calcFrom), uint64(calcTo), cfg)
	df.decompressor = valuesDecomp

	//indexes := make([]*recsplit.Index, len(a.builders))
	for _, ib := range a.builders {
		p := &background.Progress{}
		ps.Add(p)
		recsplitIdx, err := ib.Build(ctx, calcFrom, calcTo, p)
		if err != nil {
			return nil, false, err
		}

		//indexes[i] = recsplitIdx
		// TODO: add support for multiple indexes in filesItem.

		df.index = recsplitIdx
	}
	return df, true, nil
}

func (a *ProtoForkable) Repo() *SnapshotRepo {
	return a.snaps
}

func (a *ProtoForkable) Close() {
	a.snaps.Close()
}

// proto_forkable_rotx

type ProtoForkableTx struct {
	id      ForkableId
	files   visibleFiles
	a       *ProtoForkable
	noFiles bool

	readers []*recsplit.IndexReader
}

func (a *ProtoForkable) BeginFilesRo() *ProtoForkableTx {
	visibleFiles := a.snaps.visibleFiles()
	for i := range visibleFiles {
		src := visibleFiles[i].src
		if !src.frozen {
			src.refcount.Add(1)
		}
	}

	return &ProtoForkableTx{
		id:    a.a,
		files: visibleFiles,
		a:     a,
	}
}

func (a *ProtoForkable) BeginNoFilesRo() *ProtoForkableTx {
	return &ProtoForkableTx{
		id:      a.a,
		files:   nil,
		a:       a,
		noFiles: true,
	}
}

func (a *ProtoForkableTx) Close() {
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

func (a *ProtoForkableTx) StatelessIdxReader(i int) *recsplit.IndexReader {
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

func (a *ProtoForkableTx) Type() CanonicityStrategy {
	return a.a.strategy
}

func (a *ProtoForkableTx) Garbage(merged *filesItem) (outs []*filesItem) {
	return a.a.snaps.Garbage(a.files, merged)
}

func (a *ProtoForkableTx) VisibleFilesMaxRootNum() RootNum {
	a.NoFilesCheck()
	return RootNum(a.files.EndTxNum())
}

func (a *ProtoForkableTx) VisibleFilesMaxNum() Num {
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
func (a *ProtoForkableTx) GetFromFiles(entityNum Num) (b Bytes, found bool, fileIdx int, err error) {
	a.NoFilesCheck()
	ap := a.a
	lastNum := a.VisibleFilesMaxNum()
	if entityNum < lastNum && ap.builders[0].AllowsOrdinalLookupByNum() {
		index := sort.Search(len(a.files), func(i int) bool {
			idx := a.files[i].src.index
			return idx.BaseDataID()+idx.KeyCount() > uint64(entityNum)
		})
		if index == len(a.files) {
			return nil, false, -1, fmt.Errorf("entity get error: snapshot expected but not found: (%s, %d)", ap.a.Name(), entityNum)
		}

		v, f, err := a.GetFromFile(entityNum, index)
		return v, f, index, err
	}

	return nil, false, -1, nil
}

func (a *ProtoForkableTx) Files() []FilesItem {
	a.NoFilesCheck()
	v := a.files
	fi := make([]FilesItem, len(v))
	for i, f := range v {
		fi[i] = f.src
	}
	return fi
}

func (a *ProtoForkableTx) GetFromFile(entityNum Num, idx int) (v Bytes, found bool, err error) {
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
		word, _ = g.NextUncompressed()
		//word, _ = g.Next(word[:0])
		return word, true, nil
	}
	ap := a.a
	return nil, false, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", ap.a.Name(), entityNum, a.files[idx].src.decompressor.FileName())
}

func (a *ProtoForkableTx) NoFilesCheck() {
	if a.noFiles {
		panic("snapshot read attempt on noFiles mode")
	}
}
