package state

import (
	"context"
	"fmt"
	"path"
	"sort"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
)

/*
ProtoForkable with basic functionality it's not intended to be used directly.
Can be embedded in other marker/relational/appending entities.
*/
type ProtoForkable struct {
	freezer Freezer

	a        ForkableId
	cfg      *SnapshotConfig
	parser   SnapNameSchema
	builders []AccessorIndexBuilder
	snaps    *SnapshotRepo

	strategy  kv.CanonicityStrategy
	unaligned bool

	logger log.Logger
}

func NewProto(a ForkableId, builders []AccessorIndexBuilder, freezer Freezer, logger log.Logger) *ProtoForkable {
	return &ProtoForkable{
		a:        a,
		cfg:      Registry.SnapshotConfig(a),
		parser:   Registry.SnapshotConfig(a).Schema,
		builders: builders,
		freezer:  freezer,
		snaps:    NewSnapshotRepoForForkable(a, logger),
		logger:   logger,
	}
}

func (a *ProtoForkable) RecalcVisibleFiles(toRootNum RootNum) {
	a.snaps.RecalcVisibleFiles(toRootNum)
}

func (a *ProtoForkable) IntegrateDirtyFile(file *FilesItem) {
	a.snaps.IntegrateDirtyFile(file)
}

func (a *ProtoForkable) IntegrateDirtyFiles(files []*FilesItem) {
	a.snaps.IntegrateDirtyFiles(files)
}

// func (a *ProtoForkable) IntegrateDirtyFiles2(files []FilesItem) {
// 	cfiles := make([]*FilesItem, len(files))
// 	for i := range files {
// 		cfiles[i] = files[i].(*filesItem)
// 	}
// 	a.snaps.IntegrateDirtyFiles(cfiles)
// }

// BuildFile builds a single file for the given range, respecting the snapshot config.
//  1. typically this would be used to built a single step or "minimum sized snapshot", but can
//     be used to build bigger files too.
//  2. The caller is responsible for ensuring that data is available in db to freeze.
func (a *ProtoForkable) BuildFile(ctx context.Context, from, to RootNum, db kv.RoDB, compressionWorkers int, ps *background.ProgressSet) (builtFile *FilesItem, built bool, err error) {
	log.Debug("freezing %s from %d to %d", Registry.Name(a.a), from, to)
	calcFrom, calcTo := from, to
	var canFreeze bool
	cfg := Registry.SnapshotConfig(a.a)
	calcFrom, calcTo, canFreeze = a.snaps.GetFreezingRange(calcFrom, calcTo)
	if !canFreeze {
		return nil, false, nil
	}

	log.Debug("freezing %s from %d to %d", Registry.Name(a.a), calcFrom, calcTo)
	path := a.parser.DataFile(version.V1_0, calcFrom, calcTo)
	segCfg := seg.DefaultCfg
	segCfg.Workers = compressionWorkers
	sn, err := seg.NewCompressor(ctx, "Snapshot "+Registry.Name(a.a), path, Registry.Dirs(a.a).Tmp, segCfg, log.LvlTrace, a.logger)
	if err != nil {
		return nil, false, err
	}
	defer sn.Close()
	// TODO: fsync params?

	{
		compress := a.isCompressionUsed(calcFrom, calcTo)
		var addWordFn func(values []byte) error
		if compress {
			// https://github.com/erigontech/erigon/pull/11222 -- decision taken here
			// is that 1k and 10k files will be uncompressed, but 10k files also merged
			// and not built off db, so following decision is okay:
			// AddWord -- compressed -- slowbuilds (merge etc.)
			// AddUncompressedWord -- uncompressed -- fast builds
			addWordFn = sn.AddWord
		} else {
			addWordFn = sn.AddUncompressedWord
		}
		if err = a.freezer.Freeze(ctx, calcFrom, calcTo, addWordFn, db); err != nil {
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
		ps.Delete(p)

	}

	valuesDecomp, err := seg.NewDecompressor(path)
	if err != nil {
		return nil, false, err
	}

	df := newFilesItemWithSnapConfig(uint64(calcFrom), uint64(calcTo), cfg)
	df.decompressor = valuesDecomp

	indexes, err := a.BuildIndexes(ctx, calcFrom, calcTo, ps)
	if err != nil {
		return nil, false, err
	}

	// TODO: add support for multiple indexes in filesItem.
	df.index = indexes[0]
	return df, true, nil
}

func (a *ProtoForkable) BuildIndexes(ctx context.Context, from, to RootNum, ps *background.ProgressSet) (indexes []*recsplit.Index, err error) {
	closeFiles := true
	defer func() {
		if closeFiles {
			for _, index := range indexes {
				index.Close()
				_ = dir.RemoveFile(index.FilePath())
			}
		}
	}()
	for i, ib := range a.builders {
		filename := path.Base(a.snaps.schema.AccessorIdxFile(version.V1_0, from, to, uint64(i)))
		p := ps.AddNew("build_index_"+filename, 1)
		defer ps.Delete(p)
		recsplitIdx, err := ib.Build(ctx, from, to, p)
		if err != nil {
			return indexes, err
		}
		indexes = append(indexes, recsplitIdx)

		ps.Delete(p)
	}
	closeFiles = false
	return
}

func (a *ProtoForkable) isCompressionUsed(from, to RootNum) bool {
	return uint64(to-from) > a.cfg.MinimumSize
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

func (a *ProtoForkableTx) Type() kv.CanonicityStrategy {
	return a.a.strategy
}

func (a *ProtoForkableTx) Garbage(merged *FilesItem) (outs []*FilesItem) {
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
			return nil, false, -1, fmt.Errorf("entity get error: snapshot expected but not found: (%s, %d)", Registry.Name(ap.a), entityNum)
		}

		v, f, err := a.GetFromFile(entityNum, index)
		return v, f, index, err
	}

	return nil, false, -1, nil
}

func (a *ProtoForkableTx) VisibleFiles() VisibleFiles {
	a.NoFilesCheck()
	v := a.files
	fi := make([]VisibleFile, len(v))
	for i, f := range v {
		fi[i] = f
	}
	return fi
}

func (a *ProtoForkableTx) vfs() visibleFiles {
	a.NoFilesCheck()
	return a.files
}

func (a *ProtoForkableTx) GetFromFile(entityNum Num, idx int) (v Bytes, found bool, err error) {
	ap := a.a
	a.NoFilesCheck()
	if idx >= len(a.files) {
		return nil, false, fmt.Errorf("index out of range: %d >= %d", idx, len(a.files))
	}

	indexR := a.StatelessIdxReader(idx)
	id := int64(entityNum) - int64(indexR.BaseDataID())
	if id < 0 {
		ap.logger.Error("ordinal lookup by negative num", "entityNum", entityNum, "index", idx, "indexR.BaseDataID()", indexR.BaseDataID())
		panic("ordinal lookup by negative num")
	}
	offset := indexR.OrdinalLookup(uint64(id))
	file := a.files[idx].src

	g := file.decompressor.MakeGetter()
	g.Reset(offset)

	start, end := file.Range()
	compression := seg.CompressNone
	if a.a.isCompressionUsed(RootNum(start), RootNum(end)) {
		compression = seg.CompressKeys
	}
	reader := seg.NewReader(g, compression)
	reader.Reset(offset)
	var word []byte

	if reader.HasNext() {
		//start, end
		word, _ = reader.Next(word[:0])
		return word, true, nil
	}

	return nil, false, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", Registry.Name(ap.a), entityNum, a.files[idx].src.decompressor.FileName())
}

func (a *ProtoForkableTx) NoFilesCheck() {
	if a.noFiles {
		panic("snapshot read attempt on noFiles mode")
	}
}
