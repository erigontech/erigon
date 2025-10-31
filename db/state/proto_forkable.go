package state

import (
	"context"
	"fmt"
	"path"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

/*
ProtoForkable with basic functionality it's not intended to be used directly.
Can be embedded in other marker/relational/appending entities.
*/
type ProtoForkable struct {
	freezer Freezer

	id       kv.ForkableId
	snapCfg  *SnapshotConfig
	fschema  SnapNameSchema
	cfg      *statecfg.ForkableCfg
	builders []AccessorIndexBuilder
	snaps    *SnapshotRepo
	metadata []NumMetadata

	unaligned bool

	logger log.Logger
	dirs   datadir.Dirs
}

func NewProto(id kv.ForkableId, builders []AccessorIndexBuilder, freezer Freezer, dirs datadir.Dirs, logger log.Logger) *ProtoForkable {
	return &ProtoForkable{
		id:       id,
		snapCfg:  Registry.SnapshotConfig(id),
		fschema:  Registry.SnapshotConfig(id).Schema,
		builders: builders,
		freezer:  freezer,
		snaps:    NewSnapshotRepoForForkable(id, logger),
		dirs:     dirs,
		logger:   logger,
	}
}

func (a *ProtoForkable) RecalcVisibleFiles(toRootNum RootNum) (maxRootNum RootNum) {
	maxRootNum = a.snaps.RecalcVisibleFiles(toRootNum)
	visibleCount := len(a.snaps.visibleFiles())
	// Ensure sufficient capacity - simpler approach
	if cap(a.metadata) < visibleCount {
		a.metadata = make([]NumMetadata, visibleCount)
	} else {
		a.metadata = a.metadata[:visibleCount]
	}
	for i, file := range a.snaps.visibleFiles() {
		m := NumMetadata{}
		if err := m.Unmarshal(file.src.decompressor.GetMetadata()); err != nil {
			panic(err)
		}
		a.metadata[i] = m
	}
	return maxRootNum
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
//     be used to build bigger files too (like in `stage_custom_trace`).
//  2. The caller is responsible for ensuring that data is available in db to freeze.
func (a *ProtoForkable) BuildFile(ctx context.Context, from, to RootNum, db kv.RoDB, compressionWorkers int, ps *background.ProgressSet) (builtFile *FilesItem, built bool, err error) {
	calcFrom, calcTo := from, to
	var canFreeze bool
	cfg := Registry.SnapshotConfig(a.id)
	calcFrom, calcTo, canFreeze = a.snaps.GetFreezingRange(calcFrom, calcTo)
	if !canFreeze {
		return nil, false, nil
	}

	log.Debug(fmt.Sprintf("freezing %s from step %d to %d", Registry.Name(a.id), calcFrom.Uint64()/a.StepSize(), calcTo.Uint64()/a.StepSize()))
	path := a.fschema.DataFile(version.V1_0, calcFrom, calcTo)

	var exists bool
	exists, err = dir.FileExist(path)
	if err != nil {
		return
	}

	if !exists {
		segCfg := seg.DefaultCfg
		segCfg.Workers = compressionWorkers
		segCfg.ExpectMetadata = true
		sn, err := seg.NewCompressor(ctx, "Snapshot "+Registry.Name(a.id), path, a.dirs.Tmp, segCfg, log.LvlTrace, a.logger)
		if err != nil {
			return nil, false, err
		}
		defer sn.Close()
		// TODO: fsync params?

		compress := a.isCompressionUsed(calcFrom, calcTo)
		writer := a.DataWriter(sn, compress)
		defer writer.Close()
		meta, err := a.freezer.Freeze(ctx, calcFrom, calcTo, writer, db)
		if err != nil {
			return nil, false, err
		}
		mbytes, err := meta.Marshal()
		if err != nil {
			return nil, false, err
		}
		writer.SetMetadata(mbytes)

		p := ps.AddNew(filepath.Base(path), 1)
		defer ps.Delete(p)
		if err := writer.Flush(); err != nil {
			return nil, false, err
		}
		if err := writer.Compress(); err != nil {
			return nil, false, err
		}
		writer.Close()
		sn.Close()
		ps.Delete(p)
	}

	valuesDecomp, err := seg.NewDecompressorWithMetadata(path, cfg.HasMetadata)
	if err != nil {
		return nil, false, err
	}

	df := newFilesItemWithSnapConfig(uint64(calcFrom), uint64(calcTo), cfg)
	df.decompressor = valuesDecomp

	indexes, err := a.BuildIndexes(ctx, df.decompressor, calcFrom, calcTo, ps)
	if err != nil {
		return nil, false, err
	}

	// TODO: add support for multiple indexes in filesItem.
	df.index = indexes[0]
	return df, true, nil
}

func (a *ProtoForkable) DataWriter(f *seg.Compressor, compress bool) *seg.PagedWriter {
	return seg.NewPagedWriter(seg.NewWriter(f, a.cfg.Compression), a.cfg.ValuesOnCompressedPage, compress)
}

func (a *ProtoForkable) DataReader(f *seg.Decompressor, compress bool) *seg.Reader {
	return seg.NewReader(f.MakeGetter(), a.cfg.Compression)
}

func (a *ProtoForkable) PagedDataReader(f *seg.Decompressor, compress bool) *seg.PagedReader {
	return seg.NewPagedReader(a.DataReader(f, compress), a.cfg.ValuesOnCompressedPage, compress)
}

func (a *ProtoForkable) BuildIndexes(ctx context.Context, decomp *seg.Decompressor, from, to RootNum, ps *background.ProgressSet) (indexes []*recsplit.Index, err error) {
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
		recsplitIdx, err := ib.Build(ctx, decomp, from, to, p)
		if err != nil {
			return indexes, err
		}
		indexes = append(indexes, recsplitIdx)

		ps.Delete(p)
	}
	closeFiles = false
	return
}

func (a *ProtoForkable) BuildIndexes2(ctx context.Context, from, to RootNum, ps *background.ProgressSet) (indexes []*recsplit.Index, err error) {
	var decomp *seg.Decompressor
	file, found := a.snaps.dirtyFiles.Get(&FilesItem{startTxNum: from.Uint64(), endTxNum: to.Uint64()})
	if found && file.decompressor != nil {
		decomp = file.decompressor
	} else {
		decomp, err = seg.NewDecompressorWithMetadata(a.fschema.DataFile(version.V1_0, from, to), a.snapCfg.HasMetadata)
		if err != nil {
			return nil, err
		}
		defer decomp.Close()
	}

	return a.BuildIndexes(ctx, decomp, from, to, ps)
}

func (a *ProtoForkable) isCompressionUsed(from, to RootNum) bool {
	// https://github.com/erigontech/erigon/pull/11222 -- decision taken here
	// is that 1k and 10k files will be uncompressed, but 10k files also merged
	// and not built off db, so following decision is okay:
	// AddWord -- compressed -- slowbuilds (merge etc.)
	// AddUncompressedWord -- uncompressed -- fast builds
	return uint64(to-from) > a.snapCfg.MinimumSize
}

func (a *ProtoForkable) Repo() *SnapshotRepo {
	return a.snaps
}

func (a *ProtoForkable) Close() {
	a.snaps.Close()
}

func (a *ProtoForkable) StepSize() uint64 {
	return a.snaps.stepSize
}

func (a *ProtoForkable) FilesWithMissedAccessors() *MissedFilesMap {
	return a.snaps.FilesWithMissedAccessors()
}

// proto_forkable_rotx

type ProtoForkableTx struct {
	id               kv.ForkableId
	files            visibleFiles
	m                []NumMetadata
	a                *ProtoForkable
	snaps            *SnapshotRepo
	noFiles          bool
	snappyReadBuffer []byte

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
		id:    a.id,
		files: visibleFiles,
		m:     a.metadata,
		a:     a,
		snaps: a.snaps,
	}
}

// take dirtyFiles lock before using this
func (a *ProtoForkable) DebugBeginDirtyFilesRo() *forkableDirtyFilesRoTx {
	var files []*FilesItem
	a.snaps.dirtyFiles.Walk(func(items []*FilesItem) bool {
		files = append(files, items...)
		for _, item := range items {
			if !item.frozen {
				item.refcount.Add(1)
			}
		}
		return true
	})
	return &forkableDirtyFilesRoTx{
		p:     a,
		files: files,
	}
}

func (a *ProtoForkable) BuildMissedAccessors(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet, missedFilesItems *MissedFilesMap) {
	for _, file := range missedFilesItems.Get(statecfg.AccessorHashMap) {
		cfile := file
		g.Go(func() error {
			indexes, err := a.BuildIndexes2(ctx, RootNum(cfile.startTxNum), RootNum(cfile.endTxNum), ps)
			if err != nil {
				return err
			}
			for _, idx := range indexes {
				idx.Close()
			}

			return nil
		})
	}
}

func (a *ProtoForkable) BeginNoFilesRo() *ProtoForkableTx {
	return &ProtoForkableTx{
		id:      a.id,
		files:   nil,
		a:       a,
		noFiles: true,
	}
}

func (a *ProtoForkableTx) Id() kv.ForkableId { return a.id }

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

func (a *ProtoForkableTx) Garbage(merged *FilesItem) (outs []*FilesItem) {
	return a.snaps.Garbage(a.files, merged)
}

func (a *ProtoForkableTx) VisibleFilesMaxRootNum() RootNum {
	a.NoFilesCheck()
	return RootNum(a.files.EndTxNum())
}

// exclusive
func (a *ProtoForkableTx) VisibleFilesMaxNum() Num {
	a.NoFilesCheck()
	lasti := len(a.files) - 1
	if lasti < 0 {
		return 0
	}
	return a.m[lasti].Last + 1 // .last is inclusive
}

// if either found=false or err != nil, then fileIdx = -1
// can get FileItem on which entityNum was found by a.Files()[fileIdx] etc.
func (a *ProtoForkableTx) GetFromFiles(entityNum Num) (b Bytes, found bool, fileIdx int, err error) {
	a.NoFilesCheck()
	index, found := a.getFileIndex(entityNum)
	if !found {
		return nil, false, -1, nil
	}

	v, f, err := a.GetFromFile(entityNum, index)
	return v, f, index, err
}

func (a *ProtoForkableTx) getFileIndex(entityNum Num) (int, bool) {
	a.NoFilesCheck()
	lastNum := a.VisibleFilesMaxNum()
	if entityNum >= lastNum {
		return -1, false
	}
	for i := range a.files {
		meta := a.m[i]
		if entityNum >= meta.First && entityNum <= meta.Last {
			return i, true
		}
		if entityNum < meta.First {
			break
		}
	}

	return -1, false
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

	pageSize := uint64(a.a.cfg.ValuesOnCompressedPage)
	isContinuous := pageSize <= 1
	file := a.files[idx]
	snaps := a.snaps
	stepSize := snaps.stepSize
	compressionUsed := a.a.isCompressionUsed(RootNum(file.startTxNum), RootNum(file.endTxNum))

	indexR := a.StatelessIdxReader(idx)
	if isContinuous {
		offset, ok := indexR.Lookup(entityNum.EncToBytes(true))
		if !ok {
			return nil, false, fmt.Errorf("entity %d not found in index %s:%d-%d", entityNum, snaps.name, file.startTxNum/stepSize, file.endTxNum/stepSize)
		}

		g := file.src.decompressor.MakeGetter()
		g.Reset(offset)
		compression := seg.CompressNone
		if compressionUsed {
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
	} else {
		// paged, key stored
		// index stores offsets for page and not entity num
		pageNum := Num((entityNum.Uint64() / pageSize) * pageSize)
		offset, ok := indexR.Lookup(pageNum.EncToBytes(true))
		if !ok {
			return nil, false, fmt.Errorf("entity %d (page %d) not found in index %s:%d-%d", entityNum, pageNum, snaps.name, file.startTxNum/stepSize, file.endTxNum/stepSize)
		}
		g := file.src.decompressor.MakeGetter()
		g.Reset(offset)

		v, _ := g.Next(nil)

		v, a.snappyReadBuffer = seg.GetFromPage(entityNum.EncToBytes(true), v, a.snappyReadBuffer, compressionUsed)
		return v, true, nil
	}

	return nil, false, fmt.Errorf("entity get error: %s expected %d in snapshot %s but not found", Registry.Name(ap.id), entityNum, a.files[idx].src.decompressor.FileName())

}

func (a *ProtoForkableTx) NoFilesCheck() {
	if a.noFiles {
		panic("snapshot read attempt on noFiles mode")
	}
}

func (a *ProtoForkableTx) StepSize() uint64 {
	return a.snaps.stepSize
}
