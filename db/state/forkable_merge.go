package state

import (
	"context"
	"fmt"
	"path"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
)

type ForkableMergeFiles struct {
	marked   []*FilesItem
	unmarked []*FilesItem
}

func NewForkableMergeFiles(markedSize, unmarkedSize int) *ForkableMergeFiles {
	return &ForkableMergeFiles{
		marked:   make([]*FilesItem, markedSize),
		unmarked: make([]*FilesItem, unmarkedSize),
	}
}

func (f ForkableMergeFiles) Close() {
	fn := func(items []*FilesItem) {
		for _, item := range items {
			item.closeFiles()
		}
	}
	fn(f.marked)
	fn(f.unmarked)
}

func (f ForkableMergeFiles) MergedFilePresent() bool {
	fn := func(items []*FilesItem) bool {
		for _, item := range items {
			if item != nil {
				return true
			}
		}
		return false
	}

	return fn(f.marked) || fn(f.unmarked)
}

func (f *ProtoForkable) MergeFiles(ctx context.Context, _filesToMerge []visibleFile, compressWorkers int, ps *background.ProgressSet) (mergedFile *FilesItem, err error) {
	// expected sorted order if filesToMerge...
	filesToMerge := visibleFiles(_filesToMerge)
	if filesToMerge.Len() < 2 {
		return
	}
	closeFiles := true

	defer func() {
		if closeFiles {
			mergedFile.closeFilesAndRemove()
		}
		if rec := recover(); rec != nil {
			err = fmt.Errorf("[forkable] merging panic for forkable_%s: %s; %+v, trace: %s", Registry.Name(f.id), filesToMerge.String(f.snaps.stepSize), rec, dbg.Stack())
		}
	}()

	from, to := RootNum(filesToMerge[0].startTxNum), RootNum(filesToMerge.EndTxNum())

	segPath := f.snaps.schema.DataFile(version.V1_0, from, to)

	var exists bool
	exists, err = dir.FileExist(segPath)
	if err != nil {
		return
	}

	if !exists {
		cfg := seg.DefaultCfg
		cfg.Workers = compressWorkers
		cfg.ExpectMetadata = true
		r := Registry
		var comp *seg.Compressor
		comp, err = seg.NewCompressor(ctx, "merge_forkable_"+r.String(f.id), segPath, f.dirs.Tmp, cfg, log.LvlTrace, f.logger)
		if err != nil {
			return
		}
		defer comp.Close()
		writer := f.DataWriter(comp, f.isCompressionUsed(from, to))

		p := ps.AddNew(path.Base(segPath), 1)
		defer ps.Delete(p)

		{
			count := 0
			for _, item := range filesToMerge {
				count += item.src.decompressor.Count() * max(f.cfg.ValuesOnCompressedPage, 1) // approx
			}
			p.Total.Store(uint64(count))
		}

		meta := NumMetadata{}
		for _, item := range filesToMerge {
			var word = make([]byte, 0, 4096)
			startRootNum, endRootNum := item.src.Range()
			compression := f.isCompressionUsed(RootNum(startRootNum), RootNum(endRootNum))

			if err = item.src.decompressor.WithReadAhead(func() error {
				reader := f.PagedDataReader(item.src.decompressor, compression)
				var k, v []byte
				var fmeta NumMetadata
				if err := fmeta.Unmarshal(reader.GetMetadata()); err != nil {
					return err
				}
				if meta.Count == 0 {
					meta.First = fmeta.First
				}
				meta.Last = fmeta.Last
				meta.Count += fmeta.Count

				for reader.HasNext() {
					k, v, word, _ = reader.Next2(word[:0])
					if err = writer.Add(k, v); err != nil {
						return err
					}
					p.Processed.Add(1)
				}
				return nil
			}); err != nil {
				return
			}

		}

		mbytes, err := meta.Marshal()
		if err != nil {
			return nil, err
		}
		writer.SetMetadata(mbytes)
		if err = writer.Compress(); err != nil {
			return nil, err
		}

		comp.Close()
		ps.Delete(p)
	}

	mergedFile = newFilesItemWithSnapConfig(from.Uint64(), to.Uint64(), f.snapCfg)
	if mergedFile.decompressor, err = seg.NewDecompressorWithMetadata(segPath, f.snapCfg.HasMetadata); err != nil {
		return
	}
	indexes, err := f.BuildIndexes(ctx, mergedFile.decompressor, from, to, ps)
	if err != nil {
		return
	}
	// TODO: add multiple index support in filesItem
	mergedFile.index = indexes[0]
	closeFiles = false
	f.logger.Info("[fork_agg] merged", "from", from.Uint64()/f.snaps.stepSize, "to", to.Uint64()/f.snaps.stepSize)

	return
}

func (r *ForkableAgg) IntegrateMergeFiles(mf *ForkableMergeFiles) {
	r.dirtyFilesLock.Lock()
	defer r.dirtyFilesLock.Unlock()

	for i, ap := range r.marked {
		fi := mf.marked[i]
		if fi == nil {
			continue
		}
		ap.snaps.IntegrateDirtyFile(fi)
	}

	for i, ap := range r.unmarked {
		fi := mf.unmarked[i]
		if fi == nil {
			continue
		}
		ap.snaps.IntegrateDirtyFile(fi)
	}

	r.recalcVisibleFiles()
}
