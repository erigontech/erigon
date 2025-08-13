package state

import (
	"context"
	"fmt"
	"path"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
)

// type Merger struct {
// 	compressWorkers int
// 	tmpDir          string
// 	logger          log.Logger
// }

// func (m *Merger) Merge() error {

// 	return nil

// }

type ForkableMergeFiles struct {
	marked   []*FilesItem
	unmarked []*FilesItem
	buffered []*FilesItem
}

func NewForkableMergeFiles(markedSize, unmarkedSize, bufferedSize int) *ForkableMergeFiles {
	return &ForkableMergeFiles{
		marked:   make([]*FilesItem, markedSize),
		unmarked: make([]*FilesItem, unmarkedSize),
		buffered: make([]*FilesItem, bufferedSize),
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
	fn(f.buffered)
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

	return fn(f.marked) || fn(f.unmarked) || fn(f.buffered)
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
			err = fmt.Errorf("[forkable] merging panic for forkable_%s: %s", Registry.Name(f.a), filesToMerge.String(f.snaps.stepSize))
		}
	}()

	from, to := RootNum(filesToMerge[0].startTxNum), RootNum(filesToMerge.EndTxNum())

	segPath := f.snaps.schema.DataFile(version.V1_0, from, to)
	cfg := seg.DefaultCfg
	cfg.Workers = compressWorkers
	r := Registry
	comp, err := seg.NewCompressor(ctx, "merge_forkable_"+r.String(f.a), segPath, r.Dirs(f.a).Tmp, cfg, log.LvlTrace, f.logger)
	if err != nil {
		return
	}
	defer comp.Close()
	p := ps.AddNew("merge_forkable "+path.Base(segPath), 1)
	defer ps.Delete(p)
	var word = make([]byte, 0, 4096)

	var expectedTotal int
	for _, item := range filesToMerge {
		startRootNum, endRootNum := item.src.Range()
		compression := f.isCompressionUsed(RootNum(startRootNum), RootNum(endRootNum))
		g := item.src.decompressor.MakeGetter()

		for g.HasNext() {
			if compression {
				word, _ = g.Next(word[:0])
			} else {
				word, _ = g.NextUncompressed()
			}

			if err = comp.AddWord(word); err != nil {
				return
			}
		}

		expectedTotal += item.src.decompressor.Count()
	}

	if comp.Count() != expectedTotal {
		return nil, fmt.Errorf("unexpected amount after segments merge. got: %d, expected: %d", comp.Count(), expectedTotal)
	}
	if err = comp.Compress(); err != nil {
		return
	}

	mergedFile = newFilesItemWithSnapConfig(from.Uint64(), to.Uint64(), f.cfg)
	if mergedFile.decompressor, err = seg.NewDecompressor(segPath); err != nil {
		return
	}
	indexes, err := f.BuildIndexes(ctx, from, to, ps)
	if err != nil {
		return
	}
	// TODO: add multiple index support in filesItem
	mergedFile.index = indexes[0]
	closeFiles = false

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

	for i, ap := range r.buffered {
		fi := mf.buffered[i]
		if fi == nil {
			continue
		}
		ap.snaps.IntegrateDirtyFile(fi)
	}

	r.recalcVisibleFiles()
}
