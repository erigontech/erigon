package state

import (
	"fmt"
	"path/filepath"
	"sync"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// i) manages dirtyfiles and visible files,
// ii) dirtyfile integration
// iii) opening folder with dirty files
// iv) snap creation/merge configuration
// v) fileItemsWithMissedAccessors - missedBtreeAccessor/missedMapAccessor

// maybe accessor/btree build functions and data_file (.kv, .v, .seg) can also be supplied
// here as interfaces, this would allow more functions currently in DHII+A to be included here.
// caching version at caching_snap_repo.go

// NOTE: not thread safe; synchronization done on the caller side
// specially when accessing dirtyFiles or current.
type SnapshotRepo struct {
	dirtyFiles *btree2.BTreeG[*FilesItem]

	// latest version of visible files (derived from dirtyFiles)
	// when repo is used in the context of rotx, one might want to think
	// about which visibleFiles needs to be used - repo.current or
	// rotx.visibleFiles
	current visibleFiles
	entity  UniversalEntity
	name    string

	cfg       *SnapshotConfig
	schema    SnapNameSchema
	accessors statecfg.Accessors
	stepSize  uint64

	logger log.Logger
}

func NewSnapshotRepoForForkable(id ForkableId, logger log.Logger) *SnapshotRepo {
	return NewSnapshotRepo(Registry.Name(id), FromForkable(id), Registry.SnapshotConfig(id), logger)
}

func NewSnapshotRepo(name string, entity UniversalEntity, cfg *SnapshotConfig, logger log.Logger) *SnapshotRepo {
	return &SnapshotRepo{
		dirtyFiles: btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		name:       name,
		entity:     entity,
		cfg:        cfg,
		schema:     cfg.Schema,
		stepSize:   cfg.RootNumPerStep,
		accessors:  cfg.Schema.AccessorList(),
		logger:     logger,
	}
}

func (f *SnapshotRepo) OpenFolder() error {
	// this only sets up dirtyfiles, not visible files.
	// there is no integrity checks done here.
	files, err := filesFromDir(f.schema.DataDirectory())
	if err != nil {
		return err
	}

	f.closeWhatNotInList(files)
	f.loadDirtyFiles(files)
	if err := f.openDirtyFiles(); err != nil {
		return fmt.Errorf("SnapshotRepo(%s).openFolder: %w", f.schema.DataTag(), err)
	}
	return nil
}

func (f *SnapshotRepo) SetIntegrityChecker(integrity *DependencyIntegrityChecker) {
	f.cfg.Integrity = integrity
}

func (f *SnapshotRepo) Schema() SnapNameSchema {
	return f.schema
}

func (f *SnapshotRepo) IntegrateDirtyFile(file *FilesItem) {
	if file == nil {
		return
	}
	f.dirtyFiles.Set(file)
}

func (f *SnapshotRepo) IntegrateDirtyFiles(files []*FilesItem) {
	for _, file := range files {
		if file != nil {
			f.dirtyFiles.Set(file)
		}
	}
}

func (f *SnapshotRepo) IntegrateMergedFiles(dfs []*FilesItem, mergedFile *FilesItem) {
	if mergedFile != nil {
		f.dirtyFiles.Set(mergedFile)
	}
}

// DeleteFilesAfterMerge files are removed from repo and marked for deletion
// from file system.
func (f *SnapshotRepo) DeleteFilesAfterMerge(files []*FilesItem) {
	for _, file := range files {
		if file == nil {
			panic("must not happen: " + f.schema.DataTag())
		}
		f.dirtyFiles.Delete(file)
		file.canDelete.Store(true)

		// if merged file not visible for any alive reader (even for us): can remove it immediately
		// otherwise: mark it as `canDelete=true` and last reader of this file - will remove it inside `aggRoTx.Close()`
		if file.refcount.Load() == 0 {
			file.closeFilesAndRemove()

			if f.schema.DataTag() == traceFileLife && file.decompressor != nil {
				f.logger.Warn("[agg.dbg] DeleteFilesAfterMerge: remove", "f", file.decompressor.FileName())
			}
		} else {
			if f.schema.DataTag() == traceFileLife && file.decompressor != nil {
				f.logger.Warn("[agg.dbg] DeleteFilesAfterMerge: mark as canDelete=true", "f", file.decompressor.FileName())
			}
		}
	}
}

func (f *SnapshotRepo) DirtyFilesMaxRootNum() RootNum {
	fi, found := f.dirtyFiles.Max()
	if !found {
		return 0
	}
	return RootNum(fi.endTxNum)
}

func (f *SnapshotRepo) RecalcVisibleFiles(to RootNum) (maxRootNum RootNum) {
	f.current = f.calcVisibleFiles(to)
	return RootNum(f.current.EndTxNum())
}

// type VisibleFile = kv.VisibleFile
// type VisibleFiles = kv.VisibleFiles

func (f *SnapshotRepo) visibleFiles() visibleFiles {
	return f.current
}

func (f *SnapshotRepo) VisibleFiles() (files kv.VisibleFiles) {
	for _, file := range f.current {
		files = append(files, file)
	}
	return
}

func (f *SnapshotRepo) GetFreezingRange(from RootNum, to RootNum) (freezeFrom RootNum, freezeTo RootNum, canFreeze bool) {
	return getFreezingRange(from, to, f.cfg)
}

func (f *SnapshotRepo) DirtyFilesWithNoBtreeAccessors() (l []*FilesItem) {
	if !f.accessors.Has(statecfg.AccessorBTree) {
		return nil
	}
	p := f.schema
	ss := f.stepSize
	v := version.V1_0

	return fileItemsWithMissedAccessors(f.dirtyFiles.Items(), f.stepSize, func(fromStep, toStep kv.Step) []string {
		from, to := RootNum(uint64(fromStep)*ss), RootNum(uint64(toStep)*ss)
		fname := p.BtIdxFile(v, from, to)
		return []string{fname, p.ExistenceFile(v, from, to)}
	})
}

func (f *SnapshotRepo) DirtyFilesWithNoHashAccessors() (l []*FilesItem) {
	if !f.accessors.Has(statecfg.AccessorHashMap) {
		return nil
	}
	p := f.schema
	ss := f.stepSize
	v := version.V1_0
	accCount := f.schema.AccessorIdxCount()
	files := make([]string, accCount)

	return fileItemsWithMissedAccessors(f.dirtyFiles.Items(), f.stepSize, func(fromStep, toStep kv.Step) []string {
		for i := uint64(0); i < accCount; i++ {
			files[i] = p.AccessorIdxFile(v, RootNum(fromStep.ToTxNum(ss)), RootNum(toStep.ToTxNum(ss)), i)
		}
		return files
	})
}

func (f *SnapshotRepo) EndRootNum() RootNum {
	return RootNum(f.current.EndTxNum())
}

func (f *SnapshotRepo) Close() {
	if f == nil {
		return
	}
	f.closeWhatNotInList([]string{})
}

func (f *SnapshotRepo) CloseFilesAfterRootNum(after RootNum) {
	var toClose []*FilesItem
	rootNum := uint64(after)
	f.dirtyFiles.Scan(func(item *FilesItem) bool {
		if item.startTxNum >= rootNum {
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		f.dirtyFiles.Delete(item)
		fName := ""
		if item.decompressor != nil {
			fName = item.decompressor.FileName()
		}
		log.Debug(fmt.Sprintf("[snapshots] closing %s, instructed_close_after_%d", fName, rootNum))
		item.closeFiles()
	}
}

func (f *SnapshotRepo) CloseVisibleFilesAfterRootNum(after RootNum) {
	var i int
	for i = len(f.current) - 1; i >= 0; i-- {
		if f.current[i].endTxNum <= uint64(after) {
			break
		}
	}
	f.current = f.current[:i+1]
}

func (f *SnapshotRepo) Garbage(vfs visibleFiles, merged *FilesItem) (outs []*FilesItem) {
	checker := f.cfg.Integrity
	var cchecker func(startTxNum, endTxNum uint64) bool
	if checker != nil {
		cchecker = func(startTxNum, endTxNum uint64) bool {
			return checker.CheckDependentPresent(f.entity, Any, startTxNum, endTxNum)
		}
	}

	return garbage(f.dirtyFiles, vfs, merged, cchecker)
}

// TODO: crossRepoIntegrityCheck

// FindMergeRange returns the most recent merge range to process
// can be successively called with updated (merge processed) visibleFiles
// to get the next range to process.
func (f *SnapshotRepo) FindMergeRange(maxEndRootNum RootNum, files kv.VisibleFiles) (mrange MergeRange) {
	toRootNum := min(uint64(maxEndRootNum), files.EndRootNum())
	for i := 0; i < len(files); i++ {
		item := files[i]
		if item.EndRootNum() > toRootNum {
			break
		}

		startTxNum := RootNum(item.StartRootNum())
		calcFrom, calcTo, canFreeze := f.GetFreezingRange(startTxNum, RootNum(toRootNum))
		if !canFreeze {
			break
		}

		if calcFrom != startTxNum {
			panic(fmt.Sprintf("f.GetFreezingRange() returned wrong fromRootNum: %d, expected %d", calcFrom.Uint64(), startTxNum))
		}

		// skip through files which come under the above freezing range
		j := i + 1
		for ; j < len(files); j++ {
			item := files[j]
			if item.EndRootNum() > calcTo.Uint64() {
				break
			}

			// found a non-trivial range to merge
			// this function sends the most frequent merge range
			mrange.from = calcFrom.Uint64()
			mrange.needMerge = true
			mrange.to = item.EndRootNum()
		}

		i = j - 1
	}

	return
}

func (f *SnapshotRepo) FilesInRange(mrange MergeRange, files visibleFiles) (items []*FilesItem) {
	if !mrange.needMerge {
		return
	}

	for _, item := range files {
		if item.startTxNum < mrange.from {
			continue
		}
		if item.endTxNum > mrange.to {
			break
		}

		items = append(items, item.src)
	}

	return
}

func (f *SnapshotRepo) CleanAfterMerge(merged *FilesItem, vf visibleFiles) {
	outs := f.Garbage(vf, merged)
	f.DeleteFilesAfterMerge(outs)
}

// private methods

func (f *SnapshotRepo) openDirtyFiles() error {
	invalidFilesMu := sync.Mutex{}
	invalidFileItems := make([]*FilesItem, 0)
	p := f.schema
	f.dirtyFiles.Walk(func(items []*FilesItem) bool {
		for _, item := range items {
			if item.decompressor == nil {
				fPathGen := p.DataFile(version.V1_0, RootNum(item.startTxNum), RootNum(item.endTxNum))
				fPathMask, _ := version.ReplaceVersionWithMask(fPathGen)
				fPath, _, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil || !ok {
					_, fName := filepath.Split(fPath)
					if err == nil {
						f.logger.Debug("SnapshotRepo.openDirtyFiles: file doesn't exist", "f", fName)
					} else {
						f.logger.Debug("SnapshotRepo.openDirtyFiles: FileExist", "f", fName, "err", err)
					}
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					continue
				}
				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					f.logger.Error("SnapshotRepo.openDirtyFiles", "err", err, "f", fName)
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					continue
				}
			}

			accessors := p.AccessorList()

			if item.index == nil && accessors.Has(statecfg.AccessorHashMap) {
				fPathGen := p.AccessorIdxFile(version.V1_0, RootNum(item.startTxNum), RootNum(item.endTxNum), 0)
				fPathMask, _ := version.ReplaceVersionWithMask(fPathGen)
				fPath, _, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					f.logger.Debug("SnapshotRepo.openDirtyFiles: FileExist", "f", fName, "err", err)
				}

				if ok {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						f.logger.Error("SnapshotRepo.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files maybe good
					}
				}
			}

			if item.bindex == nil && accessors.Has(statecfg.AccessorBTree) {
				fPathGen := p.BtIdxFile(version.V1_0, RootNum(item.startTxNum), RootNum(item.endTxNum))
				fPathMask, _ := version.ReplaceVersionWithMask(fPathGen)
				fPath, _, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					f.logger.Warn("[agg] SnapshotRepo.openDirtyFiles", "err", err, "f", fName)
				}
				if ok {
					r := seg.NewReader(item.decompressor.MakeGetter(), p.DataFileCompression())
					if item.bindex, err = OpenBtreeIndexWithDecompressor(fPath, DefaultBtreeM, r); err != nil {
						_, fName := filepath.Split(fPath)
						f.logger.Error("SnapshotRepo.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files maybe good
					}
				}
			}
			if item.existence == nil && accessors.Has(statecfg.AccessorExistence) {
				fPathGen := p.ExistenceFile(version.V1_0, RootNum(item.startTxNum), RootNum(item.endTxNum))
				fPathMask, _ := version.ReplaceVersionWithMask(fPathGen)
				fPath, _, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					f.logger.Debug("SnapshotRepo.openDirtyFiles: FileExist", "f", fName, "err", err)
				}
				if ok {
					if item.existence, err = existence.OpenFilter(fPath, false); err != nil {
						_, fName := filepath.Split(fPath)
						f.logger.Error("SnapshotRepo.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files maybe good
					}
				}
			}
		}
		return true
	})

	for _, item := range invalidFileItems {
		item.closeFiles()
		f.dirtyFiles.Delete(item)
	}

	return nil
}

func (f *SnapshotRepo) closeWhatNotInList(fNames []string) {
	protectFiles := make(map[string]struct{}, len(fNames))
	for _, f := range fNames {
		protectFiles[f] = struct{}{}
	}
	var toClose []*FilesItem
	f.dirtyFiles.Walk(func(items []*FilesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				if _, ok := protectFiles[item.decompressor.FileName()]; ok {
					continue
				}
			}
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		item.closeFiles()
		f.dirtyFiles.Delete(item)
	}
}

func (f *SnapshotRepo) loadDirtyFiles(aps []string) {
	if f.stepSize == 0 {
		panic("step size if 0 for " + f.schema.DataTag())
	}

	for _, ap := range aps {
		fileInfo, ok := f.schema.Parse(ap)
		if !ok {
			f.logger.Trace("can't parse file name", "file", ap)
			continue
		}
		dirtyFile := newFilesItemWithSnapConfig(fileInfo.From, fileInfo.To, f.cfg)

		if _, has := f.dirtyFiles.Get(dirtyFile); !has {
			f.dirtyFiles.Set(dirtyFile)
		}
	}
}

func (f *SnapshotRepo) calcVisibleFiles(to RootNum) (roItems []visibleFile) {
	checker := f.cfg.Integrity
	var cchecker func(startTxNum, endTxNum uint64) bool
	if checker != nil {
		cchecker = func(startTxNum, endTxNum uint64) bool {
			return checker.CheckDependentPresent(f.entity, All, startTxNum, endTxNum)
		}
	}

	return calcVisibleFiles(f.dirtyFiles, f.accessors, cchecker, false, uint64(to))
}

// determine freezing ranges, given snapshot creation config
func getFreezingRange(rootFrom, rootTo RootNum, cfg *SnapshotConfig) (freezeFrom RootNum, freezeTo RootNum, canFreeze bool) {
	/**
	 1. `from`, `to` must be round off to minimum size (atleast)
	 2. mergeLimit is a function: (from, preverified files, mergeLimit default) -> biggest file size starting `from`
	 3. if mergeLimit size is not possible, then `freezeTo` should be next largest possible file size
	    as allowed by the MergeSteps or MinimumSize.
	**/

	from := uint64(rootFrom)
	to := uint64(rootTo)

	if to < cfg.SafetyMargin {
		return rootFrom, rootTo, false
	}

	to -= cfg.SafetyMargin
	from = (from / cfg.MinimumSize) * cfg.MinimumSize
	to = (to / cfg.MinimumSize) * cfg.MinimumSize
	if from >= to {
		return rootFrom, rootTo, false
	}

	mergeLimit := getMergeLimit(cfg, from)
	maxJump := cfg.RootNumPerStep

	if from%mergeLimit == 0 {
		maxJump = mergeLimit
	} else {
		for i := len(cfg.MergeStages) - 1; i >= 0; i-- {
			if from%cfg.MergeStages[i] == 0 {
				maxJump = cfg.MergeStages[i]
				break
			}
		}
	}

	_freezeFrom := from
	var _freezeTo uint64
	jump := to - from

	switch {
	case jump >= maxJump:
		// enough data, max jump
		_freezeTo = _freezeFrom + maxJump
	case jump >= cfg.MergeStages[0]:
		// else find if a merge step can be used
		// assuming merge step multiple of each other
		for i := len(cfg.MergeStages) - 1; i >= 0; i-- {
			if jump >= cfg.MergeStages[i] {
				_freezeTo = _freezeFrom + cfg.MergeStages[i]
				break
			}
		}
	case jump >= cfg.MinimumSize:
		// else use minimum size
		_freezeTo = _freezeFrom + cfg.MinimumSize

	default:
		_freezeTo = _freezeFrom
	}

	return RootNum(_freezeFrom), RootNum(_freezeTo), _freezeTo-_freezeFrom >= cfg.MinimumSize
}

func getMergeLimit(cfg *SnapshotConfig, from uint64) uint64 {
	//return 0
	maxMergeLimit := cfg.MergeStages[len(cfg.MergeStages)-1]

	for _, info := range cfg.PreverifiedParsed {
		if !info.IsDataFile() {
			continue
		}

		if from < info.From || from >= info.To {
			continue
		}

		if info.Len() >= maxMergeLimit {
			// info.Len() > maxMergeLimit --> this happens when previously a larger value
			// was used, and now the configured merge limit is smaller.
			return info.Len()
		}

		break
	}

	return maxMergeLimit
}
