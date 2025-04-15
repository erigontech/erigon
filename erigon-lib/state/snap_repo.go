package state

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	ae "github.com/erigontech/erigon-lib/state/appendable_extras"
	btree2 "github.com/tidwall/btree"
)

// i) manages dirtyfiles and visible files,
// ii) dirtyfile integration
// iii) opening folder with dirty files
// iv) snap creation/merge configuration
// v) fileItemsWithMissingAccessors - missedBtreeAccessor/missedMapAccessor

// maybe accessor/btree build functions and data_file (.kv, .v, .seg) can also be supplied
// here as interfaces, this would allow more functions currently in DHII+A to be included here.
// caching version at caching_snap_repo.go

// NOTE: not thread safe; synchronization done on the caller side
// specially when accessing dirtyFiles or current.
type SnapshotRepo struct {
	dirtyFiles *btree2.BTreeG[*filesItem]

	// latest version of visible files (derived from dirtyFiles)
	// when repo is used in the context of rotx, one might want to think
	// about which visibleFiles needs to be used - repo.current or
	// rotx.visibleFiles
	current visibleFiles
	name    string

	cfg       *ae.SnapshotConfig
	schema    ae.SnapNameSchema
	accessors Accessors
	stepSize  uint64

	logger log.Logger
}

func NewSnapshotRepoForAppendable(id AppendableId, logger log.Logger) *SnapshotRepo {
	return NewSnapshotRepo(id.Name(), id.SnapshotConfig(), logger)
}

func NewSnapshotRepo(name string, cfg *ae.SnapshotConfig, logger log.Logger) *SnapshotRepo {
	return &SnapshotRepo{
		dirtyFiles: btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		name:       name,
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

func (f *SnapshotRepo) SetIntegrityChecker(integrity ae.IntegrityChecker) {
	f.cfg.Integrity = integrity
}

func (f *SnapshotRepo) Schema() ae.SnapNameSchema {
	return f.schema
}

func (f *SnapshotRepo) IntegrateDirtyFile(file *filesItem) {
	if file == nil {
		return
	}
	f.dirtyFiles.Set(file)
}

func (f *SnapshotRepo) IntegrateDirtyFiles(files []*filesItem) {
	for _, file := range files {
		if file != nil {
			f.dirtyFiles.Set(file)
		}
	}
}

func (f *SnapshotRepo) RecalcVisibleFiles(to RootNum) {
	f.current = f.calcVisibleFiles(to)
}

type VisibleFile interface {
	Filename() string
	StartTxNum() uint64
	EndTxNum() uint64
}

type VisibleFiles []VisibleFile

func (f *SnapshotRepo) visibleFiles() visibleFiles {
	return f.current
}

func (f *SnapshotRepo) VisibleFiles() (files []VisibleFile) {
	for _, file := range f.current {
		files = append(files, file)
	}
	return
}

func (f *SnapshotRepo) GetFreezingRange(from RootNum, to RootNum) (freezeFrom RootNum, freezeTo RootNum, canFreeze bool) {
	return getFreezingRange(from, to, f.cfg)
}

func (f *SnapshotRepo) DirtyFilesWithNoBtreeAccessors() (l []*filesItem) {
	if !f.accessors.Has(AccessorBTree) {
		return nil
	}
	p := f.schema
	ss := f.stepSize
	v := ae.Version(1)

	return fileItemsWithMissingAccessors(f.dirtyFiles, f.stepSize, func(fromStep uint64, toStep uint64) []string {
		from, to := RootNum(fromStep*ss), RootNum(toStep*ss)
		fname := p.BtIdxFile(v, from, to)
		return []string{fname, p.ExistenceFile(v, from, to)}
	})
}

func (f *SnapshotRepo) DirtyFilesWithNoHashAccessors() (l []*filesItem) {
	if !f.accessors.Has(AccessorHashMap) {
		return nil
	}
	p := f.schema
	ss := f.stepSize
	v := ae.Version(1)
	accCount := f.schema.AccessorIdxCount()
	files := make([]string, accCount)

	return fileItemsWithMissingAccessors(f.dirtyFiles, f.stepSize, func(fromStep uint64, toStep uint64) []string {
		for i := uint64(0); i < accCount; i++ {
			files[i] = p.AccessorIdxFile(v, RootNum(fromStep*ss), RootNum(toStep*ss), i)
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
	var toClose []*filesItem
	rootNum := uint64(after)
	f.dirtyFiles.Scan(func(item *filesItem) bool {
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

func (f *SnapshotRepo) Garbage(visibleFiles []visibleFile, merged *filesItem) (outs []*filesItem) {
	if merged == nil {
		return
	}

	integrity := f.cfg.Integrity
	f.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen {
				continue
			}
			if item.isProperSubsetOf(merged) {
				if integrity != nil && integrity.Check(ae.RootNum(item.startTxNum), ae.RootNum(item.endTxNum)) {
					continue
				}
				outs = append(outs, item)
				continue
			}
			// delete garbage file only if it's before merged range and it has bigger file (which indexed and visible for user now - using rotx)
			if item.isBefore(merged) && hasCoverVisibleFile(visibleFiles, item) {
				if integrity != nil && integrity.Check(ae.RootNum(item.startTxNum), ae.RootNum(item.endTxNum)) {
					continue
				}
				outs = append(outs, item)
			}
		}
		return true
	})

	return outs
}

// TODO: crossRepoIntegrityCheck

// FindMergeRange returns the most recent merge range to process
// can be successively called with updated (merge processed) visibleFiles
// to get the next range to process.
func (f *SnapshotRepo) FindMergeRange(maxEndRootNum RootNum, files visibleFiles) (mrange MergeRange) {
	toRootNum := min(uint64(maxEndRootNum), files.EndTxNum())
	for i := 0; i < len(files); i++ {
		item := files[i]
		if item.endTxNum > toRootNum {
			break
		}

		calcFrom, calcTo, canFreeze := f.GetFreezingRange(RootNum(item.startTxNum), RootNum(toRootNum))
		if !canFreeze {
			break
		}

		if calcFrom.Uint64() != item.startTxNum {
			panic(fmt.Sprintf("f.GetFreezingRange() returned wrong fromRootNum: %d, expected %d", calcFrom.Uint64(), item.startTxNum))
		}

		// skip through files which come under the above freezing range
		j := i + 1
		for ; j < len(files); j++ {
			item := files[j]
			if item.endTxNum > calcTo.Uint64() {
				break
			}

			// found a non-trivial range to merge
			// this function sends the most frequent merge range
			mrange.from = calcFrom.Uint64()
			mrange.needMerge = true
			mrange.to = item.endTxNum
		}

		i = j - 1
	}

	return
}

func (f *SnapshotRepo) FilesInRange(mrange MergeRange, files visibleFiles) (items []*filesItem) {
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

func (f *SnapshotRepo) CleanAfterMerge(merged *filesItem, vf visibleFiles) {
	outs := f.Garbage(vf, merged)
	deleteMergeFile(f.dirtyFiles, outs, f.schema.DataTag(), f.logger)
}

// private methods

func (f *SnapshotRepo) openDirtyFiles() error {
	invalidFilesMu := sync.Mutex{}
	invalidFileItems := make([]*filesItem, 0)
	p := f.schema
	version := snaptype.Version(1)
	f.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor == nil {
				fPath := p.DataFile(version, ae.RootNum(item.startTxNum), ae.RootNum(item.endTxNum))
				exists, err := dir.FileExist(fPath)
				if err != nil || !exists {
					_, fName := filepath.Split(fPath)
					if err != nil {
						f.logger.Debug("SnapshotRepo.openDirtyFiles: FileExist", "f", fName, "err", err)
					} else {
						f.logger.Debug("SnapshotRepo.openDirtyFiles: file doesn't exist", "f", fName)
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

			if item.index == nil && accessors.Has(AccessorHashMap) {
				fPath := p.AccessorIdxFile(version, ae.RootNum(item.startTxNum), ae.RootNum(item.endTxNum), 0)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					f.logger.Debug("SnapshotRepo.openDirtyFiles: FileExist", "f", fName, "err", err)
				}

				if exists {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						f.logger.Error("SnapshotRepo.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files maybe good
					}
				}
			}

			if item.bindex == nil && accessors.Has(AccessorBTree) {
				fPath := p.BtIdxFile(version, ae.RootNum(item.startTxNum), ae.RootNum(item.endTxNum))
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					f.logger.Warn("[agg] SnapshotRepo.openDirtyFiles", "err", err, "f", fName)
				}
				if exists {
					if item.bindex, err = OpenBtreeIndexWithDecompressor(fPath, DefaultBtreeM, item.decompressor, p.DataFileCompression()); err != nil {
						_, fName := filepath.Split(fPath)
						f.logger.Error("SnapshotRepo.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files maybe good
					}
				}
			}
			if item.existence == nil && accessors.Has(AccessorExistence) {
				fPath := p.ExistenceFile(version, ae.RootNum(item.startTxNum), ae.RootNum(item.endTxNum))
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					f.logger.Debug("SnapshotRepo.openDirtyFiles: FileExist", "f", fName, "err", err)
				}
				if exists {
					if item.existence, err = OpenExistenceFilter(fPath); err != nil {
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
	var toClose []*filesItem
	f.dirtyFiles.Walk(func(items []*filesItem) bool {
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
		panic(fmt.Sprintf("step size if 0 for %s", f.schema.DataTag()))
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
	files := f.dirtyFiles
	trace := false

	newVisibleFiles := make([]visibleFile, 0, files.Len())
	integrity := f.cfg.Integrity
	if trace {
		log.Warn("[dbg] calcVisibleFiles", "amount", files.Len(), "toTxNum", to)
	}
	files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.endTxNum > to.Uint64() {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: ends after limit", "f", item.decompressor.FileName(), "limitRootNum", to)
				}
				continue
			}
			if item.canDelete.Load() {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: canDelete=true", "f", item.decompressor.FileName())
				}
				continue
			}

			// TODO: need somehow handle this case, but indices do not open in tests TestFindMergeRangeCornerCases
			if item.decompressor == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: decompressor not opened", "from", item.startTxNum, "to", item.endTxNum)
				}
				continue
			}
			if f.accessors.Has(AccessorBTree) && item.bindex == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: BTindex not opened", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("btindex nil: %s", item.decompressor.FileName()))
				continue
			}
			if f.accessors.Has(AccessorHashMap) && item.index == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: RecSplit not opened", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("index nil: %s", item.decompressor.FileName()))
				continue
			}
			if f.accessors.Has(AccessorExistence) && item.existence == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: Existence not opened", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("existence nil: %s", item.decompressor.FileName()))
				continue
			}

			if integrity != nil && !integrity.Check(ae.RootNum(item.startTxNum), ae.RootNum(item.endTxNum)) {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: integrity check failed, skipping:", "from", item.startTxNum, "to", item.endTxNum)
				}
				continue
			}

			// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
			// see super-set file, just drop sub-set files from list
			for len(newVisibleFiles) > 0 && newVisibleFiles[len(newVisibleFiles)-1].src.isProperSubsetOf(item) {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: marked as garbage (is subset)", "item", item.decompressor.FileName(),
						"of", newVisibleFiles[len(newVisibleFiles)-1].src.decompressor.FileName())
				}
				newVisibleFiles[len(newVisibleFiles)-1].src = nil
				newVisibleFiles = newVisibleFiles[:len(newVisibleFiles)-1]
			}

			// log.Warn("willBeVisible", "newVisibleFile", item.decompressor.FileName())
			newVisibleFiles = append(newVisibleFiles, visibleFile{
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				i:          len(newVisibleFiles),
				src:        item,
			})
		}
		return true
	})
	if newVisibleFiles == nil {
		newVisibleFiles = []visibleFile{}
	}
	return newVisibleFiles
}

// determine freezing ranges, given snapshot creation config
func getFreezingRange(rootFrom, rootTo RootNum, cfg *ae.SnapshotConfig) (freezeFrom RootNum, freezeTo RootNum, canFreeze bool) {
	/**
	 1. `from`, `to` must be round off to minimum size (atleast)
	 2. mergeLimit is a function: (from, preverified files, mergeLimit default) -> biggest file size starting `from`
	 3. if mergeLimit size is not possible, then `freezeTo` should be next largest possible file size
	    as allowed by the MergeSteps or MinimumSize.
	**/

	if rootFrom >= rootTo {
		return rootFrom, rootTo, false
	}

	from := uint64(rootFrom)
	to := uint64(rootTo)

	to = to - cfg.SafetyMargin
	from = (from / cfg.MinimumSize) * cfg.MinimumSize
	to = (to / cfg.MinimumSize) * cfg.MinimumSize

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

func getMergeLimit(cfg *ae.SnapshotConfig, from uint64) uint64 {
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
