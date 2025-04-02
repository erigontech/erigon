package state

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	ae "github.com/erigontech/erigon-lib/state/appendable_extras"
	btree2 "github.com/tidwall/btree"
)

// manages dirtyfiles and visible files,
// providing safe access
// and initiation inside a folder
// configuration for files creation/merging
// caching version at caching_files_registry.go
type FilesRegistry struct {
	dirtyFiles *btree2.BTreeG[*filesItem]
	_visible   visibleFiles
	name       string
	dir        string

	cfg     *ae.SnapshotConfig
	parser  ae.SnapNameParser
	aggStep uint64

	logger log.Logger
}

func NewFilesRegistryForAppendable(id AppendableId, logger log.Logger) *FilesRegistry {
	f := &FilesRegistry{
		dirtyFiles: btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		name:       id.Name(),
		dir:        id.SnapshotDir(),
		cfg:        id.SnapshotConfig(),
		parser:     id.SnapshotConfig().Parser,
		aggStep:    id.SnapshotConfig().RootNumPerStep,
		logger:     logger,
	}
	return f
}

func (f *FilesRegistry) OpenFolder() error {
	files, err := filesFromDir(f.dir)
	if err != nil {
		return err
	}

	f.closeWhatNotInList(files)
	f.scanDirtyFiles(files)
	if err := f.openDirtyFiles(); err != nil {
		return fmt.Errorf("Registry(%s).openFolder: %w", f.parser.Name(), err)
	}

	// TODO
	return nil
}

func (f *FilesRegistry) IntegrateDirtyFile(file *filesItem) {
	f.dirtyFiles.Set(file)
}

func (f *FilesRegistry) IntegrateDirtyFiles(files []*filesItem) {
	for _, file := range files {
		f.dirtyFiles.Set(file)
	}
}

func (f *FilesRegistry) GetFreezingRange(from RootNum, to RootNum) (freezeFrom RootNum, freezeTo RootNum, canFreeze bool) {
	return GetFreezingRange(from, to, f.cfg)
}

func (f *FilesRegistry) openDirtyFiles() error {
	invalidFilesMu := sync.Mutex{}
	invalidFileItems := make([]*filesItem, 0)
	p := f.parser
	version := snaptype.Version(1)
	f.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor == nil {
				fPath := p.DataFile(version, ae.RootNum(item.startTxNum), ae.RootNum(item.endTxNum))
				exists, err := dir.FileExist(fPath)
				if err != nil || !exists {
					_, fName := filepath.Split(fPath)
					if err != nil {
						f.logger.Debug("Registry.openDirtyFiles: FileExist", "f", fName, "err", err)
					} else {
						f.logger.Debug("Registry.openDirtyFiles: file doesn't exist", "f", fName)
					}
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					continue
				}
				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					f.logger.Error("Registry.openDirtyFiles", "err", err, "f", fName)
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					continue
				}
			}

			if item.index == nil {}
		}
		return true
	})

	return nil

}

func (f *FilesRegistry) closeWhatNotInList(fNames []string) {
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

func (f *FilesRegistry) scanDirtyFiles(aps []string) (res []*filesItem) {
	for _, ap := range aps {
		fileInfo, ok := f.parser.Parse(ap)
		if !ok {
			f.logger.Trace("can't parse file name", "file", ap)
			continue
		}
		res = append(res, newFilesItemWithSnapConfig(fileInfo.From, fileInfo.To, f.cfg))
	}
	return res
}

// determine freezing ranges, given snapshot creation config
func GetFreezingRange(rootFrom, rootTo RootNum, cfg *ae.SnapshotConfig) (freezeFrom RootNum, freezeTo RootNum, canFreeze bool) {
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
