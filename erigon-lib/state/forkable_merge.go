package state

// type Merger struct {
// 	compressWorkers int
// 	tmpDir          string
// 	logger          log.Logger
// }

// func (m *Merger) Merge() error {

// 	return nil

// }

type ForkableMergeFiles struct {
	marked   []*filesItem
	unmarked []*filesItem
	buffered []*filesItem
}

func (f ForkableMergeFiles) Close() {
	fn := func(items []*filesItem) {
		for _, item := range items {
			item.closeFiles()
		}
	}
	fn(f.marked)
	fn(f.unmarked)
	fn(f.buffered)
}

func mergeForkableFiles(filesToMerge []visibleFile) (*filesItem, error) {
	return nil, nil
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
