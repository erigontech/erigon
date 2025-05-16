// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/datastruct/existence"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	ee "github.com/erigontech/erigon-lib/state/entity_extras"
)

// filesItem is "dirty" file - means file which can be:
//   - uncomplete
//   - not_indexed
//   - overlaped_by_bigger_file
//   - marked_as_ready_for_delete
//   - can also be "good" file
//
// such files must be hiddend from user (reader), but may be useful for background merging process, etc...
// list of filesItem must be represented as Tree - because they may overlap

// visibleFile - class is used for good/visible files
type filesItem struct {
	decompressor         *seg.Decompressor
	index                *recsplit.Index
	bindex               *BtIndex
	existence            *existence.Filter
	startTxNum, endTxNum uint64 //[startTxNum, endTxNum)

	// Frozen: file of size StepsInFrozenFile. Completely immutable.
	// Cold: file of size < StepsInFrozenFile. Immutable, but can be closed/removed after merge to bigger file.
	// Hot: Stored in DB. Providing Snapshot-Isolation by CopyOnWrite.
	frozen   bool         // immutable, don't need atomic
	refcount atomic.Int32 // only for `frozen=false`

	// file can be deleted in 2 cases: 1. when `refcount == 0 && canDelete == true` 2. on app startup when `file.isSubsetOfFrozenFile()`
	// other processes (which also reading files, may have same logic)
	canDelete atomic.Bool
}

type FilesItem interface {
	Segment() *seg.Decompressor
	AccessorIndex() *recsplit.Index
	BtIndex() *BtIndex
	ExistenceFilter() *existence.Filter
	Range() (startTxNum, endTxNum uint64)
}

var _ FilesItem = (*filesItem)(nil)

func newFilesItem(startTxNum, endTxNum, stepSize uint64) *filesItem {
	return newFilesItemWithFrozenSteps(startTxNum, endTxNum, stepSize, config3.StepsInFrozenFile)
}

func newFilesItemWithSnapConfig(startTxNum, endTxNum uint64, snapConfig *ee.SnapshotConfig) *filesItem {
	return newFilesItemWithFrozenSteps(startTxNum, endTxNum, snapConfig.RootNumPerStep, snapConfig.StepsInFrozenFile())
}

func newFilesItemWithFrozenSteps(startTxNum, endTxNum, stepSize uint64, stepsInFrozenFile uint64) *filesItem {
	startStep := startTxNum / stepSize
	endStep := endTxNum / stepSize
	frozen := endStep-startStep >= stepsInFrozenFile
	return &filesItem{startTxNum: startTxNum, endTxNum: endTxNum, frozen: frozen}
}

func (i *filesItem) Segment() *seg.Decompressor { return i.decompressor }

func (i *filesItem) AccessorIndex() *recsplit.Index { return i.index }

func (i *filesItem) BtIndex() *BtIndex { return i.bindex }

func (i *filesItem) ExistenceFilter() *existence.Filter { return i.existence }

func (i *filesItem) Range() (startTxNum, endTxNum uint64) {
	return i.startTxNum, i.endTxNum
}

// isProperSubsetOf - when `j` covers `i` but not equal `i`
func (i *filesItem) isProperSubsetOf(j *filesItem) bool {
	return (j.startTxNum <= i.startTxNum && i.endTxNum <= j.endTxNum) && (j.startTxNum != i.startTxNum || i.endTxNum != j.endTxNum)
}
func (i *filesItem) isBefore(j *filesItem) bool { return i.endTxNum <= j.startTxNum }

func filesItemLess(i, j *filesItem) bool {
	if i.endTxNum == j.endTxNum {
		return i.startTxNum > j.startTxNum
	}
	return i.endTxNum < j.endTxNum
}

func (i *filesItem) closeFiles() {
	if i.decompressor != nil {
		i.decompressor.Close()
		i.decompressor = nil
	}
	if i.index != nil {
		i.index.Close()
		i.index = nil
	}
	if i.bindex != nil {
		i.bindex.Close()
		i.bindex = nil
	}
	if i.existence != nil {
		i.existence.Close()
		i.existence = nil
	}
}

func (i *filesItem) closeFilesAndRemove() {
	if i.decompressor != nil {
		i.decompressor.Close()
		// paranoic-mode on: don't delete frozen files
		if !i.frozen {
			if err := os.Remove(i.decompressor.FilePath()); err != nil {
				log.Trace("remove after close", "err", err, "file", i.decompressor.FileName())
			}
			if err := os.Remove(i.decompressor.FilePath() + ".torrent"); err != nil {
				log.Trace("remove after close", "err", err, "file", i.decompressor.FileName()+".torrent")
			}
		}
		i.decompressor = nil
	}
	if i.index != nil {
		i.index.Close()
		// paranoic-mode on: don't delete frozen files
		if !i.frozen {
			if err := os.Remove(i.index.FilePath()); err != nil {
				log.Trace("remove after close", "err", err, "file", i.index.FileName())
			}
			if err := os.Remove(i.index.FilePath() + ".torrent"); err != nil {
				log.Trace("remove after close", "err", err, "file", i.index.FileName())
			}
		}
		i.index = nil
	}
	if i.bindex != nil {
		i.bindex.Close()
		if err := os.Remove(i.bindex.FilePath()); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bindex.FileName())
		}
		if err := os.Remove(i.bindex.FilePath() + ".torrent"); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bindex.FileName())
		}
		i.bindex = nil
	}
	if i.existence != nil {
		i.existence.Close()
		if err := os.Remove(i.existence.FilePath); err != nil {
			log.Trace("remove after close", "err", err, "file", i.existence.FileName)
		}
		if err := os.Remove(i.existence.FilePath + ".torrent"); err != nil {
			log.Trace("remove after close", "err", err, "file", i.existence.FilePath)
		}
		i.existence = nil
	}
}

func scanDirtyFiles(fileNames []string, stepSize uint64, filenameBase, ext string, logger log.Logger) (res []*filesItem) {
	re := regexp.MustCompile(`^v(\d+(?:\.\d+)?)-` + filenameBase + `\.(\d+)-(\d+)\.` + ext + `$`)
	var err error

	for _, name := range fileNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				logger.Warn("File ignored by domain scan, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			logger.Warn("File ignored by domain scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			logger.Warn("File ignored by domain scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			logger.Warn("File ignored by domain scan, startTxNum > endTxNum", "name", name)
			continue
		}

		// Semantic: [startTxNum, endTxNum)
		// Example:
		//   stepSize = 4
		//   0-1.kv: [0, 8)
		//   0-2.kv: [0, 16)
		//   1-2.kv: [8, 16)
		startTxNum, endTxNum := startStep*stepSize, endStep*stepSize

		var newFile = newFilesItem(startTxNum, endTxNum, stepSize)
		res = append(res, newFile)
	}
	return res
}

func deleteMergeFile(dirtyFiles *btree2.BTreeG[*filesItem], outs []*filesItem, filenameBase string, logger log.Logger) {
	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + filenameBase)
		}
		dirtyFiles.Delete(out)
		out.canDelete.Store(true)

		// if merged file not visible for any alive reader (even for us): can remove it immediately
		// otherwise: mark it as `canDelete=true` and last reader of this file - will remove it inside `aggRoTx.Close()`
		if out.refcount.Load() == 0 {
			out.closeFilesAndRemove()

			if filenameBase == traceFileLife && out.decompressor != nil {
				logger.Warn("[agg.dbg] deleteMergeFile: remove", "f", out.decompressor.FileName())
			}
		} else {
			if filenameBase == traceFileLife && out.decompressor != nil {
				logger.Warn("[agg.dbg] deleteMergeFile: mark as canDelete=true", "f", out.decompressor.FileName())
			}
		}
	}
}

// visibleFile is like filesItem but only for good/visible files (indexed, not overlaped, not marked for deletion, etc...)
// it's ok to store visibleFile in array
type visibleFile struct {
	getter     *seg.Getter
	reader     *recsplit.IndexReader
	startTxNum uint64
	endTxNum   uint64

	i   int
	src *filesItem
}

func (i visibleFile) Filename() string {
	return i.src.decompressor.FilePath()
}

func (i visibleFile) StartRootNum() uint64 {
	return i.startTxNum
}

func (i visibleFile) EndRootNum() uint64 {
	return i.endTxNum
}

func calcVisibleFiles(files *btree2.BTreeG[*filesItem], l Accessors, trace bool, toTxNum uint64) (roItems []visibleFile) {
	newVisibleFiles := make([]visibleFile, 0, files.Len())
	// trace = true
	if trace {
		log.Warn("[dbg] calcVisibleFiles", "amount", files.Len(), "toTxNum", toTxNum)
	}
	files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.endTxNum > toTxNum {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: ends after limit", "f", item.decompressor.FileName(), "limitTxNum", toTxNum)
				}
				continue
			}
			if !checkFilesItemFields(item, l, trace) {
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

func checkFilesItemFields(item *filesItem, l Accessors, trace bool) (ok bool) {
	if item.canDelete.Load() {
		if trace {
			log.Warn("[dbg] canDelete=true", "f", item.decompressor.FileName())
		}
		return false
	}
	if item.decompressor == nil {
		if trace {
			log.Warn("[dbg] decompressor not opened", "from", item.startTxNum, "to", item.endTxNum)
		}
		return false
	}
	if l.Has(AccessorBTree) && item.bindex == nil {
		if trace {
			log.Warn("[dbg] calcVisibleFiles: BTindex not opened", "f", item.decompressor.FileName())
		}
		//panic(fmt.Errorf("btindex nil: %s", item.decompressor.FileName()))
		return false
	}
	if l.Has(AccessorHashMap) && item.index == nil {
		if trace {
			log.Warn("[dbg] calcVisibleFiles: RecSplit not opened", "f", item.decompressor.FileName())
		}
		//panic(fmt.Errorf("index nil: %s", item.decompressor.FileName()))
		return false
	}
	if l.Has(AccessorExistence) && item.existence == nil {
		if trace {
			log.Warn("[dbg] calcVisibleFiles: Existence not opened", "f", item.decompressor.FileName())
		}
		//panic(fmt.Errorf("existence nil: %s", item.decompressor.FileName()))
		return false
	}
	return true
}

// visibleFiles have no garbage (overlaps, unindexed, etc...)
type visibleFiles []visibleFile

// EndTxNum return txNum which not included in file - it will be first txNum in future file
func (files visibleFiles) EndTxNum() uint64 {
	if len(files) == 0 {
		return 0
	}
	return files[len(files)-1].endTxNum
}

func (files visibleFiles) LatestMergedRange() MergeRange {
	if len(files) == 0 {
		return MergeRange{}
	}
	for i := len(files) - 1; i >= 0; i-- {
		shardSize := (files[i].endTxNum - files[i].startTxNum) / config3.DefaultStepSize
		if shardSize > 2 {
			return MergeRange{from: files[i].startTxNum, to: files[i].endTxNum}
		}
	}
	return MergeRange{}
}
func (files visibleFiles) String(stepSize uint64) string {
	res := make([]string, 0, len(files))
	for _, file := range files {
		res = append(res, fmt.Sprintf("%d-%d", file.startTxNum/stepSize, file.endTxNum/stepSize))
	}
	return strings.Join(res, ",")
}

// fileItemsWithMissedAccessors returns list of files with missed accessors
// here "accessors" are generated dynamically by `accessorsFor`
func fileItemsWithMissedAccessors(dirtyFiles []*filesItem, aggregationStep uint64, accessorsFor func(fromStep, toStep uint64) []string) (l []*filesItem) {
	for _, item := range dirtyFiles {
		fromStep, toStep := item.startTxNum/aggregationStep, item.endTxNum/aggregationStep
		for _, fName := range accessorsFor(fromStep, toStep) {
			exists, err := dir.FileExist(fName)
			if err != nil {
				panic(err)
			}
			if !exists {
				l = append(l, item)
				break
			}
		}
	}
	return
}
