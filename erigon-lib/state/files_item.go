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
	"os"
	"sync/atomic"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv/bitmapdb"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
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
	bm                   *bitmapdb.FixedSizeBitmaps
	existence            *ExistenceFilter
	startTxNum, endTxNum uint64 //[startTxNum, endTxNum)

	// Frozen: file of size StepsInColdFile. Completely immutable.
	// Cold: file of size < StepsInColdFile. Immutable, but can be closed/removed after merge to bigger file.
	// Hot: Stored in DB. Providing Snapshot-Isolation by CopyOnWrite.
	frozen   bool         // immutable, don't need atomic
	refcount atomic.Int32 // only for `frozen=false`

	// file can be deleted in 2 cases: 1. when `refcount == 0 && canDelete == true` 2. on app startup when `file.isSubsetOfFrozenFile()`
	// other processes (which also reading files, may have same logic)
	canDelete atomic.Bool
}

func newFilesItem(startTxNum, endTxNum, stepSize uint64) *filesItem {
	startStep := startTxNum / stepSize
	endStep := endTxNum / stepSize
	frozen := endStep-startStep == StepsInColdFile
	return &filesItem{startTxNum: startTxNum, endTxNum: endTxNum, frozen: frozen}
}

// isSubsetOf - when `j` covers `i` but not equal `i`
func (i *filesItem) isSubsetOf(j *filesItem) bool {
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
	if i.bm != nil {
		i.bm.Close()
		i.bm = nil
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
	if i.bm != nil {
		i.bm.Close()
		if err := os.Remove(i.bm.FilePath()); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bm.FileName())
		}
		if err := os.Remove(i.bm.FilePath() + ".torrent"); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bm.FileName())
		}
		i.bm = nil
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

func (i *visibleFile) isSubSetOf(j *visibleFile) bool { return i.src.isSubsetOf(j.src) } //nolint
func (i *visibleFile) isSubsetOf(j *visibleFile) bool { return i.src.isSubsetOf(j.src) } //nolint

func calcVisibleFiles(files *btree2.BTreeG[*filesItem], l idxList, trace bool, toTxNum uint64) (roItems []visibleFile) {
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
			if (l&withBTree != 0) && item.bindex == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: BTindex not opened", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("btindex nil: %s", item.decompressor.FileName()))
				continue
			}
			if (l&withHashMap != 0) && item.index == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: RecSplit not opened", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("index nil: %s", item.decompressor.FileName()))
				continue
			}
			if (l&withExistence != 0) && item.existence == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: Existence not opened", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("existence nil: %s", item.decompressor.FileName()))
				continue
			}

			// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
			// see super-set file, just drop sub-set files from list
			for len(newVisibleFiles) > 0 && newVisibleFiles[len(newVisibleFiles)-1].src.isSubsetOf(item) {
				if trace {
					log.Warn("[dbg] calcVisibleFiles: marked as garbage (is subset)", "item", item.decompressor.FileName(),
						"of", newVisibleFiles[len(newVisibleFiles)-1].src.decompressor.FileName())
				}
				newVisibleFiles[len(newVisibleFiles)-1].src = nil
				newVisibleFiles = newVisibleFiles[:len(newVisibleFiles)-1]
			}

			log.Warn("willBeVisible", "newVisibleFile", item.decompressor.FileName())
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
		shardSize := (files[i].endTxNum - files[i].startTxNum) / config3.HistoryV3AggregationStep
		if shardSize > 2 {
			return MergeRange{from: files[i].startTxNum, to: files[i].endTxNum}
		}
	}
	return MergeRange{}
}

func (files visibleFiles) MergedRanges() []MergeRange {
	if len(files) == 0 {
		return nil
	}
	res := make([]MergeRange, len(files))
	for i := len(files) - 1; i >= 0; i-- {
		res[i] = MergeRange{from: files[i].startTxNum, to: files[i].endTxNum}
	}
	return res
}
