package state

import (
	"os"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/log/v3"
	btree2 "github.com/tidwall/btree"
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

// ctxItem - class is used for good/visible files
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
		}
		i.index = nil
	}
	if i.bindex != nil {
		i.bindex.Close()
		if err := os.Remove(i.bindex.FilePath()); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bindex.FileName())
		}
		i.bindex = nil
	}
	if i.bm != nil {
		i.bm.Close()
		if err := os.Remove(i.bm.FilePath()); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bm.FileName())
		}
		i.bm = nil
	}
	if i.existence != nil {
		i.existence.Close()
		if err := os.Remove(i.existence.FilePath); err != nil {
			log.Trace("remove after close", "err", err, "file", i.existence.FileName)
		}
		i.existence = nil
	}
}

// ctxItem is like filesItem but only for good/visible files (indexed, not overlaped, not marked for deletion, etc...)
// it's ok to store ctxItem in array
type ctxItem struct {
	getter     *seg.Getter
	reader     *recsplit.IndexReader
	startTxNum uint64
	endTxNum   uint64

	i   int
	src *filesItem
}

func (i *ctxItem) isSubSetOf(j *ctxItem) bool { return i.src.isSubsetOf(j.src) } //nolint
func (i *ctxItem) isSubsetOf(j *ctxItem) bool { return i.src.isSubsetOf(j.src) } //nolint

func calcVisibleFiles(files *btree2.BTreeG[*filesItem], l idxList, trace bool) (roItems []ctxItem) {
	visibleFiles := make([]ctxItem, 0, files.Len())
	if trace {
		log.Warn("[dbg] calcVisibleFiles", "amount", files.Len())
	}
	files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.canDelete.Load() {
				if trace {
					log.Warn("[dbg] calcVisibleFiles0", "f", item.decompressor.FileName())
				}
				continue
			}

			// TODO: need somehow handle this case, but indices do not open in tests TestFindMergeRangeCornerCases
			if item.decompressor == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles1", "from", item.startTxNum, "to", item.endTxNum)
				}
				continue
			}
			if (l&withBTree != 0) && item.bindex == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles2", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("btindex nil: %s", item.decompressor.FileName()))
				continue
			}
			if (l&withHashMap != 0) && item.index == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles3", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("index nil: %s", item.decompressor.FileName()))
				continue
			}
			if (l&withExistence != 0) && item.existence == nil {
				if trace {
					log.Warn("[dbg] calcVisibleFiles4", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("existence nil: %s", item.decompressor.FileName()))
				continue
			}

			// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
			// see super-set file, just drop sub-set files from list
			for len(visibleFiles) > 0 && visibleFiles[len(visibleFiles)-1].src.isSubsetOf(item) {
				if trace {
					log.Warn("[dbg] calcVisibleFiles5", "f", visibleFiles[len(visibleFiles)-1].src.decompressor.FileName())
				}
				visibleFiles[len(visibleFiles)-1].src = nil
				visibleFiles = visibleFiles[:len(visibleFiles)-1]
			}
			visibleFiles = append(visibleFiles, ctxItem{
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				i:          len(visibleFiles),
				src:        item,
			})
		}
		return true
	})
	if visibleFiles == nil {
		visibleFiles = []ctxItem{}
	}
	return visibleFiles
}
