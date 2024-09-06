// Copyright 2022 The Erigon Authors
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
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/metrics"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

// StepsInColdFile - files of this size are completely frozen/immutable.
// files of smaller size are also immutable, but can be removed after merge to bigger files.
const StepsInColdFile = 64

var (
	asserts          = dbg.EnvBool("AGG_ASSERTS", false)
	traceFileLife    = dbg.EnvString("AGG_TRACE_FILE_LIFE", "")
	traceGetAsOf     = dbg.EnvString("AGG_TRACE_GET_AS_OF", "")
	tracePutWithPrev = dbg.EnvString("AGG_TRACE_PUT_WITH_PREV", "")
)
var traceGetLatest, _ = kv.String2Domain(dbg.EnvString("AGG_TRACE_GET_LATEST", ""))

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
//
// Data-Existence in .kv vs .v files:
//  1. key doesn’t exists, then create: .kv - yes, .v - yes
//  2. acc exists, then update/delete:  .kv - yes, .v - yes
//  3. acc doesn’t exists, then delete: .kv - no,  .v - no
type Domain struct {
	*History

	name kv.Domain

	// dirtyFiles - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in Aggregator
	//
	// `_visible.files` derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// BeginRo() using _visible in zero-copy way
	dirtyFiles *btree2.BTreeG[*filesItem]

	// _visible - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visible *domainVisible

	integrityCheck func(name kv.Domain, fromStep, toStep uint64) bool

	// replaceKeysInValues allows to replace commitment branch values with shorter keys.
	// for commitment domain only
	replaceKeysInValues bool
	// restricts subset file deletions on open/close. Needed to hold files until commitment is merged
	restrictSubsetFileDeletions bool
	largeVals                   bool

	compressCfg seg.Cfg
	compression seg.FileCompression

	valsTable string // key -> inverted_step + values (Dupsort)
	stats     DomainStats
	indexList idxList
}

type domainCfg struct {
	hist     histCfg
	compress seg.FileCompression

	largeVals                   bool
	replaceKeysInValues         bool
	restrictSubsetFileDeletions bool
}

type domainVisible struct {
	files  []visibleFile
	name   kv.Domain
	caches *sync.Pool
}

var DomainCompressCfg = seg.Cfg{
	MinPatternScore:      1000,
	DictReducerSoftLimit: 2000000,
	MinPatternLen:        20,
	MaxPatternLen:        32,
	SamplingFactor:       4,
	MaxDictPatterns:      64 * 1024 * 2,
	Workers:              1,
}

func NewDomain(cfg domainCfg, aggregationStep uint64, name kv.Domain, valsTable, indexKeysTable, historyValsTable, indexTable string, integrityCheck func(name kv.Domain, fromStep, toStep uint64) bool, logger log.Logger) (*Domain, error) {
	if cfg.hist.iiCfg.dirs.SnapDomain == "" {
		panic("empty `dirs` variable")
	}

	d := &Domain{
		name:      name,
		valsTable: valsTable,

		compressCfg: DomainCompressCfg,
		compression: cfg.compress,

		dirtyFiles: btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		stats:      DomainStats{FilesQueries: &atomic.Uint64{}, TotalQueries: &atomic.Uint64{}},

		indexList:                   withBTree | withExistence,
		replaceKeysInValues:         cfg.replaceKeysInValues,         // for commitment domain only
		restrictSubsetFileDeletions: cfg.restrictSubsetFileDeletions, // to prevent not merged 'garbage' to delete on start
		largeVals:                   cfg.largeVals,
		integrityCheck:              integrityCheck,
	}

	d._visible = newDomainVisible(d.name, []visibleFile{})

	var err error
	if d.History, err = NewHistory(cfg.hist, aggregationStep, name.String(), indexKeysTable, indexTable, historyValsTable, nil, logger); err != nil {
		return nil, err
	}

	return d, nil
}
func (d *Domain) kvFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("v1-%s.%d-%d.kv", d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("v1-%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvExistenceIdxFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("v1-%s.%d-%d.kvei", d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvBtFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("v1-%s.%d-%d.bt", d.filenameBase, fromStep, toStep))
}

// maxStepInDB - return the latest available step in db (at-least 1 value in such step)
func (d *Domain) maxStepInDB(tx kv.Tx) (lstInDb uint64) {
	lstIdx, _ := kv.LastKey(tx, d.History.indexKeysTable)
	if len(lstIdx) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(lstIdx) / d.aggregationStep
}
func (d *Domain) minStepInDB(tx kv.Tx) (lstInDb uint64) {
	lstIdx, _ := kv.FirstKey(tx, d.History.indexKeysTable)
	if len(lstIdx) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(lstIdx) / d.aggregationStep
}

func (dt *DomainRoTx) NewWriter() *domainBufferedWriter { return dt.newWriter(dt.d.dirs.Tmp, false) }

// OpenList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (d *Domain) OpenList(idxFiles, histFiles, domainFiles []string) error {
	if err := d.History.openList(idxFiles, histFiles); err != nil {
		return err
	}

	d.closeWhatNotInList(domainFiles)
	d.scanDirtyFiles(domainFiles)
	if err := d.openDirtyFiles(); err != nil {
		return fmt.Errorf("Domain(%s).openList: %w", d.filenameBase, err)
	}
	d.protectFromHistoryFilesAheadOfDomainFiles()
	return nil
}

// protectFromHistoryFilesAheadOfDomainFiles - in some corner-cases app may see more .ef/.v files than .kv:
//   - `kill -9` in the middle of `buildFiles()`, then `rm -f db` (restore from backup)
//   - `kill -9` in the middle of `buildFiles()`, then `stage_exec --reset` (drop progress - as a hot-fix)
func (d *Domain) protectFromHistoryFilesAheadOfDomainFiles() {
	d.closeFilesAfterStep(d.dirtyFilesEndTxNumMinimax() / d.aggregationStep)
}

func (d *Domain) openFolder() error {
	idx, histFiles, domainFiles, err := d.fileNamesOnDisk()
	if err != nil {
		return fmt.Errorf("Domain(%s).openFolder: %w", d.filenameBase, err)
	}
	if err := d.OpenList(idx, histFiles, domainFiles); err != nil {
		return err
	}
	return nil
}

func (d *Domain) GetAndResetStats() DomainStats {
	r := d.stats
	r.DataSize, r.IndexSize, r.FilesCount = d.collectFilesStats()

	d.stats = DomainStats{FilesQueries: &atomic.Uint64{}, TotalQueries: &atomic.Uint64{}}
	return r
}

func (d *Domain) closeFilesAfterStep(lowerBound uint64) {
	var toClose []*filesItem
	d.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		d.dirtyFiles.Delete(item)
		fName := ""
		if item.decompressor != nil {
			fName = item.decompressor.FileName()
		}
		log.Debug(fmt.Sprintf("[snapshots] closing %s, because step %d was not complete", fName, lowerBound))
		item.closeFiles()
	}

	toClose = toClose[:0]
	d.History.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		d.History.dirtyFiles.Delete(item)
		fName := ""
		if item.decompressor != nil {
			fName = item.decompressor.FileName()
		}
		log.Debug(fmt.Sprintf("[snapshots] closing %s, because step %d was not complete", fName, lowerBound))
		item.closeFiles()
	}

	toClose = toClose[:0]
	d.History.InvertedIndex.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		d.History.InvertedIndex.dirtyFiles.Delete(item)
		fName := ""
		if item.decompressor != nil {
			fName = item.decompressor.FileName()
		}
		log.Debug(fmt.Sprintf("[snapshots] closing %s, because step %d was not complete", fName, lowerBound))
		item.closeFiles()
	}
}

func (d *Domain) scanDirtyFiles(fileNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^v([0-9]+)-" + d.filenameBase + ".([0-9]+)-([0-9]+).kv$")
	var err error

	for _, name := range fileNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				d.logger.Warn("File ignored by domain scan, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			d.logger.Warn("File ignored by domain scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			d.logger.Warn("File ignored by domain scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			d.logger.Warn("File ignored by domain scan, startTxNum > endTxNum", "name", name)
			continue
		}

		domainName, _ := kv.String2Domain(d.filenameBase)
		if d.integrityCheck != nil && !d.integrityCheck(domainName, startStep, endStep) {
			d.logger.Debug("[agg] skip garbage file", "name", name)
			continue
		}

		// Semantic: [startTxNum, endTxNum)
		// Example:
		//   stepSize = 4
		//   0-1.kv: [0, 8)
		//   0-2.kv: [0, 16)
		//   1-2.kv: [8, 16)
		startTxNum, endTxNum := startStep*d.aggregationStep, endStep*d.aggregationStep

		var newFile = newFilesItem(startTxNum, endTxNum, d.aggregationStep)
		newFile.frozen = false

		if _, has := d.dirtyFiles.Get(newFile); has {
			continue
		}
		d.dirtyFiles.Set(newFile)
	}
	return garbageFiles
}

func (d *Domain) openDirtyFiles() (err error) {
	invalidFileItems := make([]*filesItem, 0)
	invalidFileItemsLock := sync.Mutex{}
	d.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			if item.decompressor == nil {
				fPath := d.kvFilePath(fromStep, toStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					d.logger.Debug("[agg] Domain.openDirtyFiles: FileExist err", "f", fName, "err", err)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}
				if !exists {
					_, fName := filepath.Split(fPath)
					d.logger.Debug("[agg] Domain.openDirtyFiles: file does not exists", "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						d.logger.Debug("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
					} else {
						d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
					}
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil && !UseBpsTree {
				fPath := d.kvAccessorFilePath(fromStep, toStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
				}
				if exists {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
			if item.bindex == nil {
				fPath := d.kvBtFilePath(fromStep, toStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
				}
				if exists {
					if item.bindex, err = OpenBtreeIndexWithDecompressor(fPath, DefaultBtreeM, item.decompressor, d.compression); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
			if item.existence == nil {
				fPath := d.kvExistenceIdxFilePath(fromStep, toStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
				}
				if exists {
					if item.existence, err = OpenExistenceFilter(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
		}
		return true
	})

	for _, item := range invalidFileItems {
		item.closeFiles() // just close, not remove from disk
		d.dirtyFiles.Delete(item)
	}

	return nil
}

func (d *Domain) closeWhatNotInList(fNames []string) {
	var toClose []*filesItem
	d.dirtyFiles.Walk(func(items []*filesItem) bool {
	Loop1:
		for _, item := range items {
			for _, protectName := range fNames {
				if item.decompressor != nil && item.decompressor.FileName() == protectName {
					continue Loop1
				}
			}
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		item.closeFiles()
		d.dirtyFiles.Delete(item)
	}
}

func (d *Domain) reCalcVisibleFiles(toTxNum uint64) {
	d._visible = newDomainVisible(d.name, calcVisibleFiles(d.dirtyFiles, d.indexList, false, toTxNum))
	d.History.reCalcVisibleFiles(toTxNum)
}

func (d *Domain) Close() {
	if d == nil {
		return
	}
	d.History.Close()
	d.closeWhatNotInList([]string{})
}

func (w *domainBufferedWriter) PutWithPrev(key1, key2, val, preval []byte, prevStep uint64) error {
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `preval`` slice is invalidated
	if tracePutWithPrev != "" && tracePutWithPrev == w.h.ii.filenameBase {
		fmt.Printf("PutWithPrev(%s, txn %d, key[%x][%x] value[%x] preval[%x])\n", w.h.ii.filenameBase, w.h.ii.txNum, key1, key2, val, preval)
	}
	if err := w.h.AddPrevValue(key1, key2, preval, prevStep); err != nil {
		return err
	}
	if w.diff != nil {
		w.diff.DomainUpdate(key1, key2, preval, w.stepBytes[:], prevStep)
	}
	return w.addValue(key1, key2, val)
}

func (w *domainBufferedWriter) DeleteWithPrev(key1, key2, prev []byte, prevStep uint64) (err error) {
	// This call to update needs to happen before d.tx.Delete() later, because otherwise the content of `original`` slice is invalidated
	if tracePutWithPrev != "" && tracePutWithPrev == w.h.ii.filenameBase {
		fmt.Printf("DeleteWithPrev(%s, txn %d, key[%x][%x] preval[%x])\n", w.h.ii.filenameBase, w.h.ii.txNum, key1, key2, prev)
	}
	if err := w.h.AddPrevValue(key1, key2, prev, prevStep); err != nil {
		return err
	}
	if w.diff != nil {
		w.diff.DomainUpdate(key1, key2, prev, w.stepBytes[:], prevStep)
	}
	return w.addValue(key1, key2, nil)
}

func (w *domainBufferedWriter) SetTxNum(v uint64) {
	w.setTxNumOnce = true
	w.h.SetTxNum(v)
	binary.BigEndian.PutUint64(w.stepBytes[:], ^(v / w.h.ii.aggregationStep))
}

func (dt *DomainRoTx) newWriter(tmpdir string, discard bool) *domainBufferedWriter {
	discardHistory := discard || dt.d.historyDisabled

	w := &domainBufferedWriter{
		discard:   discard,
		aux:       make([]byte, 0, 128),
		valsTable: dt.d.valsTable,
		largeVals: dt.d.largeVals,
		values:    etl.NewCollector(dt.name.String()+"domain.flush", tmpdir, etl.NewSortableBuffer(WALCollectorRAM), dt.d.logger).LogLvl(log.LvlTrace),

		h: dt.ht.newWriter(tmpdir, discardHistory),
	}
	w.values.SortAndFlushInBackground(true)
	return w
}

type domainBufferedWriter struct {
	values *etl.Collector

	setTxNumOnce bool
	discard      bool

	valsTable string
	largeVals bool

	stepBytes [8]byte // current inverted step representation
	aux       []byte  // auxilary buffer for key1 + key2
	aux2      []byte  // auxilary buffer for step + val
	diff      *StateDiffDomain

	h *historyBufferedWriter
}

func (w *domainBufferedWriter) close() {
	if w == nil { // allow dobule-close
		return
	}
	w.h.close()
	if w.values != nil {
		w.values.Close()
	}
}

// nolint
func loadSkipFunc() etl.LoadFunc {
	var preKey, preVal []byte
	return func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if bytes.Equal(k, preKey) {
			preVal = v
			return nil
		}
		if err := next(nil, preKey, preVal); err != nil {
			return err
		}
		if err := next(k, k, v); err != nil {
			return err
		}
		preKey, preVal = k, v
		return nil
	}
}
func (w *domainBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}
	if err := w.h.Flush(ctx, tx); err != nil {
		return err
	}

	if w.largeVals {
		if err := w.values.Load(tx, w.valsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done(), EmptyVals: true}); err != nil {
			return err
		}
		w.close()
		return nil
	}

	valuesCursor, err := tx.RwCursorDupSort(w.valsTable)
	if err != nil {
		return err
	}
	defer valuesCursor.Close()
	if err := w.values.Load(tx, w.valsTable, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		foundVal, err := valuesCursor.SeekBothRange(k, v[:8])
		if err != nil {
			return err
		}
		if len(foundVal) == 0 || !bytes.Equal(foundVal[:8], v[:8]) {
			if err := valuesCursor.Put(k, v); err != nil {
				return err
			}
			return nil
		}
		if err := valuesCursor.DeleteCurrent(); err != nil {
			return err
		}
		if err := valuesCursor.Put(k, v); err != nil {
			return err
		}
		return nil
	}, etl.TransformArgs{Quit: ctx.Done(), EmptyVals: true}); err != nil {
		return err
	}
	w.close()

	return nil
}

func (w *domainBufferedWriter) addValue(key1, key2, value []byte) error {
	if w.discard {
		return nil
	}
	if !w.setTxNumOnce {
		panic("you forgot to call SetTxNum")
	}
	if w.largeVals {
		kl := len(key1) + len(key2)
		w.aux = append(append(append(w.aux[:0], key1...), key2...), w.stepBytes[:]...)
		fullkey := w.aux[:kl+8]
		if asserts && (w.h.ii.txNum/w.h.ii.aggregationStep) != ^binary.BigEndian.Uint64(w.stepBytes[:]) {
			panic(fmt.Sprintf("assert: %d != %d", w.h.ii.txNum/w.h.ii.aggregationStep, ^binary.BigEndian.Uint64(w.stepBytes[:])))
		}

		if err := w.values.Collect(fullkey, value); err != nil {
			return err
		}
		return nil
	}

	w.aux = append(append(w.aux[:0], key1...), key2...)
	w.aux2 = append(append(w.aux2[:0], w.stepBytes[:]...), value...)

	if asserts && (w.h.ii.txNum/w.h.ii.aggregationStep) != ^binary.BigEndian.Uint64(w.stepBytes[:]) {
		panic(fmt.Sprintf("assert: %d != %d", w.h.ii.txNum/w.h.ii.aggregationStep, ^binary.BigEndian.Uint64(w.stepBytes[:])))
	}

	//defer func() {
	//	fmt.Printf("addValue     [%p;tx=%d] '%x' -> '%x'\n", w, w.h.ii.txNum, fullkey, value)
	//}()

	if err := w.values.Collect(w.aux, w.aux2); err != nil {
		return err
	}
	return nil
}

type CursorType uint8

const (
	FILE_CURSOR CursorType = iota
	DB_CURSOR
	RAM_CURSOR
)

// CursorItem is the item in the priority queue used to do merge interation
// over storage of a given account
type CursorItem struct {
	cDup    kv.CursorDupSort
	cNonDup kv.Cursor

	iter         btree2.MapIter[string, dataWithPrevStep]
	dg           *seg.Reader
	dg2          *seg.Reader
	btCursor     *Cursor
	key          []byte
	val          []byte
	step         uint64
	startTxNum   uint64
	endTxNum     uint64
	latestOffset uint64     // offset of the latest value in the file
	t            CursorType // Whether this item represents state file or DB record, or tree
	reverse      bool
}

type CursorHeap []*CursorItem

func (ch CursorHeap) Len() int {
	return len(ch)
}

func (ch CursorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(ch[i].key, ch[j].key)
	if cmp == 0 {
		// when keys match, the items with later blocks are preferred
		if ch[i].reverse {
			return ch[i].endTxNum > ch[j].endTxNum
		}
		return ch[i].endTxNum < ch[j].endTxNum
	}
	return cmp < 0
}

func (ch *CursorHeap) Swap(i, j int) {
	(*ch)[i], (*ch)[j] = (*ch)[j], (*ch)[i]
}

func (ch *CursorHeap) Push(x interface{}) {
	*ch = append(*ch, x.(*CursorItem))
}

func (ch *CursorHeap) Pop() interface{} {
	old := *ch
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*ch = old[0 : n-1]
	return x
}

// DomainRoTx allows accesing the same domain from multiple go-routines
type DomainRoTx struct {
	files   visibleFiles
	visible *domainVisible
	name    kv.Domain
	ht      *HistoryRoTx

	d *Domain

	getters    []*seg.Reader
	readers    []*BtIndex
	idxReaders []*recsplit.IndexReader

	keyBuf [60]byte // 52b key and 8b for inverted step
	comBuf []byte

	valsC kv.Cursor

	getFromFileCache *DomainGetFromFileCache
}

func domainReadMetric(name kv.Domain, level int) metrics.Summary {
	if level > 4 {
		level = 5
	}
	return mxsKVGet[name][level]
}

func (dt *DomainRoTx) getFromFile(i int, filekey []byte) ([]byte, bool, error) {
	if dbg.KVReadLevelledMetrics {
		defer domainReadMetric(dt.name, i).ObserveDuration(time.Now())
	}

	g := dt.statelessGetter(i)
	if !(UseBtree || UseBpsTree) {
		reader := dt.statelessIdxReader(i)
		if reader.Empty() {
			return nil, false, nil
		}
		offset, ok := reader.Lookup(filekey)
		if !ok {
			return nil, false, nil
		}
		g.Reset(offset)

		k, _ := g.Next(nil)
		if !bytes.Equal(filekey, k) {
			return nil, false, nil
		}
		v, _ := g.Next(nil)
		return v, true, nil
	}

	_, v, _, ok, err := dt.statelessBtree(i).Get(filekey, g)
	if err != nil || !ok {
		return nil, false, err
	}
	//fmt.Printf("getLatestFromBtreeColdFiles key %x shard %d %x\n", filekey, exactColdShard, v)
	return v, true, nil
}

func (dt *DomainRoTx) DebugKVFilesWithKey(k []byte) (res []string, err error) {
	for i := len(dt.files) - 1; i >= 0; i-- {
		_, ok, err := dt.getFromFile(i, k)
		if err != nil {
			return res, err
		}
		if ok {
			res = append(res, dt.files[i].src.decompressor.FileName())
		}
	}
	return res, nil
}
func (dt *DomainRoTx) DebugEFKey(k []byte) error {
	dt.ht.iit.ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor == nil {
				continue
			}
			accessor := item.index
			if accessor == nil {
				fPath := dt.d.efAccessorFilePath(item.startTxNum/dt.d.aggregationStep, item.endTxNum/dt.d.aggregationStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					dt.d.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
					continue
				}
				if exists {
					var err error
					accessor, err = recsplit.OpenIndex(fPath)
					if err != nil {
						_, fName := filepath.Split(fPath)
						dt.d.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
						continue
					}
					defer accessor.Close()
				} else {
					continue
				}
			}

			offset, ok := accessor.GetReaderFromPool().Lookup(k)
			if !ok {
				continue
			}
			g := item.decompressor.MakeGetter()
			g.Reset(offset)
			key, _ := g.NextUncompressed()
			if !bytes.Equal(k, key) {
				continue
			}
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)

			last2 := uint64(0)
			if ef.Count() > 2 {
				last2 = ef.Get(ef.Count() - 2)
			}
			log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d\n", item.decompressor.FileName(), ef.Min(), ef.Max(), last2, stream.ToArrU64Must(ef.Iterator())))
		}
		return true
	})
	return nil
}

func (d *Domain) collectFilesStats() (datsz, idxsz, files uint64) {
	d.History.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil {
				return false
			}
			datsz += uint64(item.decompressor.Size())
			idxsz += uint64(item.index.Size())
			idxsz += uint64(item.bindex.Size())
			files += 3
		}
		return true
	})

	d.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil {
				return false
			}
			datsz += uint64(item.decompressor.Size())
			idxsz += uint64(item.index.Size())
			idxsz += uint64(item.bindex.Size())
			files += 3
		}
		return true
	})

	fcnt, fsz, isz := d.History.InvertedIndex.collectFilesStat()
	datsz += fsz
	files += fcnt
	idxsz += isz
	return
}

func (d *Domain) BeginFilesRo() *DomainRoTx {
	files := d._visible.files
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}

	return &DomainRoTx{
		name:    d.name,
		d:       d,
		ht:      d.History.BeginFilesRo(),
		visible: d._visible,
		files:   d._visible.files,
	}
}

// Collation is the set of compressors created after aggregation
type Collation struct {
	HistoryCollation
	valuesComp  *seg.Compressor
	valuesPath  string
	valuesCount int
}

func (c Collation) Close() {
	if c.valuesComp != nil {
		c.valuesComp.Close()
	}
	c.HistoryCollation.Close()
}

// collate gathers domain changes over the specified step, using read-only transaction,
// and returns compressors, elias fano, and bitmaps
// [txFrom; txTo)
func (d *Domain) collate(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx) (coll Collation, err error) {
	{ //assert
		if txFrom%d.aggregationStep != 0 {
			panic(fmt.Errorf("assert: unexpected txFrom=%d", txFrom))
		}
		if txTo%d.aggregationStep != 0 {
			panic(fmt.Errorf("assert: unexpected txTo=%d", txTo))
		}
	}

	started := time.Now()
	defer func() {
		d.stats.LastCollationTook = time.Since(started)
		mxCollateTook.ObserveDuration(started)
	}()

	coll.HistoryCollation, err = d.History.collate(ctx, step, txFrom, txTo, roTx)
	if err != nil {
		return Collation{}, err
	}

	closeCollation := true
	defer func() {
		if closeCollation {
			coll.Close()
		}
	}()

	coll.valuesPath = d.kvFilePath(step, step+1)
	if coll.valuesComp, err = seg.NewCompressor(ctx, d.filenameBase+".domain.collate", coll.valuesPath, d.dirs.Tmp, d.compressCfg, log.LvlTrace, d.logger); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}

	// Don't use `d.compress` config in collate. Because collat+build must be very-very fast (to keep db small).
	// Compress files only in `merge` which ok to be slow.
	comp := seg.NewWriter(coll.valuesComp, seg.CompressNone)

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^step)

	var valsCursor kv.Cursor

	if d.largeVals {
		valsCursor, err = roTx.Cursor(d.valsTable)
		if err != nil {
			return Collation{}, fmt.Errorf("create %s values cursorDupsort: %w", d.filenameBase, err)
		}
	} else {
		valsCursor, err = roTx.CursorDupSort(d.valsTable)
		if err != nil {
			return Collation{}, fmt.Errorf("create %s values cursorDupsort: %w", d.filenameBase, err)
		}
	}
	defer valsCursor.Close()

	kvs := make([]struct {
		k, v []byte
	}, 0, 128)

	var stepInDB []byte
	for k, v, err := valsCursor.First(); k != nil; {
		if err != nil {
			return coll, err
		}

		if d.largeVals {
			stepInDB = k[len(k)-8:]
		} else {
			stepInDB = v[:8]
		}
		if !bytes.Equal(stepBytes, stepInDB) { // [txFrom; txTo)
			k, v, err = valsCursor.Next()
			continue
		}

		if d.largeVals {
			kvs = append(kvs, struct {
				k, v []byte
			}{k[:len(k)-8], v})
			k, v, err = valsCursor.Next()
		} else {
			if err = comp.AddWord(k); err != nil {
				return coll, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, k, err)
			}
			if err = comp.AddWord(v[8:]); err != nil {
				return coll, fmt.Errorf("add %s values [%x]=>[%x]: %w", d.filenameBase, k, v[8:], err)
			}
			k, v, err = valsCursor.(kv.CursorDupSort).NextNoDup()
		}
	}

	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].k, kvs[j].k) < 0
	})
	// check if any key is duplicated
	for i := 1; i < len(kvs); i++ {
		if bytes.Equal(kvs[i].k, kvs[i-1].k) {
			return coll, fmt.Errorf("duplicate key [%x]", kvs[i].k)
		}
	}
	for _, kv := range kvs {
		if err = comp.AddWord(kv.k); err != nil {
			return coll, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, kv.k, err)
		}
		if err = comp.AddWord(kv.v); err != nil {
			return coll, fmt.Errorf("add %s values [%x]=>[%x]: %w", d.filenameBase, kv.k, kv.v, err)
		}
	}

	closeCollation = false
	coll.valuesCount = coll.valuesComp.Count() / 2
	mxCollationSize.SetUint64(uint64(coll.valuesCount))
	return coll, nil
}

type StaticFiles struct {
	HistoryFiles
	valuesDecomp *seg.Decompressor
	valuesIdx    *recsplit.Index
	valuesBt     *BtIndex
	bloom        *ExistenceFilter
}

// CleanupOnError - call it on collation fail. It closing all files
func (sf StaticFiles) CleanupOnError() {
	if sf.valuesDecomp != nil {
		sf.valuesDecomp.Close()
	}
	if sf.valuesIdx != nil {
		sf.valuesIdx.Close()
	}
	if sf.valuesBt != nil {
		sf.valuesBt.Close()
	}
	if sf.bloom != nil {
		sf.bloom.Close()
	}
	sf.HistoryFiles.CleanupOnError()
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (d *Domain) buildFiles(ctx context.Context, step uint64, collation Collation, ps *background.ProgressSet) (StaticFiles, error) {
	mxRunningFilesBuilding.Inc()
	defer mxRunningFilesBuilding.Dec()
	if traceFileLife != "" && d.filenameBase == traceFileLife {
		d.logger.Warn("[agg.dbg] buildFiles", "step", step, "domain", d.filenameBase)
	}

	start := time.Now()
	defer func() {
		d.stats.LastFileBuildingTook = time.Since(start)
		mxBuildTook.ObserveDuration(start)
	}()

	hStaticFiles, err := d.History.buildFiles(ctx, step, collation.HistoryCollation, ps)
	if err != nil {
		return StaticFiles{}, err
	}
	valuesComp := collation.valuesComp

	var (
		valuesDecomp *seg.Decompressor
		valuesIdx    *recsplit.Index
		bt           *BtIndex
		bloom        *ExistenceFilter
	)
	closeComp := true
	defer func() {
		if closeComp {
			hStaticFiles.CleanupOnError()
			if valuesComp != nil {
				valuesComp.Close()
			}
			if valuesDecomp != nil {
				valuesDecomp.Close()
			}
			if valuesIdx != nil {
				valuesIdx.Close()
			}
			if bt != nil {
				bt.Close()
			}
			if bloom != nil {
				bloom.Close()
			}
		}
	}()
	if d.noFsync {
		valuesComp.DisableFsync()
	}
	if err = valuesComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s values: %w", d.filenameBase, err)
	}
	valuesComp.Close()
	valuesComp = nil
	if valuesDecomp, err = seg.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}

	if !UseBpsTree {
		if err = d.buildAccessor(ctx, step, step+1, valuesDecomp, ps); err != nil {
			return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
		}
		valuesIdx, err = recsplit.OpenIndex(d.efAccessorFilePath(step, step+1))
		if err != nil {
			return StaticFiles{}, err
		}
	}

	{
		btPath := d.kvBtFilePath(step, step+1)
		bt, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesDecomp, d.compression, *d.salt, ps, d.dirs.Tmp, d.logger, d.noFsync)
		if err != nil {
			return StaticFiles{}, fmt.Errorf("build %s .bt idx: %w", d.filenameBase, err)
		}
	}
	{
		fPath := d.kvExistenceIdxFilePath(step, step+1)
		exists, err := dir.FileExist(fPath)
		if err != nil {
			return StaticFiles{}, fmt.Errorf("build %s .kvei: %w", d.filenameBase, err)
		}
		if exists {
			bloom, err = OpenExistenceFilter(fPath)
			if err != nil {
				return StaticFiles{}, fmt.Errorf("build %s .kvei: %w", d.filenameBase, err)
			}
		}
	}
	closeComp = false
	return StaticFiles{
		HistoryFiles: hStaticFiles,
		valuesDecomp: valuesDecomp,
		valuesIdx:    valuesIdx,
		valuesBt:     bt,
		bloom:        bloom,
	}, nil
}

func (d *Domain) buildAccessor(ctx context.Context, fromStep, toStep uint64, data *seg.Decompressor, ps *background.ProgressSet) error {
	idxPath := d.kvAccessorFilePath(fromStep, toStep)
	cfg := recsplit.RecSplitArgs{
		Enums:              false,
		LessFalsePositives: false,

		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     d.dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       d.salt,
		NoFsync:    d.noFsync,
	}
	return buildAccessor(ctx, data, d.compression, idxPath, false, cfg, ps, d.logger)
}

func (d *Domain) missedBtreeAccessors() (l []*filesItem) {
	d.dirtyFiles.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			fPath := d.kvBtFilePath(fromStep, toStep)
			exists, err := dir.FileExist(fPath)
			if err != nil {
				panic(err)
			}
			if !exists {
				l = append(l, item)
				continue
			}
			fPath = d.kvExistenceIdxFilePath(fromStep, toStep)
			exists, err = dir.FileExist(fPath)
			if err != nil {
				panic(err)
			}
			if !exists {
				l = append(l, item)
				continue
			}
		}
		return true
	})
	return l
}
func (d *Domain) missedAccessors() (l []*filesItem) {
	d.dirtyFiles.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			fPath := d.kvAccessorFilePath(fromStep, toStep)
			exists, err := dir.FileExist(fPath)
			if err != nil {
				panic(err)
			}
			if !exists {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

//func (d *Domain) missedExistenceFilter() (l []*filesItem) {
//	d.files.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
//		for _, item := range items {
//			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
//      bloomPath := d.kvExistenceIdxFilePath(fromStep, toStep)
//      if !dir.FileExist(bloomPath) {
//				l = append(l, item)
//			}
//		}
//		return true
//	})
//	return l
//}

// BuildMissedAccessors - produce .efi/.vi/.kvi from .ef/.v/.kv
func (d *Domain) BuildMissedAccessors(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	d.History.BuildMissedAccessors(ctx, g, ps)
	for _, item := range d.missedBtreeAccessors() {
		if !UseBpsTree {
			continue
		}
		if item.decompressor == nil {
			log.Warn(fmt.Sprintf("[dbg] BuildMissedAccessors: item with nil decompressor %s %d-%d", d.filenameBase, item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep))
		}
		item := item

		g.Go(func() error {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			idxPath := d.kvBtFilePath(fromStep, toStep)
			if err := BuildBtreeIndexWithDecompressor(idxPath, item.decompressor, d.compression, ps, d.dirs.Tmp, *d.salt, d.logger, d.noFsync); err != nil {
				return fmt.Errorf("failed to build btree index for %s:  %w", item.decompressor.FileName(), err)
			}
			return nil
		})
	}
	for _, item := range d.missedAccessors() {
		if UseBpsTree {
			continue
		}
		if item.decompressor == nil {
			log.Warn(fmt.Sprintf("[dbg] BuildMissedAccessors: item with nil decompressor %s %d-%d", d.filenameBase, item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep))
		}
		item := item
		g.Go(func() error {
			if UseBpsTree {
				return nil
			}

			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			err := d.buildAccessor(ctx, fromStep, toStep, item.decompressor, ps)
			if err != nil {
				return fmt.Errorf("build %s values recsplit index: %w", d.filenameBase, err)
			}
			return nil
		})
	}
}

func buildAccessor(ctx context.Context, d *seg.Decompressor, compressed seg.FileCompression, idxPath string, values bool, cfg recsplit.RecSplitArgs, ps *background.ProgressSet, logger log.Logger) error {
	_, fileName := filepath.Split(idxPath)
	count := d.Count()
	if !values {
		count = d.Count() / 2
	}
	p := ps.AddNew(fileName, uint64(count))
	defer ps.Delete(p)

	defer d.EnableMadvNormal().DisableReadAhead()

	g := seg.NewReader(d.MakeGetter(), compressed)
	var rs *recsplit.RecSplit
	var err error
	cfg.KeyCount = count
	if rs, err = recsplit.NewRecSplit(cfg, logger); err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	var keyPos, valPos uint64
	for {
		word := make([]byte, 0, 256)
		if err := ctx.Err(); err != nil {
			return err
		}
		g.Reset(0)
		for g.HasNext() {
			word, valPos = g.Next(word[:0])
			if values {
				if err = rs.AddKey(word, valPos); err != nil {
					return fmt.Errorf("add idx key [%x]: %w", word, err)
				}
			} else {
				if err = rs.AddKey(word, keyPos); err != nil {
					return fmt.Errorf("add idx key [%x]: %w", word, err)
				}
			}

			// Skip value
			keyPos, _ = g.Skip()

			p.Processed.Add(1)
		}
		if err = rs.Build(ctx); err != nil {
			if rs.Collision() {
				logger.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	return nil
}

func (d *Domain) integrateDirtyFiles(sf StaticFiles, txNumFrom, txNumTo uint64) {
	d.History.integrateDirtyFiles(sf.HistoryFiles, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, d.aggregationStep)
	fi.frozen = false
	fi.decompressor = sf.valuesDecomp
	fi.index = sf.valuesIdx
	fi.bindex = sf.valuesBt
	fi.existence = sf.bloom
	d.dirtyFiles.Set(fi)
}

// unwind is similar to prune but the difference is that it restores domain values from the history as of txFrom
// context Flush should be managed by caller.
func (dt *DomainRoTx) Unwind(ctx context.Context, rwTx kv.RwTx, step, txNumUnwindTo uint64, domainDiffs []DomainEntryDiff) error {
	// fmt.Printf("[domain][%s] unwinding domain to txNum=%d, step %d\n", d.filenameBase, txNumUnwindTo, step)
	d := dt.d

	sf := time.Now()
	defer mxUnwindTook.ObserveDuration(sf)
	mxRunningUnwind.Inc()
	defer mxRunningUnwind.Dec()
	logEvery := time.NewTicker(time.Second * 30)
	defer logEvery.Stop()

	valsCursor, err := rwTx.RwCursorDupSort(d.valsTable)
	if err != nil {
		return err
	}
	defer valsCursor.Close()
	// First revert keys
	for i := range domainDiffs {
		key, value, prevStepBytes := domainDiffs[i].Key, domainDiffs[i].Value, domainDiffs[i].PrevStepBytes
		if dt.d.largeVals {
			if len(value) == 0 {
				if !bytes.Equal(key[len(key)-8:], prevStepBytes) {
					if err := rwTx.Delete(d.valsTable, key); err != nil {
						return err
					}
				} else {
					if err := rwTx.Put(d.valsTable, key, []byte{}); err != nil {
						return err
					}
				}
			} else {
				if err := rwTx.Put(d.valsTable, key, value); err != nil {
					return err
				}
			}
			continue
		}
		stepBytes := key[len(key)-8:]
		fullKey := key[:len(key)-8]
		// Second, we need to restore the previous value
		valInDB, err := valsCursor.SeekBothRange(fullKey, stepBytes)
		if err != nil {
			return err
		}
		if len(valInDB) > 0 {
			stepInDB := valInDB[:8]
			if bytes.Equal(stepInDB, stepBytes) {
				if err := valsCursor.DeleteCurrent(); err != nil {
					return err
				}
			}
		}

		if !bytes.Equal(stepBytes, prevStepBytes) {
			continue
		}

		if err := valsCursor.Put(fullKey, append(stepBytes, value...)); err != nil {
			return err
		}
	}
	// Compare valsKV with prevSeenKeys
	if _, err := dt.ht.Prune(ctx, rwTx, txNumUnwindTo, math.MaxUint64, math.MaxUint64, true, logEvery); err != nil {
		return fmt.Errorf("[domain][%s] unwinding, prune history to txNum=%d, step %d: %w", dt.d.filenameBase, txNumUnwindTo, step, err)
	}
	return nil
}

var (
	UseBtree = true // if true, will use btree for all files
)

func (dt *DomainRoTx) getFromFiles(filekey []byte) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error) {
	if len(dt.files) == 0 {
		return
	}

	hi, lo := dt.ht.iit.hashKey(filekey)

	if dt.getFromFileCache == nil {
		dt.getFromFileCache = dt.visible.newGetFromFileCache()
	}
	if dt.getFromFileCache != nil {
		cv, ok := dt.getFromFileCache.Get(u128{hi: hi, lo: lo})
		if ok {
			return cv.v, true, dt.files[cv.lvl].startTxNum, dt.files[cv.lvl].endTxNum, nil
		}
	}

	for i := len(dt.files) - 1; i >= 0; i-- {
		if dt.d.indexList&withExistence != 0 {
			if dt.files[i].src.existence != nil {
				if !dt.files[i].src.existence.ContainsHash(hi) {
					if traceGetLatest == dt.name {
						fmt.Printf("GetLatest(%s, %x) -> existence index %s -> false\n", dt.d.filenameBase, filekey, dt.files[i].src.existence.FileName)
					}
					continue
				} else {
					if traceGetLatest == dt.name {
						fmt.Printf("GetLatest(%s, %x) -> existence index %s -> true\n", dt.d.filenameBase, filekey, dt.files[i].src.existence.FileName)
					}
				}
			} else {
				if traceGetLatest == dt.name {
					fmt.Printf("GetLatest(%s, %x) -> existence index is nil %s\n", dt.name.String(), filekey, dt.files[i].src.decompressor.FileName())
				}
			}
		}

		v, found, err = dt.getFromFile(i, filekey)
		if err != nil {
			return nil, false, 0, 0, err
		}
		if !found {
			if traceGetLatest == dt.name {
				fmt.Printf("GetLatest(%s, %x) -> not found in file %s\n", dt.name.String(), filekey, dt.files[i].src.decompressor.FileName())
			}
			continue
		}
		if traceGetLatest == dt.name {
			fmt.Printf("GetLatest(%s, %x) -> found in file %s\n", dt.name.String(), filekey, dt.files[i].src.decompressor.FileName())
		}

		if dt.getFromFileCache != nil {
			dt.getFromFileCache.Add(u128{hi: hi, lo: lo}, domainGetFromFileCacheItem{lvl: uint8(i), v: v})
		}
		return v, true, dt.files[i].startTxNum, dt.files[i].endTxNum, nil
	}
	if traceGetLatest == dt.name {
		fmt.Printf("GetLatest(%s, %x) -> not found in %d files\n", dt.name.String(), filekey, len(dt.files))
	}

	if dt.getFromFileCache != nil {
		dt.getFromFileCache.Add(u128{hi: hi, lo: lo}, domainGetFromFileCacheItem{lvl: 0, v: nil})
	}
	return nil, false, 0, 0, nil
}

// GetAsOf does not always require usage of roTx. If it is possible to determine
// historical value based only on static files, roTx will not be used.
func (dt *DomainRoTx) GetAsOf(key []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	v, hOk, err := dt.ht.HistorySeek(key, txNum, roTx)
	if err != nil {
		return nil, err
	}
	if hOk {
		// if history returned marker of key creation
		// domain must return nil
		if len(v) == 0 {
			if traceGetAsOf == dt.d.filenameBase {
				fmt.Printf("GetAsOf(%s  , %x, %d) -> not found in history\n", dt.d.filenameBase, key, txNum)
			}
			return nil, nil
		}
		if traceGetAsOf == dt.d.filenameBase {
			fmt.Printf("GetAsOf(%s, %x, %d) -> found in history\n", dt.d.filenameBase, key, txNum)
		}
		return v, nil
	}
	v, _, _, err = dt.GetLatest(key, nil, roTx)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (dt *DomainRoTx) Close() {
	if dt.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := dt.files
	dt.files = nil
	for i := range files {
		src := files[i].src
		if src == nil || src.frozen {
			continue
		}
		refCnt := src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && src.canDelete.Load() {
			if traceFileLife != "" && dt.d.filenameBase == traceFileLife {
				dt.d.logger.Warn("[agg.dbg] real remove at DomainRoTx.Close", "file", src.decompressor.FileName())
			}
			src.closeFilesAndRemove()
		}
	}
	dt.ht.Close()

	dt.visible.returnGetFromFileCache(dt.getFromFileCache)
}

func (dt *DomainRoTx) statelessGetter(i int) *seg.Reader {
	if dt.getters == nil {
		dt.getters = make([]*seg.Reader, len(dt.files))
	}
	r := dt.getters[i]
	if r == nil {
		r = seg.NewReader(dt.files[i].src.decompressor.MakeGetter(), dt.d.compression)
		dt.getters[i] = r
	}
	return r
}

func (dt *DomainRoTx) statelessIdxReader(i int) *recsplit.IndexReader {
	if dt.idxReaders == nil {
		dt.idxReaders = make([]*recsplit.IndexReader, len(dt.files))
	}
	r := dt.idxReaders[i]
	if r == nil {
		r = dt.files[i].src.index.GetReaderFromPool()
		dt.idxReaders[i] = r
	}
	return r
}

func (dt *DomainRoTx) statelessBtree(i int) *BtIndex {
	if dt.readers == nil {
		dt.readers = make([]*BtIndex, len(dt.files))
	}
	r := dt.readers[i]
	if r == nil {
		r = dt.files[i].src.bindex
		dt.readers[i] = r
	}
	return r
}

func (dt *DomainRoTx) valsCursor(tx kv.Tx) (c kv.Cursor, err error) {
	if dt.valsC != nil {
		return dt.valsC, nil
	}

	if dt.d.largeVals {
		dt.valsC, err = tx.Cursor(dt.d.valsTable)
		return dt.valsC, err
	}
	dt.valsC, err = tx.CursorDupSort(dt.d.valsTable)
	return dt.valsC, err
}

func (dt *DomainRoTx) getLatestFromDb(key []byte, roTx kv.Tx) ([]byte, uint64, bool, error) {
	valsC, err := dt.valsCursor(roTx)
	if err != nil {
		return nil, 0, false, err
	}
	var v, foundInvStep []byte

	if dt.d.largeVals {
		var fullkey []byte
		fullkey, v, err = valsC.Seek(key)
		if err != nil {
			return nil, 0, false, fmt.Errorf("valsCursor.Seek: %w", err)
		}
		if len(fullkey) == 0 {
			return nil, 0, false, nil // This key is not in DB
		}
		if !bytes.Equal(fullkey[:len(fullkey)-8], key) {
			return nil, 0, false, nil // This key is not in DB
		}
		foundInvStep = fullkey[len(fullkey)-8:]
	} else {
		_, stepWithVal, err := valsC.SeekExact(key)
		if err != nil {
			return nil, 0, false, fmt.Errorf("valsCursor.SeekExact: %w", err)
		}
		if len(stepWithVal) == 0 {
			return nil, 0, false, nil
		}

		v = stepWithVal[8:]

		foundInvStep = stepWithVal[:8]
	}

	foundStep := ^binary.BigEndian.Uint64(foundInvStep)

	if lastTxNumOfStep(foundStep, dt.d.aggregationStep) >= dt.files.EndTxNum() {
		return v, foundStep, true, nil
	}

	return nil, 0, false, nil
}

// GetLatest returns value, step in which the value last changed, and bool value which is true if the value
// is present, and false if it is not present (not set or deleted)
func (dt *DomainRoTx) GetLatest(key1, key2 []byte, roTx kv.Tx) ([]byte, uint64, bool, error) {
	key := key1
	if len(key2) > 0 {
		key = append(append(dt.keyBuf[:0], key1...), key2...)
	}

	var v []byte
	var foundStep uint64
	var found bool
	var err error

	if traceGetLatest == dt.name {
		defer func() {
			fmt.Printf("GetLatest(%s, '%x' -> '%x') (from db=%t; istep=%x stepInFiles=%d)\n",
				dt.name.String(), key, v, found, foundStep, dt.files.EndTxNum()/dt.d.aggregationStep)
		}()
	}

	v, foundStep, found, err = dt.getLatestFromDb(key, roTx)
	if err != nil {
		return nil, 0, false, fmt.Errorf("getLatestFromDb: %w", err)
	}
	if found {
		return v, foundStep, true, nil
	}

	v, foundInFile, _, endTxNum, err := dt.getFromFiles(key)
	if err != nil {
		return nil, 0, false, fmt.Errorf("getFromFiles: %w", err)
	}
	return v, endTxNum / dt.d.aggregationStep, foundInFile, nil
}

func (dt *DomainRoTx) GetLatestFromFiles(key []byte) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error) {
	return dt.getFromFiles(key)
}

func (dt *DomainRoTx) DomainRange(ctx context.Context, tx kv.Tx, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it stream.KV, err error) {
	if !asc {
		panic("implement me")
	}
	//histStateIt, err := tx.aggTx.AccountHistoricalStateRange(asOfTs, fromKey, toKey, limit, tx.MdbxTx)
	//if err != nil {
	//	return nil, err
	//}
	//lastestStateIt, err := tx.aggTx.DomainRangeLatest(tx.MdbxTx, kv.AccountDomain, fromKey, toKey, limit)
	//if err != nil {
	//	return nil, err
	//}
	histStateIt, err := dt.ht.WalkAsOf(ctx, ts, fromKey, toKey, tx, limit)
	if err != nil {
		return nil, err
	}
	lastestStateIt, err := dt.DomainRangeLatest(tx, fromKey, toKey, limit)
	if err != nil {
		return nil, err
	}
	return stream.UnionKV(histStateIt, lastestStateIt, limit), nil
}

func (dt *DomainRoTx) DomainRangeLatest(roTx kv.Tx, fromKey, toKey []byte, limit int) (stream.KV, error) {
	s := &DomainLatestIterFile{from: fromKey, to: toKey, limit: limit, dc: dt,
		roTx:      roTx,
		valsTable: dt.d.valsTable,
		h:         &CursorHeap{},
	}
	if err := s.init(dt); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return s, nil
}

// CanPruneUntil returns true if domain OR history tables can be pruned until txNum
func (dt *DomainRoTx) CanPruneUntil(tx kv.Tx, untilTx uint64) bool {
	canDomain, _ := dt.canPruneDomainTables(tx, untilTx)
	canHistory, _ := dt.ht.canPruneUntil(tx, untilTx)
	return canHistory || canDomain
}

func (dt *DomainRoTx) canBuild(dbtx kv.Tx) bool { //nolint
	maxStepInFiles := dt.files.EndTxNum() / dt.d.aggregationStep
	return maxStepInFiles < dt.d.maxStepInDB(dbtx)
}

// checks if there is anything to prune in DOMAIN tables.
// everything that aggregated is prunable.
// history.CanPrune should be called separately because it responsible for different tables
func (dt *DomainRoTx) canPruneDomainTables(tx kv.Tx, untilTx uint64) (can bool, maxStepToPrune uint64) {
	if m := dt.files.EndTxNum(); m > 0 {
		maxStepToPrune = (m - 1) / dt.d.aggregationStep
	}
	var untilStep uint64
	if untilTx > 0 {
		untilStep = (untilTx - 1) / dt.d.aggregationStep
	}
	sm, err := GetExecV3PrunableProgress(tx, []byte(dt.d.valsTable))
	if err != nil {
		dt.d.logger.Error("get domain pruning progress", "name", dt.d.filenameBase, "error", err)
		return false, maxStepToPrune
	}

	delta := float64(max(maxStepToPrune, sm) - min(maxStepToPrune, sm)) // maxStep could be 0
	switch dt.d.filenameBase {
	case "account":
		mxPrunableDAcc.Set(delta)
	case "storage":
		mxPrunableDSto.Set(delta)
	case "code":
		mxPrunableDCode.Set(delta)
	case "commitment":
		mxPrunableDComm.Set(delta)
	}
	//fmt.Printf("smallestToPrune[%s] minInDB %d inFiles %d until %d\n", dt.d.filenameBase, sm, maxStepToPrune, untilStep)
	return sm <= min(maxStepToPrune, untilStep), maxStepToPrune
}

type DomainPruneStat struct {
	MinStep uint64
	MaxStep uint64
	Values  uint64
	History *InvertedIndexPruneStat
}

func (dc *DomainPruneStat) PrunedNothing() bool {
	return dc.Values == 0 && (dc.History == nil || dc.History.PrunedNothing())
}

func (dc *DomainPruneStat) String() (kvstr string) {
	if dc.PrunedNothing() {
		return ""
	}
	if dc.Values > 0 {
		kvstr = fmt.Sprintf("kv: %s from steps %d-%d", common.PrettyCounter(dc.Values), dc.MinStep, dc.MaxStep)
	}
	if dc.History != nil {
		if kvstr != "" {
			kvstr += ", "
		}
		kvstr += dc.History.String()
	}
	return kvstr
}

func (dc *DomainPruneStat) Accumulate(other *DomainPruneStat) {
	if other == nil {
		return
	}
	dc.MinStep = min(dc.MinStep, other.MinStep)
	dc.MaxStep = max(dc.MaxStep, other.MaxStep)
	dc.Values += other.Values
	if dc.History == nil {
		if other.History != nil {
			dc.History = other.History
		}
	} else {
		dc.History.Accumulate(other.History)
	}
}

func (dt *DomainRoTx) Prune(ctx context.Context, rwTx kv.RwTx, step, txFrom, txTo, limit uint64, logEvery *time.Ticker) (stat *DomainPruneStat, err error) {
	if limit == 0 {
		limit = math.MaxUint64
	}

	stat = &DomainPruneStat{MinStep: math.MaxUint64}
	if stat.History, err = dt.ht.Prune(ctx, rwTx, txFrom, txTo, limit, false, logEvery); err != nil {
		return nil, fmt.Errorf("prune history at step %d [%d, %d): %w", step, txFrom, txTo, err)
	}
	canPrune, maxPrunableStep := dt.canPruneDomainTables(rwTx, txTo)
	if !canPrune {
		return stat, nil
	}
	if step > maxPrunableStep {
		step = maxPrunableStep
	}

	st := time.Now()
	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()

	var valsCursor kv.RwCursor

	ancientDomainValsCollector := etl.NewCollector(dt.name.String()+".domain.collate", dt.d.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), dt.d.logger).LogLvl(log.LvlTrace)
	defer ancientDomainValsCollector.Close()

	if dt.d.largeVals {
		valsCursor, err = rwTx.RwCursor(dt.d.valsTable)
		if err != nil {
			return stat, fmt.Errorf("create %s domain values cursor: %w", dt.name.String(), err)
		}
	} else {
		valsCursor, err = rwTx.RwCursorDupSort(dt.d.valsTable)
		if err != nil {
			return stat, fmt.Errorf("create %s domain values cursor: %w", dt.name.String(), err)
		}
	}
	defer valsCursor.Close()

	loadFunc := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		if dt.d.largeVals {
			return valsCursor.Delete(k)
		}
		return valsCursor.(kv.RwCursorDupSort).DeleteExact(k, v)
	}

	prunedKey, err := GetExecV3PruneProgress(rwTx, dt.d.valsTable)
	if err != nil {
		dt.d.logger.Error("get domain pruning progress", "name", dt.name.String(), "error", err)
	}

	var k, v []byte
	if prunedKey != nil && limit < 100_000 {
		k, v, err = valsCursor.Seek(prunedKey)
	} else {
		k, v, err = valsCursor.First()
	}
	if err != nil {
		return nil, err
	}
	var stepBytes []byte
	for ; k != nil; k, v, err = valsCursor.Next() {
		if err != nil {
			return stat, fmt.Errorf("iterate over %s domain keys: %w", dt.name.String(), err)
		}

		if dt.d.largeVals {
			stepBytes = k[len(k)-8:]
		} else {
			stepBytes = v[:8]
		}

		is := ^binary.BigEndian.Uint64(stepBytes)
		if is > step {
			continue
		}
		if limit == 0 {
			if err := ancientDomainValsCollector.Load(rwTx, dt.d.valsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
				return stat, fmt.Errorf("load domain values: %w", err)
			}
			if err := SaveExecV3PruneProgress(rwTx, dt.d.valsTable, k); err != nil {
				return stat, fmt.Errorf("save domain pruning progress: %s, %w", dt.name.String(), err)
			}
			return stat, nil
		}
		limit--
		stat.Values++
		if err := ancientDomainValsCollector.Collect(k, v); err != nil {
			return nil, err
		}
		stat.MinStep = min(stat.MinStep, is)
		stat.MaxStep = max(stat.MaxStep, is)
		select {
		case <-ctx.Done():
			// consider ctx exiting as incorrect outcome, error is returned
			return stat, ctx.Err()
		case <-logEvery.C:
			dt.d.logger.Info("[snapshots] prune domain", "name", dt.name.String(),
				"pruned keys", stat.Values,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(dt.d.aggregationStep), float64(txTo)/float64(dt.d.aggregationStep)))
		default:
		}
	}
	mxPruneSizeDomain.AddUint64(stat.Values)
	if err := ancientDomainValsCollector.Load(rwTx, dt.d.valsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return stat, fmt.Errorf("load domain values: %w", err)
	}
	if err := SaveExecV3PruneProgress(rwTx, dt.d.valsTable, nil); err != nil {
		return stat, fmt.Errorf("save domain pruning progress: %s, %w", dt.d.filenameBase, err)
	}

	if err := SaveExecV3PrunableProgress(rwTx, []byte(dt.d.valsTable), step+1); err != nil {
		return stat, err
	}
	mxPruneTookDomain.ObserveDuration(st)
	return stat, nil
}

type DomainLatestIterFile struct {
	dc *DomainRoTx

	roTx      kv.Tx
	valsTable string

	limit int

	from, to []byte
	nextVal  []byte
	nextKey  []byte

	h *CursorHeap

	k, v, kBackup, vBackup []byte
	largeVals              bool
}

func (hi *DomainLatestIterFile) Close() {
}
func (hi *DomainLatestIterFile) init(dc *DomainRoTx) error {
	// Implementation:
	//     File endTxNum  = last txNum of file step
	//     DB endTxNum    = first txNum of step in db
	//     RAM endTxNum   = current txnum
	//  Example: stepSize=8, file=0-2.kv, db has key of step 2, current txn num is 17
	//     File endTxNum  = 15, because `0-2.kv` has steps 0 and 1, last txNum of step 1 is 15
	//     DB endTxNum    = 16, because db has step 2, and first txNum of step 2 is 16.
	//     RAM endTxNum   = 17, because current tcurrent txNum is 17
	hi.largeVals = dc.d.largeVals
	heap.Init(hi.h)
	var key, value []byte

	if dc.d.largeVals {
		valsCursor, err := hi.roTx.Cursor(dc.d.valsTable)
		if err != nil {
			return err
		}
		if key, value, err = valsCursor.Seek(hi.from); err != nil {
			return err
		}
		if key != nil && (hi.to == nil || bytes.Compare(key[:len(key)-8], hi.to) < 0) {
			k := key[:len(key)-8]
			stepBytes := key[len(key)-8:]
			step := ^binary.BigEndian.Uint64(stepBytes)
			endTxNum := step * dc.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files

			heap.Push(hi.h, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(value), cNonDup: valsCursor, endTxNum: endTxNum, reverse: true})
		}
	} else {
		valsCursor, err := hi.roTx.CursorDupSort(dc.d.valsTable)
		if err != nil {
			return err
		}

		if key, value, err = valsCursor.Seek(hi.from); err != nil {
			return err
		}
		if key != nil && (hi.to == nil || bytes.Compare(key, hi.to) < 0) {
			stepBytes := value[:8]
			value = value[8:]
			step := ^binary.BigEndian.Uint64(stepBytes)
			endTxNum := step * dc.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files

			heap.Push(hi.h, &CursorItem{t: DB_CURSOR, key: common.Copy(key), val: common.Copy(value), cDup: valsCursor, endTxNum: endTxNum, reverse: true})
		}
	}

	for i, item := range dc.files {
		// todo release btcursor when iter over/make it truly stateless
		btCursor, err := dc.statelessBtree(i).Seek(dc.statelessGetter(i), hi.from)
		if err != nil {
			return err
		}
		if btCursor == nil {
			continue
		}

		key := btCursor.Key()
		if key != nil && (hi.to == nil || bytes.Compare(key, hi.to) < 0) {
			val := btCursor.Value()
			txNum := item.endTxNum - 1 // !important: .kv files have semantic [from, t)
			heap.Push(hi.h, &CursorItem{t: FILE_CURSOR, key: key, val: val, btCursor: btCursor, endTxNum: txNum, reverse: true})
		}
	}
	return hi.advanceInFiles()
}

func (hi *DomainLatestIterFile) advanceInFiles() error {
	for hi.h.Len() > 0 {
		lastKey := (*hi.h)[0].key
		lastVal := (*hi.h)[0].val

		// Advance all the items that have this key (including the top)
		for hi.h.Len() > 0 && bytes.Equal((*hi.h)[0].key, lastKey) {
			ci1 := heap.Pop(hi.h).(*CursorItem)
			switch ci1.t {
			case FILE_CURSOR:
				if ci1.btCursor.Next() {
					ci1.key = ci1.btCursor.Key()
					ci1.val = ci1.btCursor.Value()
					if ci1.key != nil && (hi.to == nil || bytes.Compare(ci1.key, hi.to) < 0) {
						heap.Push(hi.h, ci1)
					}
				}
			case DB_CURSOR:
				if hi.largeVals {
					// start from current go to next
					initial, v, err := ci1.cNonDup.Current()
					if err != nil {
						return err
					}
					var k []byte
					for initial != nil && (k == nil || bytes.Equal(initial[:len(initial)-8], k[:len(k)-8])) {
						k, v, err = ci1.cNonDup.Next()
						if err != nil {
							return err
						}
						if k == nil {
							break
						}
					}

					if len(k) > 0 && (hi.to == nil || bytes.Compare(k[:len(k)-8], hi.to) < 0) {
						stepBytes := k[len(k)-8:]
						k = k[:len(k)-8]
						ci1.key = common.Copy(k)
						step := ^binary.BigEndian.Uint64(stepBytes)
						endTxNum := step * hi.dc.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
						ci1.endTxNum = endTxNum

						ci1.val = common.Copy(v)
						heap.Push(hi.h, ci1)
					}
				} else {
					// start from current go to next
					k, stepBytesWithValue, err := ci1.cDup.NextNoDup()
					if err != nil {
						return err
					}

					if len(k) > 0 && (hi.to == nil || bytes.Compare(k, hi.to) < 0) {
						stepBytes := stepBytesWithValue[:8]
						v := stepBytesWithValue[8:]
						ci1.key = common.Copy(k)
						step := ^binary.BigEndian.Uint64(stepBytes)
						endTxNum := step * hi.dc.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
						ci1.endTxNum = endTxNum

						ci1.val = common.Copy(v)
						heap.Push(hi.h, ci1)
					}
				}

			}
		}
		if len(lastVal) > 0 {
			hi.nextKey, hi.nextVal = lastKey, lastVal
			return nil // founc
		}
	}
	hi.nextKey = nil
	return nil
}

func (hi *DomainLatestIterFile) HasNext() bool {
	return hi.limit != 0 && hi.nextKey != nil
}

func (hi *DomainLatestIterFile) Next() ([]byte, []byte, error) {
	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy iter.Dual Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advanceInFiles(); err != nil {
		return nil, nil, err
	}
	return hi.kBackup, hi.vBackup, nil
}

func (d *Domain) stepsRangeInDBAsStr(tx kv.Tx) string {
	a1, a2 := d.History.InvertedIndex.stepsRangeInDB(tx)
	//ad1, ad2 := d.stepsRangeInDB(tx)
	//if ad2-ad1 < 0 {
	//	fmt.Printf("aaa: %f, %f\n", ad1, ad2)
	//}
	return fmt.Sprintf("%s:%.1f", d.filenameBase, a2-a1)
}

func (dt *DomainRoTx) Files() (res []string) {
	for _, item := range dt.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return append(res, dt.ht.Files()...)
}

type SelectedStaticFiles struct {
	accounts       []*filesItem
	accountsIdx    []*filesItem
	accountsHist   []*filesItem
	storage        []*filesItem
	storageIdx     []*filesItem
	storageHist    []*filesItem
	code           []*filesItem
	codeIdx        []*filesItem
	codeHist       []*filesItem
	commitment     []*filesItem
	commitmentIdx  []*filesItem
	commitmentHist []*filesItem
	//codeI          int
	//storageI       int
	//accountsI      int
	//commitmentI    int
}

//func (sf SelectedStaticFiles) FillV3(s *SelectedStaticFilesV3) SelectedStaticFiles {
//	sf.accounts, sf.accountsIdx, sf.accountsHist = s.accounts, s.accountsIdx, s.accountsHist
//	sf.storage, sf.storageIdx, sf.storageHist = s.storage, s.storageIdx, s.storageHist
//	sf.code, sf.codeIdx, sf.codeHist = s.code, s.codeIdx, s.codeHist
//	sf.commitment, sf.commitmentIdx, sf.commitmentHist = s.commitment, s.commitmentIdx, s.commitmentHist
//	sf.codeI, sf.accountsI, sf.storageI, sf.commitmentI = s.codeI, s.accountsI, s.storageI, s.commitmentI
//	return sf
//}

func (sf SelectedStaticFiles) Close() {
	for _, group := range [][]*filesItem{
		sf.accounts, sf.accountsIdx, sf.accountsHist,
		sf.storage, sf.storageIdx, sf.storageHist,
		sf.code, sf.codeIdx, sf.codeHist,
		sf.commitment, sf.commitmentIdx, sf.commitmentHist,
	} {
		for _, item := range group {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.index != nil {
					item.index.Close()
				}
				if item.bindex != nil {
					item.bindex.Close()
				}
			}
		}
	}
}

type DomainStats struct {
	MergesCount          uint64
	LastCollationTook    time.Duration
	LastPruneTook        time.Duration
	LastPruneHistTook    time.Duration
	LastFileBuildingTook time.Duration
	LastCollationSize    uint64
	LastPruneSize        uint64

	FilesQueries *atomic.Uint64
	TotalQueries *atomic.Uint64
	EfSearchTime time.Duration
	DataSize     uint64
	IndexSize    uint64
	FilesCount   uint64
}

func (ds *DomainStats) Accumulate(other DomainStats) {
	if other.FilesQueries != nil {
		ds.FilesQueries.Add(other.FilesQueries.Load())
	}
	if other.TotalQueries != nil {
		ds.TotalQueries.Add(other.TotalQueries.Load())
	}
	ds.EfSearchTime += other.EfSearchTime
	ds.IndexSize += other.IndexSize
	ds.DataSize += other.DataSize
	ds.FilesCount += other.FilesCount
}

func ParseStepsFromFileName(fileName string) (from, to uint64, err error) {
	rangeString := strings.Split(fileName, ".")[1]
	rangeNums := strings.Split(rangeString, "-")
	// convert the range to uint64
	from, err = strconv.ParseUint(rangeNums[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse to %s: %w", rangeNums[1], err)
	}
	to, err = strconv.ParseUint(rangeNums[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse to %s: %w", rangeNums[1], err)
	}
	return from, to, nil
}
