/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/backup"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"
)

// StepsInColdFile - files of this size are completely frozen/immutable.
// files of smaller size are also immutable, but can be removed after merge to bigger files.
const StepsInColdFile = 64

var (
	asserts          = dbg.EnvBool("AGG_ASSERTS", false)
	traceFileLife    = dbg.EnvString("AGG_TRACE_FILE_LIFE", "")
	traceGetLatest   = dbg.EnvString("AGG_TRACE_GET_LATEST", "")
	traceGetAsOf     = dbg.EnvString("AGG_TRACE_GET_AS_OF", "")
	tracePutWithPrev = dbg.EnvString("AGG_TRACE_PUT_WITH_PREV", "")
)

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
//
// Data-Existence in .kv vs .v files:
//  1. key doesn’t exists, then create: .kv - yes, .v - yes
//  2. acc exists, then update/delete:  .kv - yes, .v - yes
//  3. acc doesn’t exists, then delete: .kv - no,  .v - no
type Domain struct {
	*History

	// dirtyFiles - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in Aggregator
	//
	// _visibleFiles derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// BeginRo() using _visibleFiles in zero-copy way
	dirtyFiles *btree2.BTreeG[*filesItem]

	// _visibleFiles - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visibleFiles []ctxItem

	// replaceKeysInValues allows to replace commitment branch values with shorter keys.
	// for commitment domain only
	replaceKeysInValues bool
	// restricts subset file deletions on open/close. Needed to hold files until commitment is merged
	restrictSubsetFileDeletions bool

	keysTable   string // key -> invertedStep , invertedStep = ^(txNum / aggregationStep), Needs to be table with DupSort
	valsTable   string // key + invertedStep -> values
	stats       DomainStats
	compression FileCompression
	indexList   idxList
}

type domainCfg struct {
	hist     histCfg
	compress FileCompression

	replaceKeysInValues         bool
	restrictSubsetFileDeletions bool
}

func NewDomain(cfg domainCfg, aggregationStep uint64, filenameBase, keysTable, valsTable, indexKeysTable, historyValsTable, indexTable string, logger log.Logger) (*Domain, error) {
	if cfg.hist.iiCfg.dirs.SnapDomain == "" {
		panic("empty `dirs` varialbe")
	}
	d := &Domain{
		keysTable:   keysTable,
		valsTable:   valsTable,
		compression: cfg.compress,
		dirtyFiles:  btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		stats:       DomainStats{FilesQueries: &atomic.Uint64{}, TotalQueries: &atomic.Uint64{}},

		indexList:                   withBTree | withExistence,
		replaceKeysInValues:         cfg.replaceKeysInValues,         // for commitment domain only
		restrictSubsetFileDeletions: cfg.restrictSubsetFileDeletions, // to prevent not merged 'garbage' to delete on start
	}

	d._visibleFiles = []ctxItem{}

	var err error
	if d.History, err = NewHistory(cfg.hist, aggregationStep, filenameBase, indexKeysTable, indexTable, historyValsTable, nil, logger); err != nil {
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

// LastStepInDB - return the latest available step in db (at-least 1 value in such step)
func (d *Domain) LastStepInDB(tx kv.Tx) (lstInDb uint64) {
	lstIdx, _ := kv.LastKey(tx, d.History.indexKeysTable)
	if len(lstIdx) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(lstIdx) / d.aggregationStep
}
func (d *Domain) FirstStepInDB(tx kv.Tx) (lstInDb uint64) {
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
func (d *Domain) OpenList(idxFiles, histFiles, domainFiles []string, readonly bool) error {
	if err := d.History.OpenList(idxFiles, histFiles, readonly); err != nil {
		return err
	}
	if err := d.openList(domainFiles, readonly); err != nil {
		return fmt.Errorf("Domain(%s).OpenFolder: %w", d.filenameBase, err)
	}
	return nil
}

func (d *Domain) openList(names []string, readonly bool) error {
	defer d.reCalcVisibleFiles()
	d.closeWhatNotInList(names)
	d.scanStateFiles(names)
	if err := d.openFiles(); err != nil {
		return fmt.Errorf("Domain.openList: %w, %s", err, d.filenameBase)
	}
	d.reCalcVisibleFiles()
	d.protectFromHistoryFilesAheadOfDomainFiles(readonly)
	return nil
}

// protectFromHistoryFilesAheadOfDomainFiles - in some corner-cases app may see more .ef/.v files than .kv:
//   - `kill -9` in the middle of `buildFiles()`, then `rm -f db` (restore from backup)
//   - `kill -9` in the middle of `buildFiles()`, then `stage_exec --reset` (drop progress - as a hot-fix)
func (d *Domain) protectFromHistoryFilesAheadOfDomainFiles(readonly bool) {
	d.removeFilesAfterStep(d.dirtyFilesEndTxNumMinimax()/d.aggregationStep, readonly)
}

func (d *Domain) OpenFolder(readonly bool) error {
	idx, histFiles, domainFiles, err := d.fileNamesOnDisk()
	if err != nil {
		return fmt.Errorf("Domain(%s).OpenFolder: %w", d.filenameBase, err)
	}
	if err := d.OpenList(idx, histFiles, domainFiles, readonly); err != nil {
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

func (d *Domain) removeFilesAfterStep(lowerBound uint64, readonly bool) {
	var toDelete []*filesItem
	d.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toDelete = append(toDelete, item)
		}
		return true
	})
	for _, item := range toDelete {
		d.dirtyFiles.Delete(item)
		if !readonly {
			log.Debug(fmt.Sprintf("[snapshots] delete %s, because step %d has not enough files (was not complete). stack: %s", item.decompressor.FileName(), lowerBound, dbg.Stack()))
			item.closeFilesAndRemove()
		} else {
			log.Debug(fmt.Sprintf("[snapshots] closing %s, because step %d has not enough files (was not complete). stack: %s", item.decompressor.FileName(), lowerBound, dbg.Stack()))
			item.closeFiles()
		}
	}

	toDelete = toDelete[:0]
	d.History.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toDelete = append(toDelete, item)
		}
		return true
	})
	for _, item := range toDelete {
		d.History.dirtyFiles.Delete(item)
		if !readonly {
			log.Debug(fmt.Sprintf("[snapshots] deleting some histor files - because step %d has not enough files (was not complete)", lowerBound))
			item.closeFilesAndRemove()
		} else {
			log.Debug(fmt.Sprintf("[snapshots] closing some histor files - because step %d has not enough files (was not complete)", lowerBound))
			item.closeFiles()
		}
	}

	toDelete = toDelete[:0]
	d.History.InvertedIndex.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toDelete = append(toDelete, item)
		}
		return true
	})
	for _, item := range toDelete {
		d.History.InvertedIndex.dirtyFiles.Delete(item)
		if !readonly {
			log.Debug(fmt.Sprintf("[snapshots] delete %s, because step %d has not enough files (was not complete)", item.decompressor.FileName(), lowerBound))
			item.closeFilesAndRemove()
		} else {
			log.Debug(fmt.Sprintf("[snapshots] closing %s, because step %d has not enough files (was not complete)", item.decompressor.FileName(), lowerBound))
			item.closeFiles()
		}
	}
}

func (d *Domain) scanStateFiles(fileNames []string) (garbageFiles []*filesItem) {
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

func (d *Domain) openFiles() (err error) {
	invalidFileItems := make([]*filesItem, 0)
	invalidFileItemsLock := sync.Mutex{}
	d.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			if item.decompressor == nil {
				fPath := d.kvFilePath(fromStep, toStep)
				if !dir.FileExist(fPath) {
					_, fName := filepath.Split(fPath)
					d.logger.Debug("[agg] Domain.openFiles: file does not exists", "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						d.logger.Debug("[agg] Domain.openFiles", "err", err, "f", fName)
					} else {
						d.logger.Warn("[agg] Domain.openFiles", "err", err, "f", fName)
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
				if dir.FileExist(fPath) {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
			if item.bindex == nil {
				fPath := d.kvBtFilePath(fromStep, toStep)
				if dir.FileExist(fPath) {
					if item.bindex, err = OpenBtreeIndexWithDecompressor(fPath, DefaultBtreeM, item.decompressor, d.compression); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
			if item.existence == nil {
				fPath := d.kvExistenceIdxFilePath(fromStep, toStep)
				if dir.FileExist(fPath) {
					if item.existence, err = OpenExistenceFilter(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openFiles", "err", err, "f", fName)
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
	var toDelete []*filesItem
	d.dirtyFiles.Walk(func(items []*filesItem) bool {
	Loop1:
		for _, item := range items {
			for _, protectName := range fNames {
				if item.decompressor != nil && item.decompressor.FileName() == protectName {
					continue Loop1
				}
			}
			toDelete = append(toDelete, item)
		}
		return true
	})
	for _, item := range toDelete {
		item.closeFiles()
		d.dirtyFiles.Delete(item)
	}
}

func (d *Domain) reCalcVisibleFiles() {
	d._visibleFiles = calcVisibleFiles(d.dirtyFiles, d.indexList, false)
	d.History.reCalcVisibleFiles()
}

func (d *Domain) Close() {
	d.History.Close()
	d.closeWhatNotInList([]string{})
}

func (w *domainBufferedWriter) PutWithPrev(key1, key2, val, preval []byte, prevStep uint64) error {
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `preval`` slice is invalidated
	if tracePutWithPrev != "" && tracePutWithPrev == w.h.ii.filenameBase {
		fmt.Printf("PutWithPrev(%s, tx %d, key[%x][%x] value[%x] preval[%x])\n", w.h.ii.filenameBase, w.h.ii.txNum, key1, key2, val, preval)
	}
	if err := w.h.AddPrevValue(key1, key2, preval, prevStep); err != nil {
		return err
	}
	return w.addValue(key1, key2, val)
}

func (w *domainBufferedWriter) DeleteWithPrev(key1, key2, prev []byte, prevStep uint64) (err error) {
	// This call to update needs to happen before d.tx.Delete() later, because otherwise the content of `original`` slice is invalidated
	if tracePutWithPrev != "" && tracePutWithPrev == w.h.ii.filenameBase {
		fmt.Printf("DeleteWithPrev(%s, tx %d, key[%x][%x] preval[%x])\n", w.h.ii.filenameBase, w.h.ii.txNum, key1, key2, prev)
	}
	if err := w.h.AddPrevValue(key1, key2, prev, prevStep); err != nil {
		return err
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
		keysTable: dt.d.keysTable,
		valsTable: dt.d.valsTable,
		keys:      etl.NewCollector(dt.d.keysTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), dt.d.logger),
		values:    etl.NewCollector(dt.d.valsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), dt.d.logger),

		h: dt.ht.newWriter(tmpdir, discardHistory),
	}
	w.keys.LogLvl(log.LvlTrace)
	w.values.LogLvl(log.LvlTrace)
	w.keys.SortAndFlushInBackground(true)
	w.values.SortAndFlushInBackground(true)
	return w
}

type domainBufferedWriter struct {
	keys, values *etl.Collector

	setTxNumOnce bool
	discard      bool

	keysTable, valsTable string

	stepBytes [8]byte // current inverted step representation
	aux       []byte

	h *historyBufferedWriter
}

func (w *domainBufferedWriter) close() {
	if w == nil { // allow dobule-close
		return
	}
	w.h.close()
	if w.keys != nil {
		w.keys.Close()
	}
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

	if err := w.keys.Load(tx, w.keysTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := w.values.Load(tx, w.valsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
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

	kl := len(key1) + len(key2)
	w.aux = append(append(append(w.aux[:0], key1...), key2...), w.stepBytes[:]...)
	fullkey := w.aux[:kl+8]
	if asserts && (w.h.ii.txNum/w.h.ii.aggregationStep) != ^binary.BigEndian.Uint64(w.stepBytes[:]) {
		panic(fmt.Sprintf("assert: %d != %d", w.h.ii.txNum/w.h.ii.aggregationStep, ^binary.BigEndian.Uint64(w.stepBytes[:])))
	}

	//defer func() {
	//	fmt.Printf("addValue     [%p;tx=%d] '%x' -> '%x'\n", w, w.h.ii.txNum, fullkey, value)
	//}()

	if err := w.keys.Collect(fullkey[:kl], fullkey[kl:]); err != nil {
		return err
	}
	if err := w.values.Collect(fullkey, value); err != nil {
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
	c            kv.CursorDupSort
	iter         btree2.MapIter[string, []byte]
	dg           ArchiveGetter
	dg2          ArchiveGetter
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
	ht         *HistoryRoTx
	d          *Domain
	files      []ctxItem
	getters    []ArchiveGetter
	readers    []*BtIndex
	idxReaders []*recsplit.IndexReader

	keyBuf [60]byte // 52b key and 8b for inverted step
	valBuf [128]byte

	keysC kv.CursorDupSort
	valsC kv.Cursor
}

func (dt *DomainRoTx) getFromFile(i int, filekey []byte) ([]byte, bool, error) {
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

	_, v, ok, err := dt.statelessBtree(i).Get(filekey, g)
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
			idx := item.index
			if idx == nil {
				fPath := dt.d.efAccessorFilePath(item.startTxNum/dt.d.aggregationStep, item.endTxNum/dt.d.aggregationStep)
				if dir.FileExist(fPath) {
					var err error
					idx, err = recsplit.OpenIndex(fPath)
					if err != nil {
						_, fName := filepath.Split(fPath)
						dt.d.logger.Warn("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
						continue
					}
					defer idx.Close()
				} else {
					continue
				}
			}

			offset, ok := idx.GetReaderFromPool().Lookup(k)
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
			log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d\n", item.decompressor.FileName(), ef.Min(), ef.Max(), last2, iter.ToArrU64Must(ef.Iterator())))
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
	files := d._visibleFiles
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}
	return &DomainRoTx{
		d:     d,
		ht:    d.History.BeginFilesRo(),
		files: files,
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
	if coll.valuesComp, err = seg.NewCompressor(ctx, "collate values", coll.valuesPath, d.dirs.Tmp, seg.MinPatternScore, d.compressWorkers, log.LvlTrace, d.logger); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}
	comp := NewArchiveWriter(coll.valuesComp, d.compression)

	keysCursor, err := roTx.CursorDupSort(d.keysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	var (
		stepBytes = make([]byte, 8)
		keySuffix = make([]byte, 256+8)
		v         []byte

		valsDup kv.CursorDupSort
	)
	binary.BigEndian.PutUint64(stepBytes, ^step)
	valsDup, err = roTx.CursorDupSort(d.valsTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s values cursorDupsort: %w", d.filenameBase, err)
	}
	defer valsDup.Close()

	for k, stepInDB, err := keysCursor.First(); k != nil; k, stepInDB, err = keysCursor.Next() {
		if err != nil {
			return coll, err
		}
		if !bytes.Equal(stepBytes, stepInDB) { // [txFrom; txTo)
			continue
		}

		copy(keySuffix, k)
		copy(keySuffix[len(k):], stepInDB)

		v, err = roTx.GetOne(d.valsTable, keySuffix[:len(k)+8])
		if err != nil {
			return coll, fmt.Errorf("find last %s value for aggregation step k=[%x]: %w", d.filenameBase, k, err)
		}

		if err = comp.AddWord(k); err != nil {
			return coll, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, k, err)
		}
		if err = comp.AddWord(v); err != nil {
			return coll, fmt.Errorf("add %s values [%x]=>[%x]: %w", d.filenameBase, k, v, err)
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
	if d.filenameBase == traceFileLife {
		d.logger.Warn("[snapshots] buildFiles", "step", step, "domain", d.filenameBase)
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
		if err = d.buildMapIdx(ctx, step, step+1, valuesDecomp, ps); err != nil {
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
		if dir.FileExist(fPath) {
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

func (d *Domain) buildMapIdx(ctx context.Context, fromStep, toStep uint64, data *seg.Decompressor, ps *background.ProgressSet) error {
	idxPath := d.kvAccessorFilePath(fromStep, toStep)
	cfg := recsplit.RecSplitArgs{
		Enums:              false,
		LessFalsePositives: false,

		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     d.dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       d.salt,
	}
	return buildIndex(ctx, data, d.compression, idxPath, false, cfg, ps, d.logger, d.noFsync)
}

func (d *Domain) missedBtreeIdxFiles() (l []*filesItem) {
	d.dirtyFiles.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			fPath := d.kvBtFilePath(fromStep, toStep)
			if !dir.FileExist(fPath) {
				l = append(l, item)
				continue
			}
			fPath = d.kvExistenceIdxFilePath(fromStep, toStep)
			if !dir.FileExist(fPath) {
				l = append(l, item)
				continue
			}
		}
		return true
	})
	return l
}
func (d *Domain) missedKviIdxFiles() (l []*filesItem) {
	d.dirtyFiles.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			fPath := d.kvAccessorFilePath(fromStep, toStep)
			if !dir.FileExist(fPath) {
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

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (d *Domain) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	d.History.BuildMissedIndices(ctx, g, ps)
	for _, item := range d.missedBtreeIdxFiles() {
		if !UseBpsTree {
			continue
		}
		if item.decompressor == nil {
			log.Warn(fmt.Sprintf("[dbg] BuildMissedIndices: item with nil decompressor %s %d-%d", d.filenameBase, item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep))
		}
		item := item

		g.Go(func() error {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			idxPath := d.kvBtFilePath(fromStep, toStep)
			if err := BuildBtreeIndexWithDecompressor(idxPath, item.decompressor, CompressNone, ps, d.dirs.Tmp, *d.salt, d.logger, d.noFsync); err != nil {
				return fmt.Errorf("failed to build btree index for %s:  %w", item.decompressor.FileName(), err)
			}
			return nil
		})
	}
	for _, item := range d.missedKviIdxFiles() {
		if UseBpsTree {
			continue
		}
		if item.decompressor == nil {
			log.Warn(fmt.Sprintf("[dbg] BuildMissedIndices: item with nil decompressor %s %d-%d", d.filenameBase, item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep))
		}
		item := item
		g.Go(func() error {
			if UseBpsTree {
				return nil
			}

			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			err := d.buildMapIdx(ctx, fromStep, toStep, item.decompressor, ps)
			if err != nil {
				return fmt.Errorf("build %s values recsplit index: %w", d.filenameBase, err)
			}
			return nil
		})
	}
}

func buildIndexFilterThenOpen(ctx context.Context, d *seg.Decompressor, compressed FileCompression, idxPath, tmpdir string, salt *uint32, ps *background.ProgressSet, logger log.Logger, noFsync bool) (*ExistenceFilter, error) {
	if err := buildIdxFilter(ctx, d, compressed, idxPath, salt, ps, logger, noFsync); err != nil {
		return nil, err
	}
	if !dir.FileExist(idxPath) {
		return nil, nil
	}
	return OpenExistenceFilter(idxPath)
}
func buildIndex(ctx context.Context, d *seg.Decompressor, compressed FileCompression, idxPath string, values bool, cfg recsplit.RecSplitArgs, ps *background.ProgressSet, logger log.Logger, noFsync bool) error {
	_, fileName := filepath.Split(idxPath)
	count := d.Count()
	if !values {
		count = d.Count() / 2
	}
	p := ps.AddNew(fileName, uint64(count))
	defer ps.Delete(p)

	defer d.EnableReadAhead().DisableReadAhead()

	g := NewArchiveGetter(d.MakeGetter(), compressed)
	var rs *recsplit.RecSplit
	var err error
	cfg.KeyCount = count
	if rs, err = recsplit.NewRecSplit(cfg, logger); err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)
	if noFsync {
		rs.DisableFsync()
	}

	word := make([]byte, 0, 256)
	var keyPos, valPos uint64
	for {
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
func (dt *DomainRoTx) Unwind(ctx context.Context, rwTx kv.RwTx, step, txNumUnwindTo uint64) error {
	d := dt.d
	//fmt.Printf("[domain][%s] unwinding domain to txNum=%d, step %d\n", d.filenameBase, txNumUnwindTo, step)
	histRng, err := dt.ht.HistoryRange(int(txNumUnwindTo), -1, order.Asc, -1, rwTx)
	if err != nil {
		return fmt.Errorf("historyRange %s: %w", dt.ht.h.filenameBase, err)
	}

	seen := make(map[string]struct{})
	restored := dt.NewWriter()

	for histRng.HasNext() && txNumUnwindTo > 0 {
		k, v, _, err := histRng.Next()
		if err != nil {
			return err
		}

		ic, err := dt.ht.IdxRange(k, int(txNumUnwindTo)-1, 0, order.Desc, -1, rwTx)
		if err != nil {
			return err
		}
		if ic.HasNext() {
			nextTxn, err := ic.Next()
			if err != nil {
				return err
			}
			restored.SetTxNum(nextTxn) // todo what if we actually had to decrease current step to provide correct update?
		} else {
			restored.SetTxNum(txNumUnwindTo - 1)
		}
		//fmt.Printf("[%s] unwinding %x ->'%x'\n", dt.d.filenameBase, k, v)
		if err := restored.addValue(k, nil, v); err != nil {
			return err
		}
		seen[string(k)] = struct{}{}
	}

	keysCursor, err := dt.keysCursor(rwTx)
	if err != nil {
		return err
	}
	keysCursorForDeletes, err := rwTx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain delete cursor: %w", d.filenameBase, err)
	}
	defer keysCursorForDeletes.Close()

	var valsC kv.RwCursor
	valsC, err = rwTx.RwCursor(d.valsTable)
	if err != nil {
		return err
	}
	defer valsC.Close()

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^step)
	var k, v []byte

	for k, v, err = keysCursor.First(); k != nil; k, v, err = keysCursor.Next() {
		if err != nil {
			return fmt.Errorf("iterate over %s domain keys: %w", d.filenameBase, err)
		}
		if !bytes.Equal(v, stepBytes) {
			continue
		}
		if _, replaced := seen[string(k)]; !replaced && txNumUnwindTo > 0 {
			continue
		}

		kk, _, err := valsC.SeekExact(common.Append(k, stepBytes))
		if err != nil {
			return err
		}
		if kk != nil {
			//fmt.Printf("[domain][%s] rm large value %x v %x\n", d.filenameBase, kk, vv)
			if err = valsC.DeleteCurrent(); err != nil {
				return err
			}
		}

		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if _, _, err = keysCursorForDeletes.SeekBothExact(k, v); err != nil {
			return err
		}
		if err = keysCursorForDeletes.DeleteCurrent(); err != nil {
			return err
		}
	}

	logEvery := time.NewTicker(time.Second * 30)
	defer logEvery.Stop()
	if _, err := dt.ht.Prune(ctx, rwTx, txNumUnwindTo, math.MaxUint64, math.MaxUint64, true, false, logEvery); err != nil {
		return fmt.Errorf("[domain][%s] unwinding, prune history to txNum=%d, step %d: %w", dt.d.filenameBase, txNumUnwindTo, step, err)
	}
	return restored.Flush(ctx, rwTx)
}

func (d *Domain) isEmpty(tx kv.Tx) (bool, error) {
	k, err := kv.FirstKey(tx, d.keysTable)
	if err != nil {
		return false, err
	}
	k2, err := kv.FirstKey(tx, d.valsTable)
	if err != nil {
		return false, err
	}
	isEmptyHist, err := d.History.isEmpty(tx)
	if err != nil {
		return false, err
	}
	return k == nil && k2 == nil && isEmptyHist, nil
}

var (
	UseBtree = true // if true, will use btree for all files
)

func (dt *DomainRoTx) getFromFiles(filekey []byte) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error) {
	hi, _ := dt.ht.iit.hashKey(filekey)

	for i := len(dt.files) - 1; i >= 0; i-- {
		if dt.d.indexList&withExistence != 0 {
			//if dt.files[i].src.existence == nil {
			//	panic(dt.files[i].src.decompressor.FileName())
			//}
			if dt.files[i].src.existence != nil {
				if !dt.files[i].src.existence.ContainsHash(hi) {
					if traceGetLatest == dt.d.filenameBase {
						fmt.Printf("GetLatest(%s, %x) -> existence index %s -> false\n", dt.d.filenameBase, filekey, dt.files[i].src.existence.FileName)
					}
					continue
				} else {
					if traceGetLatest == dt.d.filenameBase {
						fmt.Printf("GetLatest(%s, %x) -> existence index %s -> true\n", dt.d.filenameBase, filekey, dt.files[i].src.existence.FileName)
					}
				}
			} else {
				if traceGetLatest == dt.d.filenameBase {
					fmt.Printf("GetLatest(%s, %x) -> existence index is nil %s\n", dt.d.filenameBase, filekey, dt.files[i].src.decompressor.FileName())
				}
			}
		}

		//t := time.Now()
		v, found, err = dt.getFromFile(i, filekey)
		if err != nil {
			return nil, false, 0, 0, err
		}
		if !found {
			if traceGetLatest == dt.d.filenameBase {
				fmt.Printf("GetLatest(%s, %x) -> not found in file %s\n", dt.d.filenameBase, filekey, dt.files[i].src.decompressor.FileName())
			}
			//	LatestStateReadGrindNotFound.ObserveDuration(t)
			continue
		}
		if traceGetLatest == dt.d.filenameBase {
			fmt.Printf("GetLatest(%s, %x) -> found in file %s\n", dt.d.filenameBase, filekey, dt.files[i].src.decompressor.FileName())
		}
		//LatestStateReadGrind.ObserveDuration(t)
		return v, true, dt.files[i].startTxNum, dt.files[i].endTxNum, nil
	}
	if traceGetLatest == dt.d.filenameBase {
		fmt.Printf("GetLatest(%s, %x) -> not found in %d files\n", dt.d.filenameBase, filekey, len(dt.files))
	}

	return nil, false, 0, 0, nil
}

// GetAsOf does not always require usage of roTx. If it is possible to determine
// historical value based only on static files, roTx will not be used.
func (dt *DomainRoTx) GetAsOf(key []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	v, hOk, err := dt.ht.GetNoStateWithRecent(key, txNum, roTx)
	if err != nil {
		return nil, err
	}
	if hOk {
		// if history returned marker of key creation
		// domain must return nil
		if len(v) == 0 {
			if traceGetAsOf == dt.d.filenameBase {
				fmt.Printf("GetAsOf(%s, %x, %d) -> not found in history\n", dt.d.filenameBase, key, txNum)
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
	for i := 0; i < len(files); i++ {
		if files[i].src.frozen {
			continue
		}
		refCnt := files[i].src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && files[i].src.canDelete.Load() {
			files[i].src.closeFilesAndRemove()
		}
	}
	dt.ht.Close()
}

func (dt *DomainRoTx) statelessGetter(i int) ArchiveGetter {
	if dt.getters == nil {
		dt.getters = make([]ArchiveGetter, len(dt.files))
	}
	r := dt.getters[i]
	if r == nil {
		r = NewArchiveGetter(dt.files[i].src.decompressor.MakeGetter(), dt.d.compression)
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
	dt.valsC, err = tx.Cursor(dt.d.valsTable)
	if err != nil {
		return nil, err
	}
	return dt.valsC, nil
}

func (dt *DomainRoTx) keysCursor(tx kv.Tx) (c kv.CursorDupSort, err error) {
	if dt.keysC != nil {
		return dt.keysC, nil
	}
	dt.keysC, err = tx.CursorDupSort(dt.d.keysTable)
	if err != nil {
		return nil, err
	}
	return dt.keysC, nil
}

func (dt *DomainRoTx) getLatestFromDb(key []byte, roTx kv.Tx) ([]byte, uint64, bool, error) {
	keysC, err := dt.keysCursor(roTx)
	if err != nil {
		return nil, 0, false, err
	}
	var v, foundInvStep []byte
	_, foundInvStep, err = keysC.SeekExact(key)
	if err != nil {
		return nil, 0, false, err
	}
	if foundInvStep != nil {
		foundStep := ^binary.BigEndian.Uint64(foundInvStep)
		if LastTxNumOfStep(foundStep, dt.d.aggregationStep) >= dt.maxTxNumInDomainFiles(false) {
			valsC, err := dt.valsCursor(roTx)
			if err != nil {
				return nil, foundStep, false, err
			}
			_, v, err = valsC.SeekExact(append(append(dt.valBuf[:0], key...), foundInvStep...))
			if err != nil {
				return nil, foundStep, false, fmt.Errorf("GetLatest value: %w", err)
			}
			return v, foundStep, true, nil
		}
	}
	//if traceGetLatest == dt.d.filenameBase {
	//	it, err := dt.ht.IdxRange(common.FromHex("0x105083929bF9bb22C26cB1777Ec92661170D4285"), 1390000, -1, order.Asc, -1, roTx) //[from, to)
	//	if err != nil {
	//		panic(err)
	//	}
	//	l := iter.ToArrU64Must(it)
	//	fmt.Printf("L: %d\n", l)
	//	it2, err := dt.ht.IdxRange(common.FromHex("0x105083929bF9bb22C26cB1777Ec92661170D4285"), -1, 1390000, order.Desc, -1, roTx) //[from, to)
	//	if err != nil {
	//		panic(err)
	//	}
	//	l2 := iter.ToArrU64Must(it2)
	//	fmt.Printf("K: %d\n", l2)
	//	panic(1)
	//
	//	fmt.Printf("GetLatest(%s, %x) -> not found in db\n", dt.d.filenameBase, key)
	//}
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

	if traceGetLatest == dt.d.filenameBase {
		defer func() {
			fmt.Printf("GetLatest(%s, '%x' -> '%x') (from db=%t; istep=%x stepInFiles=%d)\n",
				dt.d.filenameBase, key, v, found, foundStep, dt.maxTxNumInDomainFiles(false)/dt.d.aggregationStep)
		}()
	}

	v, foundStep, found, err = dt.getLatestFromDb(key, roTx)
	if err != nil {
		return nil, 0, false, err
	}
	if found {
		return v, foundStep, true, nil
	}

	v, foundInFile, _, endTxNum, err := dt.getFromFiles(key)
	if err != nil {
		return nil, 0, false, err
	}
	return v, endTxNum / dt.d.aggregationStep, foundInFile, nil
}

func (dt *DomainRoTx) GetLatestFromFiles(key []byte) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error) {
	return dt.getFromFiles(key)
}

func (dt *DomainRoTx) IteratePrefix(roTx kv.Tx, prefix []byte, it func(k []byte, v []byte) error) error {
	// Implementation:
	//     File endTxNum  = last txNum of file step
	//     DB endTxNum    = first txNum of step in db
	//     RAM endTxNum   = current txnum
	//  Example: stepSize=8, file=0-2.kv, db has key of step 2, current tx num is 17
	//     File endTxNum  = 15, because `0-2.kv` has steps 0 and 1, last txNum of step 1 is 15
	//     DB endTxNum    = 16, because db has step 2, and first txNum of step 2 is 16.
	//     RAM endTxNum   = 17, because current tcurrent txNum is 17

	var cp CursorHeap
	heap.Init(&cp)
	var k, v []byte
	var err error

	keysCursor, err := roTx.CursorDupSort(dt.d.keysTable)
	if err != nil {
		return err
	}
	defer keysCursor.Close()
	if k, v, err = keysCursor.Seek(prefix); err != nil {
		return err
	}
	if k != nil && bytes.HasPrefix(k, prefix) {
		step := ^binary.BigEndian.Uint64(v)
		endTxNum := step * dt.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files

		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		if v, err = roTx.GetOne(dt.d.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(&cp, &CursorItem{t: DB_CURSOR, key: k, val: v, c: keysCursor, endTxNum: endTxNum, reverse: true})
	}

	for i, item := range dt.files {
		if UseBtree || UseBpsTree {
			cursor, err := dt.statelessBtree(i).Seek(dt.statelessGetter(i), prefix)
			if err != nil {
				return err
			}
			if cursor == nil {
				continue
			}
			dt.d.stats.FilesQueries.Add(1)
			key := cursor.Key()
			if key != nil && bytes.HasPrefix(key, prefix) {
				val := cursor.Value()
				txNum := item.endTxNum - 1 // !important: .kv files have semantic [from, t)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: dt.statelessGetter(i), key: key, val: val, btCursor: cursor, endTxNum: txNum, reverse: true})
			}
		} else {
			offset, ok := dt.statelessIdxReader(i).Lookup(prefix)
			if !ok {
				continue
			}
			g := dt.statelessGetter(i)
			g.Reset(offset)
			if !g.HasNext() {
				continue
			}
			key, _ := g.Next(nil)
			dt.d.stats.FilesQueries.Add(1)
			if key != nil && bytes.HasPrefix(key, prefix) {
				val, lofft := g.Next(nil)
				txNum := item.endTxNum - 1 // !important: .kv files have semantic [from, t)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: g, latestOffset: lofft, key: key, val: val, endTxNum: txNum, reverse: true})
			}
		}
	}

	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(&cp).(*CursorItem)
			switch ci1.t {
			//case RAM_CURSOR:
			//	if ci1.iter.Next() {
			//		k = []byte(ci1.iter.Key())
			//		if k != nil && bytes.HasPrefix(k, prefix) {
			//			ci1.key = common.Copy(k)
			//			ci1.val = common.Copy(ci1.iter.Value())
			//		}
			//	}
			//	heap.Push(&cp, ci1)
			case FILE_CURSOR:
				if UseBtree || UseBpsTree {
					if ci1.btCursor.Next() {
						ci1.key = ci1.btCursor.Key()
						if ci1.key != nil && bytes.HasPrefix(ci1.key, prefix) {
							ci1.val = ci1.btCursor.Value()
							heap.Push(&cp, ci1)
						}
					}
				} else {
					ci1.dg.Reset(ci1.latestOffset)
					if !ci1.dg.HasNext() {
						break
					}
					key, _ := ci1.dg.Next(nil)
					if key != nil && bytes.HasPrefix(key, prefix) {
						ci1.key = key
						ci1.val, ci1.latestOffset = ci1.dg.Next(nil)
						heap.Push(&cp, ci1)
					}
				}
			case DB_CURSOR:
				k, v, err = ci1.c.NextNoDup()
				if err != nil {
					return err
				}
				if k != nil && bytes.HasPrefix(k, prefix) {
					ci1.key = k
					step := ^binary.BigEndian.Uint64(v)
					endTxNum := step * dt.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
					ci1.endTxNum = endTxNum

					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = roTx.GetOne(dt.d.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = v
					heap.Push(&cp, ci1)
				}
			}
		}
		if len(lastVal) > 0 {
			if err := it(lastKey, lastVal); err != nil {
				return err
			}
		}
	}
	return nil
}

func (dt *DomainRoTx) DomainRange(tx kv.Tx, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it iter.KV, err error) {
	if !asc {
		panic("implement me")
	}
	//histStateIt, err := tx.aggCtx.AccountHistoricalStateRange(asOfTs, fromKey, toKey, limit, tx.MdbxTx)
	//if err != nil {
	//	return nil, err
	//}
	//lastestStateIt, err := tx.aggCtx.DomainRangeLatest(tx.MdbxTx, kv.AccountDomain, fromKey, toKey, limit)
	//if err != nil {
	//	return nil, err
	//}
	histStateIt, err := dt.ht.WalkAsOf(ts, fromKey, toKey, tx, limit)
	if err != nil {
		return nil, err
	}
	lastestStateIt, err := dt.DomainRangeLatest(tx, fromKey, toKey, limit)
	if err != nil {
		return nil, err
	}
	return iter.UnionKV(histStateIt, lastestStateIt, limit), nil
}

func (dt *DomainRoTx) IteratePrefix2(roTx kv.Tx, fromKey, toKey []byte, limit int) (iter.KV, error) {
	return dt.DomainRangeLatest(roTx, fromKey, toKey, limit)
}

func (dt *DomainRoTx) DomainRangeLatest(roTx kv.Tx, fromKey, toKey []byte, limit int) (iter.KV, error) {
	fit := &DomainLatestIterFile{from: fromKey, to: toKey, limit: limit, dc: dt,
		roTx:         roTx,
		idxKeysTable: dt.d.keysTable,
		h:            &CursorHeap{},
	}
	if err := fit.init(dt); err != nil {
		return nil, err
	}
	return fit, nil
}

// CanPruneUntil returns true if domain OR history tables can be pruned until txNum
func (dt *DomainRoTx) CanPruneUntil(tx kv.Tx, untilTx uint64) bool {
	canDomain, _ := dt.canPruneDomainTables(tx, untilTx)
	canHistory, _ := dt.ht.canPruneUntil(tx, untilTx)
	return canHistory || canDomain
}

// checks if there is anything to prune in DOMAIN tables.
// everything that aggregated is prunable.
// history.CanPrune should be called separately because it responsible for different tables
func (dt *DomainRoTx) canPruneDomainTables(tx kv.Tx, untilTx uint64) (can bool, maxStepToPrune uint64) {
	if m := dt.maxTxNumInDomainFiles(false); m > 0 {
		maxStepToPrune = (m - 1) / dt.d.aggregationStep
	}
	var untilStep uint64
	if untilTx > 0 {
		untilStep = (untilTx - 1) / dt.d.aggregationStep
	}
	sm := dt.smallestStepForPruning(tx)

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

func (dt *DomainRoTx) smallestStepForPruning(tx kv.Tx) uint64 {
	pkr, err := GetExecV3PruneProgress(tx, dt.d.keysTable)
	if err != nil {
		dt.d.logger.Warn("smallestStepForPruning: failed to get progress", "domain", dt.d.filenameBase, "error", err)
		return math.MaxUint64
	}

	c, err := tx.CursorDupSort(dt.d.keysTable)
	if err != nil {
		dt.d.logger.Warn("smallestStepForPruning: failed to open cursor", "domain", dt.d.filenameBase, "error", err)
		return math.MaxUint64
	}
	defer c.Close()

	var k, v []byte
	minStep := uint64(math.MaxUint64)

	if pkr != nil {
		_, vs, err := c.Seek(pkr)
		if err != nil {
			return math.MaxUint64
		}
		minStep = min(minStep, ^binary.BigEndian.Uint64(vs))

		k, v, err = c.PrevNoDup() //nolint
	} else {
		k, v, err = c.First()
	}
	if k == nil {
		return math.MaxUint64
	}
	if err != nil {
		dt.d.logger.Warn("smallestStepForPruning: failed to seek", "domain", dt.d.filenameBase, "error", err)
		return math.MaxUint64
	}

	minStep = min(minStep, ^binary.BigEndian.Uint64(v))
	fv, err := c.LastDup()
	if err != nil {
		return math.MaxUint64
	}
	return min(minStep, ^binary.BigEndian.Uint64(fv))
}

type DomainPruneStat struct {
	MinStep uint64
	MaxStep uint64
	Values  uint64
	History *InvertedIndexPruneStat
}

func (dc *DomainPruneStat) String() string {
	if dc.MinStep == math.MaxUint64 && dc.Values == 0 {
		if dc.History == nil {
			return ""
		}
		return dc.History.String()
	}
	if dc.History == nil {
		return fmt.Sprintf("%d kv's step %d-%d", dc.Values, dc.MinStep, dc.MaxStep)
	}
	return fmt.Sprintf("%d kv's step %d-%d; v%s", dc.Values, dc.MinStep, dc.MaxStep, dc.History)
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

// TODO test idea. Generate 4 keys with updates for several steps. Count commitment after each prune over 4 known keys.
//   минус локалити - не умеет отсеивать несуществующие ключи, и это не шардед индекс а кросс шардед (1 файл на все кв или еф файлы)

// history prunes keys in range [txFrom; txTo), domain prunes any records with rStep <= step.
// In case of context cancellation pruning stops and returns error, but simply could be started again straight away.
func (dt *DomainRoTx) Warmup(ctx context.Context) (cleanup func()) {
	ctx, cancel := context.WithCancel(ctx)
	wg := &errgroup.Group{}
	wg.Go(func() error {
		backup.WarmupTable(ctx, dt.d.db, dt.d.keysTable, log.LvlDebug, 4)
		return nil
	})
	wg.Go(func() error {
		backup.WarmupTable(ctx, dt.d.db, dt.d.valsTable, log.LvlDebug, 4)
		return nil
	})
	return func() {
		cancel()
		_ = wg.Wait()
	}
}

func (dt *DomainRoTx) Prune(ctx context.Context, rwTx kv.RwTx, step, txFrom, txTo, limit uint64, withWarmup bool, logEvery *time.Ticker) (stat *DomainPruneStat, err error) {
	if limit == 0 {
		limit = math.MaxUint64
	}

	stat = &DomainPruneStat{MinStep: math.MaxUint64}
	if stat.History, err = dt.ht.Prune(ctx, rwTx, txFrom, txTo, limit, false, withWarmup, logEvery); err != nil {
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

	if withWarmup {
		cleanup := dt.Warmup(ctx)
		defer cleanup()
	}

	keysCursorForDeletes, err := rwTx.RwCursorDupSort(dt.d.keysTable)
	if err != nil {
		return stat, fmt.Errorf("create %s domain cursor: %w", dt.d.filenameBase, err)
	}
	defer keysCursorForDeletes.Close()
	keysCursor, err := rwTx.RwCursorDupSort(dt.d.keysTable)
	if err != nil {
		return stat, fmt.Errorf("create %s domain cursor: %w", dt.d.filenameBase, err)
	}
	defer keysCursor.Close()

	//fmt.Printf("prune domain %s from %d to %d step %d limit %d\n", dt.d.filenameBase, txFrom, txTo, step, limit)
	//defer func() {
	//	dt.d.logger.Info("[snapshots] prune domain",
	//		"name", dt.d.filenameBase,
	//		"pruned keys", stat.Values,
	//		"from", txFrom, "to", txTo, "step", step,
	//		"keys until limit", limit)
	//}()
	prunedKey, err := GetExecV3PruneProgress(rwTx, dt.d.keysTable)
	if err != nil {
		dt.d.logger.Error("get domain pruning progress", "name", dt.d.filenameBase, "error", err)
	}

	var k, v []byte
	if prunedKey != nil {
		_, _, err = keysCursor.Seek(prunedKey)
		if err != nil {
			return stat, err
		}
		// could have some smaller steps to prune
		k, v, err = keysCursor.NextNoDup()
	} else {
		k, v, err = keysCursor.Last()
	}
	if err != nil {
		return nil, err
	}

	seek := make([]byte, 0, 256)
	for k != nil {
		if err != nil {
			return stat, fmt.Errorf("iterate over %s domain keys: %w", dt.d.filenameBase, err)
		}

		is := ^binary.BigEndian.Uint64(v)
		if is > step {
			k, v, err = keysCursor.PrevNoDup()
			continue
		}
		if limit == 0 {
			if err := SaveExecV3PruneProgress(rwTx, dt.d.keysTable, k); err != nil {
				return stat, fmt.Errorf("save domain pruning progress: %s, %w", dt.d.filenameBase, err)
			}
			return stat, nil
		}
		limit--

		seek = append(append(seek[:0], k...), v...)
		err = rwTx.Delete(dt.d.valsTable, seek)
		if err != nil {
			return stat, fmt.Errorf("prune domain value: %w", err)
		}

		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if _, _, err = keysCursorForDeletes.SeekBothExact(k, v); err != nil {
			return stat, err
		}
		if err = keysCursorForDeletes.DeleteCurrent(); err != nil {
			return stat, err
		}
		stat.Values++
		stat.MaxStep = max(stat.MaxStep, is)
		stat.MinStep = min(stat.MinStep, is)
		mxPruneSizeDomain.Inc()

		k, v, err = keysCursor.Prev()

		select {
		case <-ctx.Done():
			// consider ctx exiting as incorrect outcome, error is returned
			return stat, ctx.Err()
		case <-logEvery.C:
			dt.d.logger.Info("[snapshots] prune domain", "name", dt.d.filenameBase,
				"pruned keys", stat.Values,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(dt.d.aggregationStep), float64(txTo)/float64(dt.d.aggregationStep)))
		default:
		}
	}
	if err := SaveExecV3PruneProgress(rwTx, dt.d.keysTable, nil); err != nil {
		return stat, fmt.Errorf("save domain pruning progress: %s, %w", dt.d.filenameBase, err)
	}
	mxPruneTookDomain.ObserveDuration(st)
	return stat, nil
}

type DomainLatestIterFile struct {
	dc *DomainRoTx

	roTx         kv.Tx
	idxKeysTable string

	limit int

	from, to []byte
	nextVal  []byte
	nextKey  []byte

	h *CursorHeap

	k, v, kBackup, vBackup []byte
}

func (hi *DomainLatestIterFile) Close() {
}
func (hi *DomainLatestIterFile) init(dc *DomainRoTx) error {
	// Implementation:
	//     File endTxNum  = last txNum of file step
	//     DB endTxNum    = first txNum of step in db
	//     RAM endTxNum   = current txnum
	//  Example: stepSize=8, file=0-2.kv, db has key of step 2, current tx num is 17
	//     File endTxNum  = 15, because `0-2.kv` has steps 0 and 1, last txNum of step 1 is 15
	//     DB endTxNum    = 16, because db has step 2, and first txNum of step 2 is 16.
	//     RAM endTxNum   = 17, because current tcurrent txNum is 17

	heap.Init(hi.h)
	var k, v []byte
	var err error

	keysCursor, err := hi.roTx.CursorDupSort(dc.d.keysTable)
	if err != nil {
		return err
	}
	if k, v, err = keysCursor.Seek(hi.from); err != nil {
		return err
	}
	if k != nil && (hi.to == nil || bytes.Compare(k, hi.to) < 0) {
		step := ^binary.BigEndian.Uint64(v)
		endTxNum := step * dc.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files

		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		if v, err = hi.roTx.GetOne(dc.d.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(hi.h, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), c: keysCursor, endTxNum: endTxNum, reverse: true})
	}

	for i, item := range dc.files {
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
				k, v, err := ci1.c.NextNoDup()
				if err != nil {
					return err
				}
				if k != nil && (hi.to == nil || bytes.Compare(k, hi.to) < 0) {
					ci1.key = common.Copy(k)
					step := ^binary.BigEndian.Uint64(v)
					endTxNum := step * hi.dc.d.aggregationStep // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
					ci1.endTxNum = endTxNum

					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = hi.roTx.GetOne(hi.dc.d.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = common.Copy(v)
					heap.Push(hi.h, ci1)
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
func (d *Domain) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	fst, _ := kv.FirstKey(tx, d.valsTable)
	if len(fst) > 0 {
		to = float64(^binary.BigEndian.Uint64(fst[len(fst)-8:]))
	}
	lst, _ := kv.LastKey(tx, d.valsTable)
	if len(lst) > 0 {
		from = float64(^binary.BigEndian.Uint64(lst[len(lst)-8:]))
	}
	//fmt.Printf("first %x (to %f) - %x (from %f)\n", fst, to, lst, from)
	if to == 0 {
		to = from
	}
	return from, to
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
