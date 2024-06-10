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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/assert"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/log/v3"
	"github.com/spaolacci/murmur3"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

// Appendable - data type allows store data for different blockchain forks.
// - It assign new AutoIncrementID to each entity. Example: receipts, logs.
// - Each record key has `AutoIncrementID` format.
// - Use external table to refer it.
// - Only data which belongs to `canonical` block moving from DB to files.
// - It doesn't need Unwind
type Appendable struct {
	cfg AppendableCfg

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

	table           string // txnNum_u64 -> key (k+auto_increment)
	filenameBase    string
	aggregationStep uint64

	//TODO: re-visit this check - maybe we don't need it. It's abot kill in the middle of merge
	integrityCheck func(fromStep, toStep uint64) bool

	// fields for history write
	logger log.Logger

	noFsync bool // fsync is enabled by default, but tests can manually disable

	compression     FileCompression
	compressWorkers int
	indexList       idxList
}

type AppendableCfg struct {
	Salt *uint32
	Dirs datadir.Dirs
	DB   kv.RoDB // global db pointer. mostly for background warmup.

	CanonicalMarkersTable string

	iters IterFactory
}

func NewAppendable(cfg AppendableCfg, aggregationStep uint64, filenameBase, table string, integrityCheck func(fromStep uint64, toStep uint64) bool, logger log.Logger) (*Appendable, error) {
	if cfg.Dirs.SnapDomain == "" {
		panic("empty `dirs` varialbe")
	}
	fk := Appendable{
		cfg:             cfg,
		dirtyFiles:      btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
		table:           table,
		compressWorkers: 1,
		integrityCheck:  integrityCheck,
		logger:          logger,
		compression:     CompressNone, //CompressKeys | CompressVals,
	}
	fk.indexList = withHashMap
	fk._visibleFiles = []ctxItem{}

	return &fk, nil
}

func (fk *Appendable) fkAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(fk.cfg.Dirs.SnapAccessors, fmt.Sprintf("v1-%s.%d-%d.api", fk.filenameBase, fromStep, toStep))
}
func (fk *Appendable) fkFilePath(fromStep, toStep uint64) string {
	return filepath.Join(fk.cfg.Dirs.SnapHistory, fmt.Sprintf("v1-%s.%d-%d.ap", fk.filenameBase, fromStep, toStep))
}

func (fk *Appendable) fileNamesOnDisk() (idx, hist, domain []string, err error) {
	idx, err = filesFromDir(fk.cfg.Dirs.SnapIdx)
	if err != nil {
		return
	}
	hist, err = filesFromDir(fk.cfg.Dirs.SnapHistory)
	if err != nil {
		return
	}
	domain, err = filesFromDir(fk.cfg.Dirs.SnapDomain)
	if err != nil {
		return
	}
	return
}

func (fk *Appendable) OpenList(fNames []string, readonly bool) error {
	fk.closeWhatNotInList(fNames)
	fk.scanStateFiles(fNames)
	if err := fk.openFiles(); err != nil {
		return fmt.Errorf("NewHistory.openFiles: %w, %s", err, fk.filenameBase)
	}
	_ = readonly // for future safety features. RPCDaemon must not delte files
	return nil
}

func (fk *Appendable) OpenFolder(readonly bool) error {
	idxFiles, _, _, err := fk.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return fk.OpenList(idxFiles, readonly)
}

func (fk *Appendable) scanStateFiles(fileNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^v([0-9]+)-" + fk.filenameBase + ".([0-9]+)-([0-9]+).ef$")
	var err error
	for _, name := range fileNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				fk.logger.Warn("File ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			fk.logger.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			fk.logger.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			fk.logger.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*fk.aggregationStep, endStep*fk.aggregationStep
		var newFile = newFilesItem(startTxNum, endTxNum, fk.aggregationStep)

		if fk.integrityCheck != nil && !fk.integrityCheck(startStep, endStep) {
			continue
		}

		if _, has := fk.dirtyFiles.Get(newFile); has {
			continue
		}

		fk.dirtyFiles.Set(newFile)
	}
	return garbageFiles
}

func (fk *Appendable) reCalcVisibleFiles() {
	fk._visibleFiles = calcVisibleFiles(fk.dirtyFiles, fk.indexList, false)
}

func (fk *Appendable) missedIdxFiles() (l []*filesItem) {
	fk.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/fk.aggregationStep, item.endTxNum/fk.aggregationStep
			if !dir.FileExist(fk.fkAccessorFilePath(fromStep, toStep)) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

func (fk *Appendable) buildIdx(ctx context.Context, fromStep, toStep uint64, d *seg.Decompressor, ps *background.ProgressSet) error {
	if d == nil {
		return fmt.Errorf("buildIdx: passed item with nil decompressor %s %d-%d", fk.filenameBase, fromStep, toStep)
	}
	idxPath := fk.fkAccessorFilePath(fromStep, toStep)
	cfg := recsplit.RecSplitArgs{
		Enums: true,

		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     fk.cfg.Dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       fk.cfg.Salt,
		NoFsync:    fk.noFsync,

		KeyCount: d.Count(),
	}
	_, fileName := filepath.Split(idxPath)
	count := d.Count()
	p := ps.AddNew(fileName, uint64(count))
	defer ps.Delete(p)

	num := make([]byte, binary.MaxVarintLen64)
	return buildSimpleIndex(ctx, d, cfg, fk.logger, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if p != nil {
			p.Processed.Add(1)
		}
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	})
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (fk *Appendable) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	for _, item := range fk.missedIdxFiles() {
		item := item
		g.Go(func() error {
			fromStep, toStep := item.startTxNum/fk.aggregationStep, item.endTxNum/fk.aggregationStep
			return fk.buildIdx(ctx, fromStep, toStep, item.decompressor, ps)
		})
	}
}

func (fk *Appendable) openFiles() error {
	var invalidFileItems []*filesItem
	invalidFileItemsLock := sync.Mutex{}
	fk.dirtyFiles.Walk(func(items []*filesItem) bool {
		var err error
		for _, item := range items {
			item := item
			fromStep, toStep := item.startTxNum/fk.aggregationStep, item.endTxNum/fk.aggregationStep
			if item.decompressor == nil {
				fPath := fk.fkFilePath(fromStep, toStep)
				if !dir.FileExist(fPath) {
					_, fName := filepath.Split(fPath)
					fk.logger.Debug("[agg] InvertedIndex.openFiles: file does not exists", "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						fk.logger.Debug("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
					} else {
						fk.logger.Warn("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
					}
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil {
				fPath := fk.fkAccessorFilePath(fromStep, toStep)
				if dir.FileExist(fPath) {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						fk.logger.Warn("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
		}

		return true
	})
	for _, item := range invalidFileItems {
		item.closeFiles()
		fk.dirtyFiles.Delete(item)
	}

	return nil
}

func (fk *Appendable) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	fk.dirtyFiles.Walk(func(items []*filesItem) bool {
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
		fk.dirtyFiles.Delete(item)
	}
}

func (fk *Appendable) Close() {
	fk.closeWhatNotInList([]string{})
}

// DisableFsync - just for tests
func (fk *Appendable) DisableFsync() { fk.noFsync = true }

func (tx *AppendableRoTx) Files() (res []string) {
	for _, item := range tx.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return res
}

func (tx *AppendableRoTx) Get(ts uint64, dbtx kv.Tx) ([]byte, bool, error) {
	v, ok := tx.getFromFiles(ts)
	if ok {
		return v, true, nil
	}
	return tx.fk.getFromDBByTs(ts, dbtx)
}

func (tx *AppendableRoTx) getFromFiles(ts uint64) (v []byte, ok bool) {
	i, ok := tx.fileByTS(ts)
	if !ok {
		return nil, false
	}

	lookup := ts - tx.files[i].startTxNum
	idx := tx.files[i].src.index
	if idx.KeyCount() <= lookup {
		return nil, false
	}
	offset := idx.OrdinalLookup(ts - tx.files[i].startTxNum)
	g := tx.statelessGetter(i)
	g.Reset(offset)
	k, _ := g.Next(nil)
	return k, true
}

func (tx *AppendableRoTx) fileByTS(ts uint64) (i int, ok bool) {
	for i = 0; i < len(tx.files); i++ {
		if tx.files[i].hasTS(ts) {
			return i, true
		}
	}
	return 0, false
}

func (tx *AppendableRoTx) Put(ts uint64, v []byte, dbtx kv.RwTx) error {
	return dbtx.Put(tx.fk.table, hexutility.EncodeTs(ts), v)
}

func (fk *Appendable) getFromDBByTs(ts uint64, dbtx kv.Tx) ([]byte, bool, error) {
	return fk.getFromDB(hexutility.EncodeTs(ts), dbtx)
}
func (fk *Appendable) getFromDB(k []byte, dbtx kv.Tx) ([]byte, bool, error) {
	v, err := dbtx.GetOne(fk.table, k)
	if err != nil {
		return nil, false, err
	}
	return v, v != nil, err
}

// Add - !NotThreadSafe. Must use WalRLock/BatchHistoryWriteEnd
func (w *appendableBufferedWriter) Add(blockNum uint64, blockHash common.Hash, v []byte) error {
	if w.discard {
		return nil
	}
	k := make([]byte, length.BlockNum+length.Hash)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[length.BlockNum:], blockHash[:])
	if err := w.tableCollector.Collect(k, v); err != nil {
		return err
	}
	return nil
}

func (tx *AppendableRoTx) NewWriter() *appendableBufferedWriter {
	return tx.newWriter(tx.fk.cfg.Dirs.Tmp, false)
}

type appendableBufferedWriter struct {
	tableCollector *etl.Collector
	tmpdir         string
	discard        bool
	filenameBase   string

	table string

	aggregationStep uint64

	//current TimeStamp - BlockNum or TxNum
	timestamp      uint64
	timestampBytes [8]byte
}

func (w *appendableBufferedWriter) SetTimeStamp(ts uint64) {
	w.timestamp = ts
	binary.BigEndian.PutUint64(w.timestampBytes[:], w.timestamp)
}

func (w *appendableBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}
	if err := w.tableCollector.Load(tx, w.table, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	w.close()
	return nil
}

func (w *appendableBufferedWriter) close() {
	if w == nil {
		return
	}
	if w.tableCollector != nil {
		w.tableCollector.Close()
	}
}

func (tx *AppendableRoTx) newWriter(tmpdir string, discard bool) *appendableBufferedWriter {
	w := &appendableBufferedWriter{
		discard:         discard,
		tmpdir:          tmpdir,
		filenameBase:    tx.fk.filenameBase,
		aggregationStep: tx.fk.aggregationStep,

		table: tx.fk.table,
		// etl collector doesn't fsync: means if have enough ram, all files produced by all collectors will be in ram
		tableCollector: etl.NewCollector("flush "+tx.fk.table, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), tx.fk.logger),
	}
	w.tableCollector.LogLvl(log.LvlTrace)
	w.tableCollector.SortAndFlushInBackground(true)
	return w
}

func (fk *Appendable) BeginFilesRo() *AppendableRoTx {
	files := fk._visibleFiles
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}
	return &AppendableRoTx{
		fk:    fk,
		files: files,
	}
}

func (tx *AppendableRoTx) Close() {
	if tx.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := tx.files
	tx.files = nil
	for i := 0; i < len(files); i++ {
		if files[i].src.frozen {
			continue
		}
		refCnt := files[i].src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && files[i].src.canDelete.Load() {
			if tx.fk.filenameBase == traceFileLife {
				tx.fk.logger.Warn(fmt.Sprintf("[agg] real remove at ctx close: %s", files[i].src.decompressor.FileName()))
			}
			files[i].src.closeFilesAndRemove()
		}
	}

	for _, r := range tx.readers {
		r.Close()
	}
}

type AppendableRoTx struct {
	fk      *Appendable
	files   []ctxItem // have no garbage (overlaps, etc...)
	getters []ArchiveGetter
	readers []*recsplit.IndexReader

	_hasher murmur3.Hash128
}

func (tx *AppendableRoTx) statelessHasher() murmur3.Hash128 {
	if tx._hasher == nil {
		tx._hasher = murmur3.New128WithSeed(*tx.fk.cfg.Salt)
	}
	return tx._hasher
}
func (tx *AppendableRoTx) hashKey(k []byte) (hi, lo uint64) {
	hasher := tx.statelessHasher()
	tx._hasher.Reset()
	_, _ = hasher.Write(k) //nolint:errcheck
	return hasher.Sum128()
}

func (tx *AppendableRoTx) statelessGetter(i int) ArchiveGetter {
	if tx.getters == nil {
		tx.getters = make([]ArchiveGetter, len(tx.files))
	}
	r := tx.getters[i]
	if r == nil {
		g := tx.files[i].src.decompressor.MakeGetter()
		r = NewArchiveGetter(g, tx.fk.compression)
		tx.getters[i] = r
	}
	return r
}
func (tx *AppendableRoTx) statelessIdxReader(i int) *recsplit.IndexReader {
	if tx.readers == nil {
		tx.readers = make([]*recsplit.IndexReader, len(tx.files))
	}
	r := tx.readers[i]
	if r == nil {
		r = tx.files[i].src.index.GetReaderFromPool()
		tx.readers[i] = r
	}
	return r
}

func (tx *AppendableRoTx) smallestTxNum(dbtx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(dbtx, tx.fk.table)
	if len(fst) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		return min(fstInDb, math.MaxUint64)
	}
	return math.MaxUint64
}

func (tx *AppendableRoTx) highestTxNum(dbtx kv.Tx) uint64 {
	lst, _ := kv.LastKey(dbtx, tx.fk.table)
	if len(lst) > 0 {
		lstInDb := binary.BigEndian.Uint64(lst)
		return max(lstInDb, 0)
	}
	return 0
}

func (tx *AppendableRoTx) CanPrune(dbtx kv.Tx) bool {
	return tx.smallestTxNum(dbtx) < tx.maxTxNumInFiles(false)
}

func (tx *AppendableRoTx) maxTxNumInFiles(cold bool) uint64 {
	if len(tx.files) == 0 {
		return 0
	}
	if !cold {
		return tx.files[len(tx.files)-1].endTxNum
	}
	for i := len(tx.files) - 1; i >= 0; i-- {
		if !tx.files[i].src.frozen {
			continue
		}
		return tx.files[i].endTxNum
	}
	return 0
}

type AppendablePruneStat struct {
	MinTxNum         uint64
	MaxTxNum         uint64
	PruneCountTx     uint64
	PruneCountValues uint64
}

func (is *AppendablePruneStat) String() string {
	if is.MinTxNum == math.MaxUint64 && is.PruneCountTx == 0 {
		return ""
	}
	return fmt.Sprintf("ii %d txs and %d vals in %.2fM-%.2fM", is.PruneCountTx, is.PruneCountValues, float64(is.MinTxNum)/1_000_000.0, float64(is.MaxTxNum)/1_000_000.0)
}

func (is *AppendablePruneStat) Accumulate(other *InvertedIndexPruneStat) {
	if other == nil {
		return
	}
	is.MinTxNum = min(is.MinTxNum, other.MinTxNum)
	is.MaxTxNum = max(is.MaxTxNum, other.MaxTxNum)
	is.PruneCountTx += other.PruneCountTx
	is.PruneCountValues += other.PruneCountValues
}

// [txFrom; txTo)
// forced - prune even if CanPrune returns false, so its true only when we do Unwind.
func (tx *AppendableRoTx) Prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced, withWarmup bool, fn func(key []byte, txnum []byte) error) (stat *AppendablePruneStat, err error) {
	stat = &AppendablePruneStat{MinTxNum: math.MaxUint64}
	if !forced && !tx.CanPrune(rwTx) {
		return stat, nil
	}

	_ = withWarmup

	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()
	defer func(t time.Time) { mxPruneTookIndex.ObserveDuration(t) }(time.Now())

	if limit == 0 {
		limit = math.MaxUint64
	}

	ii := tx.fk
	//defer func() {
	//	ii.logger.Error("[snapshots] prune index",
	//		"name", ii.filenameBase,
	//		"forced", forced,
	//		"pruned tx", fmt.Sprintf("%.2f-%.2f", float64(minTxnum)/float64(iit.ii.aggregationStep), float64(maxTxnum)/float64(iit.ii.aggregationStep)),
	//		"pruned values", pruneCount,
	//		"tx until limit", limit)
	//}()

	keysCursor, err := rwTx.RwCursorDupSort(tx.fk.table)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", tx.fk.filenameBase, err)
	}
	defer keysCursor.Close()
	keysCursorForDel, err := rwTx.RwCursorDupSort(tx.fk.table)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursorForDel.Close()
	idxC, err := rwTx.RwCursorDupSort(ii.table)
	if err != nil {
		return nil, err
	}
	defer idxC.Close()
	idxValuesCount, err := idxC.Count()
	if err != nil {
		return nil, err
	}
	indexWithValues := idxValuesCount != 0 || fn != nil

	collector := etl.NewCollector("snapshots", ii.cfg.Dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize/8), ii.logger)
	defer collector.Close()
	collector.LogLvl(log.LvlDebug)
	collector.SortAndFlushInBackground(true)

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", ii.filenameBase, err)
		}

		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo || limit == 0 {
			break
		}
		if txNum < txFrom {
			panic(fmt.Errorf("assert: index pruning txn=%d [%d-%d)", txNum, txFrom, txTo))
		}
		limit--
		stat.MinTxNum = min(stat.MinTxNum, txNum)
		stat.MaxTxNum = max(stat.MaxTxNum, txNum)

		if indexWithValues {
			for ; v != nil; _, v, err = keysCursor.NextDup() {
				if err != nil {
					return nil, fmt.Errorf("iterate over %s index keys: %w", ii.filenameBase, err)
				}
				if err := collector.Collect(v, k); err != nil {
					return nil, err
				}
			}
		}

		stat.PruneCountTx++
		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if err = rwTx.Delete(ii.table, k); err != nil {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if !indexWithValues {
		return stat, nil
	}

	idxCForDeletes, err := rwTx.RwCursorDupSort(ii.table)
	if err != nil {
		return nil, err
	}
	defer idxCForDeletes.Close()

	binary.BigEndian.PutUint64(txKey[:], txFrom)
	err = collector.Load(nil, "", func(key, txnm []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if fn != nil {
			if err = fn(key, txnm); err != nil {
				return fmt.Errorf("fn error: %w", err)
			}
		}
		if idxValuesCount > 0 {
			if err = idxCForDeletes.DeleteExact(key, txnm); err != nil {
				return err
			}
		}
		mxPruneSizeIndex.Inc()
		stat.PruneCountValues++

		select {
		case <-logEvery.C:
			txNum := binary.BigEndian.Uint64(txnm)
			ii.logger.Info("[snapshots] prune index", "name", ii.filenameBase, "pruned tx", stat.PruneCountTx,
				"pruned values", stat.PruneCountValues,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(ii.aggregationStep), float64(txNum)/float64(ii.aggregationStep)))
		default:
		}
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})

	return stat, err
}

func (tx *AppendableRoTx) DebugEFAllValuesAreInRange(ctx context.Context) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	iterStep := func(item ctxItem) error {
		g := item.src.decompressor.MakeGetter()
		g.Reset(0)
		defer item.src.decompressor.EnableReadAhead().DisableReadAhead()

		for g.HasNext() {
			k, _ := g.NextUncompressed()
			_ = k
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			if ef.Count() == 0 {
				continue
			}
			if item.startTxNum > ef.Min() {
				err := fmt.Errorf("DebugEFAllValuesAreInRange1: %d > %d, %s, %x", item.startTxNum, ef.Min(), g.FileName(), k)
				log.Warn(err.Error())
				//return err
			}
			if item.endTxNum < ef.Max() {
				err := fmt.Errorf("DebugEFAllValuesAreInRange2: %d < %d, %s, %x", item.endTxNum, ef.Max(), g.FileName(), k)
				log.Warn(err.Error())
				//return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[integrity] EFAllValuesAreInRange: %s, k=%x", g.FileName(), k))
			default:
			}
		}
		return nil
	}

	for _, item := range tx.files {
		if item.src.decompressor == nil {
			continue
		}
		if err := iterStep(item); err != nil {
			return err
		}
		//log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d\n", item.src.decompressor.FileName(), ef.Min(), ef.Max(), last2, iter.ToArrU64Must(ef.Iterator())))
	}
	return nil
}

func (fk *Appendable) collate(ctx context.Context, step uint64, roTx kv.Tx) (AppendableCollation, error) {
	stepTo := step + 1
	txFrom, txTo := step*fk.aggregationStep, stepTo*fk.aggregationStep
	start := time.Now()
	defer mxCollateTookIndex.ObserveDuration(start)

	var (
		coll = AppendableCollation{
			iiPath: fk.fkFilePath(step, stepTo),
		}
		closeComp bool
	)
	defer func() {
		if closeComp {
			coll.Close()
		}
	}()
	comp, err := seg.NewCompressor(ctx, "collate "+fk.filenameBase, coll.iiPath, fk.cfg.Dirs.Tmp, seg.MinPatternScore, fk.compressWorkers, log.LvlTrace, fk.logger)
	if err != nil {
		return AppendableCollation{}, fmt.Errorf("create %s compressor: %w", fk.filenameBase, err)
	}
	coll.writer = NewArchiveWriter(comp, fk.compression)

	it, err := fk.cfg.iters.TxnIdsOfCanonicalBlocks(roTx, int(txFrom), int(txTo), order.Asc, -1)
	if err != nil {
		return AppendableCollation{}, fmt.Errorf("collate %s: %w", fk.filenameBase, err)
	}
	defer it.Close()

	for it.HasNext() {
		k, err := it.Next()
		if err != nil {
			return AppendableCollation{}, fmt.Errorf("collate %s: %w", fk.filenameBase, err)
		}
		v, ok, err := fk.getFromDBByTs(k, roTx)
		if err != nil {
			return AppendableCollation{}, fmt.Errorf("collate %s: %w", fk.filenameBase, err)
		}
		if !ok {
			fmt.Printf("collate: %d, not found\n", k)
			continue
		}
		fmt.Printf("collate: %d, %x\n", k, v)
		if err = coll.writer.AddWord(v); err != nil {
			return AppendableCollation{}, fmt.Errorf("collate %s: %w", fk.filenameBase, err)
		}
	}

	closeComp = false
	return coll, nil
}

func (fk *Appendable) stepsRangeInDBAsStr(tx kv.Tx) string {
	a1, a2 := fk.stepsRangeInDB(tx)
	return fmt.Sprintf("%s: %.1f", fk.filenameBase, a2-a1)
}
func (fk *Appendable) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	fst, _ := kv.FirstKey(tx, fk.table)
	if len(fst) > 0 {
		from = float64(binary.BigEndian.Uint64(fst)) / float64(fk.aggregationStep)
	}
	lst, _ := kv.LastKey(tx, fk.table)
	if len(lst) > 0 {
		to = float64(binary.BigEndian.Uint64(lst)) / float64(fk.aggregationStep)
	}
	if to == 0 {
		to = from
	}
	return from, to
}

type AppendableFiles struct {
	decomp *seg.Decompressor
	index  *recsplit.Index
}

func (sf AppendableFiles) CleanupOnError() {
	if sf.decomp != nil {
		sf.decomp.Close()
	}
	if sf.index != nil {
		sf.index.Close()
	}
}

type AppendableCollation struct {
	iiPath string
	writer ArchiveWriter
}

func (ic AppendableCollation) Close() {
	if ic.writer != nil {
		ic.writer.Close()
	}
}

// buildFiles - `step=N` means build file `[N:N+1)` which is equal to [N:N+1)
func (fk *Appendable) buildFiles(ctx context.Context, step uint64, coll AppendableCollation, ps *background.ProgressSet) (AppendableFiles, error) {
	var (
		decomp *seg.Decompressor
		index  *recsplit.Index
		err    error
	)
	mxRunningFilesBuilding.Inc()
	defer mxRunningFilesBuilding.Dec()
	closeComp := true
	defer func() {
		if closeComp {
			coll.Close()
			if decomp != nil {
				decomp.Close()
			}
			if index != nil {
				index.Close()
			}
		}
	}()

	if assert.Enable {
		if coll.iiPath == "" && reflect.ValueOf(coll.writer).IsNil() {
			panic("assert: collation is not initialized " + fk.filenameBase)
		}
	}

	{
		p := ps.AddNew(path.Base(coll.iiPath), 1)
		if err = coll.writer.Compress(); err != nil {
			ps.Delete(p)
			return AppendableFiles{}, fmt.Errorf("compress %s: %w", fk.filenameBase, err)
		}
		coll.Close()
		ps.Delete(p)
	}

	if decomp, err = seg.NewDecompressor(coll.iiPath); err != nil {
		return AppendableFiles{}, fmt.Errorf("open %s decompressor: %w", fk.filenameBase, err)
	}

	if err := fk.buildIdx(ctx, step, step+1, decomp, ps); err != nil {
		return AppendableFiles{}, fmt.Errorf("build %s api: %w", fk.filenameBase, err)
	}
	if index, err = recsplit.OpenIndex(fk.fkAccessorFilePath(step, step+1)); err != nil {
		return AppendableFiles{}, err
	}

	closeComp = false
	return AppendableFiles{decomp: decomp, index: index}, nil
}

func (fk *Appendable) integrateDirtyFiles(sf AppendableFiles, txNumFrom, txNumTo uint64) {
	fi := newFilesItem(txNumFrom, txNumTo, fk.aggregationStep)
	fi.decompressor = sf.decomp
	fi.index = sf.index
	fk.dirtyFiles.Set(fi)
}

func (fk *Appendable) collectFilesStat() (filesCount, filesSize, idxSize uint64) {
	if fk.dirtyFiles == nil {
		return 0, 0, 0
	}
	fk.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil {
				return false
			}
			filesSize += uint64(item.decompressor.Size())
			idxSize += uint64(item.index.Size())
			idxSize += uint64(item.bindex.Size())
			filesCount += 3
		}
		return true
	})
	return filesCount, filesSize, idxSize
}
