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
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/assert"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv/backup"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/log/v3"
	"github.com/spaolacci/murmur3"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

type InvertedIndex struct {
	iiCfg

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

	indexKeysTable  string // txnNum_u64 -> key (k+auto_increment)
	indexTable      string // k -> txnNum_u64 , Needs to be table with DupSort
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

type iiCfg struct {
	salt *uint32
	dirs datadir.Dirs
	db   kv.RoDB // global db pointer. mostly for background warmup.
}

func NewInvertedIndex(cfg iiCfg, aggregationStep uint64, filenameBase, indexKeysTable, indexTable string, integrityCheck func(fromStep uint64, toStep uint64) bool, logger log.Logger) (*InvertedIndex, error) {
	if cfg.dirs.SnapDomain == "" {
		panic("empty `dirs` varialbe")
	}
	ii := InvertedIndex{
		iiCfg:           cfg,
		dirtyFiles:      btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
		indexKeysTable:  indexKeysTable,
		indexTable:      indexTable,
		compressWorkers: 1,
		integrityCheck:  integrityCheck,
		logger:          logger,
		compression:     CompressNone,
	}
	ii.indexList = withHashMap

	ii._visibleFiles = []ctxItem{}

	return &ii, nil
}

func (ii *InvertedIndex) efAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(ii.dirs.SnapAccessors, fmt.Sprintf("v1-%s.%d-%d.efi", ii.filenameBase, fromStep, toStep))
}
func (ii *InvertedIndex) efFilePath(fromStep, toStep uint64) string {
	return filepath.Join(ii.dirs.SnapIdx, fmt.Sprintf("v1-%s.%d-%d.ef", ii.filenameBase, fromStep, toStep))
}

func filesFromDir(dir string) ([]string, error) {
	allFiles, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("filesFromDir: %w, %s", err, dir)
	}
	filtered := make([]string, 0, len(allFiles))
	for _, f := range allFiles {
		if f.IsDir() || !f.Type().IsRegular() {
			continue
		}
		filtered = append(filtered, f.Name())
	}
	return filtered, nil
}
func (ii *InvertedIndex) fileNamesOnDisk() (idx, hist, domain []string, err error) {
	idx, err = filesFromDir(ii.dirs.SnapIdx)
	if err != nil {
		return
	}
	hist, err = filesFromDir(ii.dirs.SnapHistory)
	if err != nil {
		return
	}
	domain, err = filesFromDir(ii.dirs.SnapDomain)
	if err != nil {
		return
	}
	return
}

func (ii *InvertedIndex) OpenList(fNames []string, readonly bool) error {
	ii.closeWhatNotInList(fNames)
	ii.scanStateFiles(fNames)
	if err := ii.openFiles(); err != nil {
		return fmt.Errorf("NewHistory.openFiles: %w, %s", err, ii.filenameBase)
	}
	_ = readonly // for future safety features. RPCDaemon must not delte files
	return nil
}

func (ii *InvertedIndex) OpenFolder(readonly bool) error {
	idxFiles, _, _, err := ii.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return ii.OpenList(idxFiles, readonly)
}

func (ii *InvertedIndex) scanStateFiles(fileNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^v([0-9]+)-" + ii.filenameBase + ".([0-9]+)-([0-9]+).ef$")
	var err error
	for _, name := range fileNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				ii.logger.Warn("File ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			ii.logger.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			ii.logger.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			ii.logger.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*ii.aggregationStep, endStep*ii.aggregationStep
		var newFile = newFilesItem(startTxNum, endTxNum, ii.aggregationStep)

		if ii.integrityCheck != nil && !ii.integrityCheck(startStep, endStep) {
			continue
		}

		if _, has := ii.dirtyFiles.Get(newFile); has {
			continue
		}

		ii.dirtyFiles.Set(newFile)
	}
	return garbageFiles
}

type idxList int

var (
	withBTree     idxList = 0b1
	withHashMap   idxList = 0b10
	withExistence idxList = 0b100
)

func (ii *InvertedIndex) reCalcVisibleFiles() {
	ii._visibleFiles = calcVisibleFiles(ii.dirtyFiles, ii.indexList, false)
}

func (ii *InvertedIndex) missedIdxFiles() (l []*filesItem) {
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
			if !dir.FileExist(ii.efAccessorFilePath(fromStep, toStep)) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

func (ii *InvertedIndex) buildEfi(ctx context.Context, item *filesItem, ps *background.ProgressSet) (err error) {
	if item.decompressor == nil {
		return fmt.Errorf("buildEfi: passed item with nil decompressor %s %d-%d", ii.filenameBase, item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
	}
	fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
	return ii.buildMapIdx(ctx, fromStep, toStep, item.decompressor, ps)
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (ii *InvertedIndex) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	for _, item := range ii.missedIdxFiles() {
		item := item
		g.Go(func() error {
			return ii.buildEfi(ctx, item, ps)
		})
	}

}

func (ii *InvertedIndex) openFiles() error {
	var invalidFileItems []*filesItem
	invalidFileItemsLock := sync.Mutex{}
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		var err error
		for _, item := range items {
			item := item
			fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
			if item.decompressor == nil {
				fPath := ii.efFilePath(fromStep, toStep)
				if !dir.FileExist(fPath) {
					_, fName := filepath.Split(fPath)
					ii.logger.Debug("[agg] InvertedIndex.openFiles: file does not exists", "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						ii.logger.Debug("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
					} else {
						ii.logger.Warn("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
					}
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil {
				fPath := ii.efAccessorFilePath(fromStep, toStep)
				if dir.FileExist(fPath) {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						ii.logger.Warn("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
		}

		return true
	})
	for _, item := range invalidFileItems {
		item.closeFiles()
		ii.dirtyFiles.Delete(item)
	}

	return nil
}

func (ii *InvertedIndex) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
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
		ii.dirtyFiles.Delete(item)
	}
}

func (ii *InvertedIndex) Close() {
	ii.closeWhatNotInList([]string{})
}

// DisableFsync - just for tests
func (ii *InvertedIndex) DisableFsync() { ii.noFsync = true }

func (iit *InvertedIndexRoTx) Files() (res []string) {
	for _, item := range iit.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return res
}

// Add - !NotThreadSafe. Must use WalRLock/BatchHistoryWriteEnd
func (w *invertedIndexBufferedWriter) Add(key []byte) error {
	return w.add(key, key)
}

func (iit *InvertedIndexRoTx) NewWriter() *invertedIndexBufferedWriter {
	return iit.newWriter(iit.ii.dirs.Tmp, false)
}

type invertedIndexBufferedWriter struct {
	index, indexKeys *etl.Collector
	tmpdir           string
	discard          bool
	filenameBase     string

	indexTable, indexKeysTable string

	txNum           uint64
	aggregationStep uint64
	txNumBytes      [8]byte
}

// loadFunc - is analog of etl.Identity, but it signaling to etl - use .Put instead of .AppendDup - to allow duplicates
// maybe in future we will improve etl, to sort dupSort values in the way that allow use .AppendDup
func loadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	return next(k, k, v)
}

func (w *invertedIndexBufferedWriter) SetTxNum(txNum uint64) {
	w.txNum = txNum
	binary.BigEndian.PutUint64(w.txNumBytes[:], w.txNum)
}

func (w *invertedIndexBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}
	if err := w.index.Load(tx, w.indexTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := w.indexKeys.Load(tx, w.indexKeysTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	w.close()
	return nil
}

func (w *invertedIndexBufferedWriter) close() {
	if w == nil {
		return
	}
	if w.index != nil {
		w.index.Close()
	}
	if w.indexKeys != nil {
		w.indexKeys.Close()
	}
}

// 3_domains * 2 + 3_history * 1 + 4_indices * 2 = 17 etl collectors, 17*(256Mb/8) = 512Mb - for all collectros
var WALCollectorRAM = dbg.EnvDataSize("AGG_WAL_RAM", etl.BufferOptimalSize/8)
var CollateETLRAM = dbg.EnvDataSize("AGG_COLLATE_RAM", etl.BufferOptimalSize/4)

func (iit *InvertedIndexRoTx) newWriter(tmpdir string, discard bool) *invertedIndexBufferedWriter {
	w := &invertedIndexBufferedWriter{
		discard:         discard,
		tmpdir:          tmpdir,
		filenameBase:    iit.ii.filenameBase,
		aggregationStep: iit.ii.aggregationStep,

		indexKeysTable: iit.ii.indexKeysTable,
		indexTable:     iit.ii.indexTable,
		// etl collector doesn't fsync: means if have enough ram, all files produced by all collectors will be in ram
		indexKeys: etl.NewCollector("flush "+iit.ii.indexKeysTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), iit.ii.logger),
		index:     etl.NewCollector("flush "+iit.ii.indexTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), iit.ii.logger),
	}
	w.indexKeys.LogLvl(log.LvlTrace)
	w.index.LogLvl(log.LvlTrace)
	w.indexKeys.SortAndFlushInBackground(true)
	w.index.SortAndFlushInBackground(true)
	return w
}

func (w *invertedIndexBufferedWriter) add(key, indexKey []byte) error {
	if w.discard {
		return nil
	}
	if err := w.indexKeys.Collect(w.txNumBytes[:], key); err != nil {
		return err
	}
	if err := w.index.Collect(indexKey, w.txNumBytes[:]); err != nil {
		return err
	}
	return nil
}

func (ii *InvertedIndex) BeginFilesRo() *InvertedIndexRoTx {
	files := ii._visibleFiles
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}
	return &InvertedIndexRoTx{
		ii:    ii,
		files: files,
	}
}
func (iit *InvertedIndexRoTx) Close() {
	if iit.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := iit.files
	iit.files = nil
	for i := 0; i < len(files); i++ {
		if files[i].src.frozen {
			continue
		}
		refCnt := files[i].src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && files[i].src.canDelete.Load() {
			if iit.ii.filenameBase == traceFileLife {
				iit.ii.logger.Warn(fmt.Sprintf("[agg] real remove at ctx close: %s", files[i].src.decompressor.FileName()))
			}
			files[i].src.closeFilesAndRemove()
		}
	}

	for _, r := range iit.readers {
		r.Close()
	}
}

type InvertedIndexRoTx struct {
	ii      *InvertedIndex
	files   []ctxItem // have no garbage (overlaps, etc...)
	getters []ArchiveGetter
	readers []*recsplit.IndexReader

	_hasher murmur3.Hash128
}

func (iit *InvertedIndexRoTx) statelessHasher() murmur3.Hash128 {
	if iit._hasher == nil {
		iit._hasher = murmur3.New128WithSeed(*iit.ii.salt)
	}
	return iit._hasher
}
func (iit *InvertedIndexRoTx) hashKey(k []byte) (hi, lo uint64) {
	hasher := iit.statelessHasher()
	iit._hasher.Reset()
	_, _ = hasher.Write(k) //nolint:errcheck
	return hasher.Sum128()
}

func (iit *InvertedIndexRoTx) statelessGetter(i int) ArchiveGetter {
	if iit.getters == nil {
		iit.getters = make([]ArchiveGetter, len(iit.files))
	}
	r := iit.getters[i]
	if r == nil {
		g := iit.files[i].src.decompressor.MakeGetter()
		r = NewArchiveGetter(g, iit.ii.compression)
		iit.getters[i] = r
	}
	return r
}
func (iit *InvertedIndexRoTx) statelessIdxReader(i int) *recsplit.IndexReader {
	if iit.readers == nil {
		iit.readers = make([]*recsplit.IndexReader, len(iit.files))
	}
	r := iit.readers[i]
	if r == nil {
		r = iit.files[i].src.index.GetReaderFromPool()
		iit.readers[i] = r
	}
	return r
}

func (iit *InvertedIndexRoTx) seekInFiles(key []byte, txNum uint64) (found bool, equalOrHigherTxNum uint64) {
	hi, lo := iit.hashKey(key)

	for i := 0; i < len(iit.files); i++ {
		if iit.files[i].endTxNum <= txNum {
			continue
		}
		offset, ok := iit.statelessIdxReader(i).TwoLayerLookupByHash(hi, lo)
		if !ok {
			continue
		}

		g := iit.statelessGetter(i)
		g.Reset(offset)
		k, _ := g.Next(nil)
		if !bytes.Equal(k, key) {
			continue
		}
		eliasVal, _ := g.Next(nil)
		equalOrHigherTxNum, found = eliasfano32.Seek(eliasVal, txNum)

		if found {
			return true, equalOrHigherTxNum
		}
	}
	return false, 0
}

// it is assumed files are always sorted
func (iit *InvertedIndexRoTx) lastTxNumInFiles() uint64 {
	return iit.files[len(iit.files)-1].endTxNum
}

// IdxRange - return range of txNums for given `key`
// is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)
func (iit *InvertedIndexRoTx) IdxRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.U64, error) {
	frozenIt, err := iit.iterateRangeFrozen(key, startTxNum, endTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	recentIt, err := iit.recentIterateRange(key, startTxNum, endTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	return iter.Union[uint64](frozenIt, recentIt, asc, limit), nil
}

func (iit *InvertedIndexRoTx) recentIterateRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.U64, error) {
	//optimization: return empty pre-allocated iterator if range is frozen
	if asc {
		isFrozenRange := len(iit.files) > 0 && endTxNum >= 0 && iit.lastTxNumInFiles() >= uint64(endTxNum)
		if isFrozenRange {
			return iter.EmptyU64, nil
		}
	} else {
		isFrozenRange := len(iit.files) > 0 && startTxNum >= 0 && iit.lastTxNumInFiles() >= uint64(startTxNum)
		if isFrozenRange {
			return iter.EmptyU64, nil
		}
	}

	var from []byte
	if startTxNum >= 0 {
		from = make([]byte, 8)
		binary.BigEndian.PutUint64(from, uint64(startTxNum))
	}

	var to []byte
	if endTxNum >= 0 {
		to = make([]byte, 8)
		binary.BigEndian.PutUint64(to, uint64(endTxNum))
	}
	it, err := roTx.RangeDupSort(iit.ii.indexTable, key, from, to, asc, limit)
	if err != nil {
		return nil, err
	}
	return iter.TransformKV2U64(it, func(_, v []byte) (uint64, error) {
		return binary.BigEndian.Uint64(v), nil
	}), nil
}

// IdxRange is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)
func (iit *InvertedIndexRoTx) iterateRangeFrozen(key []byte, startTxNum, endTxNum int, asc order.By, limit int) (*FrozenInvertedIdxIter, error) {
	if asc && (startTxNum >= 0 && endTxNum >= 0) && startTxNum > endTxNum {
		return nil, fmt.Errorf("startTxNum=%d epected to be lower than endTxNum=%d", startTxNum, endTxNum)
	}
	if !asc && (startTxNum >= 0 && endTxNum >= 0) && startTxNum < endTxNum {
		return nil, fmt.Errorf("startTxNum=%d epected to be bigger than endTxNum=%d", startTxNum, endTxNum)
	}

	it := &FrozenInvertedIdxIter{
		key:         key,
		startTxNum:  startTxNum,
		endTxNum:    endTxNum,
		indexTable:  iit.ii.indexTable,
		orderAscend: asc,
		limit:       limit,
		ef:          eliasfano32.NewEliasFano(1, 1),
	}
	if asc {
		for i := len(iit.files) - 1; i >= 0; i-- {
			// [from,to) && from < to
			if endTxNum >= 0 && int(iit.files[i].startTxNum) >= endTxNum {
				continue
			}
			if startTxNum >= 0 && iit.files[i].endTxNum <= uint64(startTxNum) {
				break
			}
			if iit.files[i].src.index.KeyCount() == 0 {
				continue
			}
			it.stack = append(it.stack, iit.files[i])
			it.stack[len(it.stack)-1].getter = it.stack[len(it.stack)-1].src.decompressor.MakeGetter()
			it.stack[len(it.stack)-1].reader = it.stack[len(it.stack)-1].src.index.GetReaderFromPool()
			it.hasNext = true
		}
	} else {
		for i := 0; i < len(iit.files); i++ {
			// [from,to) && from > to
			if endTxNum >= 0 && int(iit.files[i].endTxNum) <= endTxNum {
				continue
			}
			if startTxNum >= 0 && iit.files[i].startTxNum > uint64(startTxNum) {
				break
			}
			if iit.files[i].src.index == nil { // assert
				err := fmt.Errorf("why file has not index: %s\n", iit.files[i].src.decompressor.FileName())
				panic(err)
			}
			if iit.files[i].src.index.KeyCount() == 0 {
				continue
			}
			it.stack = append(it.stack, iit.files[i])
			it.stack[len(it.stack)-1].getter = it.stack[len(it.stack)-1].src.decompressor.MakeGetter()
			it.stack[len(it.stack)-1].reader = it.stack[len(it.stack)-1].src.index.GetReaderFromPool()
			it.hasNext = true
		}
	}
	it.advance()
	return it, nil
}

func (iit *InvertedIndexRoTx) smallestTxNum(tx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(tx, iit.ii.indexKeysTable)
	if len(fst) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		return cmp.Min(fstInDb, math.MaxUint64)
	}
	return math.MaxUint64
}

func (iit *InvertedIndexRoTx) highestTxNum(tx kv.Tx) uint64 {
	lst, _ := kv.LastKey(tx, iit.ii.indexKeysTable)
	if len(lst) > 0 {
		lstInDb := binary.BigEndian.Uint64(lst)
		return cmp.Max(lstInDb, 0)
	}
	return 0
}

func (iit *InvertedIndexRoTx) CanPrune(tx kv.Tx) bool {
	return iit.smallestTxNum(tx) < iit.maxTxNumInFiles(false)
}

type InvertedIndexPruneStat struct {
	MinTxNum         uint64
	MaxTxNum         uint64
	PruneCountTx     uint64
	PruneCountValues uint64
}

func (is *InvertedIndexPruneStat) String() string {
	if is.MinTxNum == math.MaxUint64 && is.PruneCountTx == 0 {
		return ""
	}
	return fmt.Sprintf("ii %d txs and %d vals in %.2fM-%.2fM", is.PruneCountTx, is.PruneCountValues, float64(is.MinTxNum)/1_000_000.0, float64(is.MaxTxNum)/1_000_000.0)
}

func (is *InvertedIndexPruneStat) Accumulate(other *InvertedIndexPruneStat) {
	if other == nil {
		return
	}
	is.MinTxNum = min(is.MinTxNum, other.MinTxNum)
	is.MaxTxNum = max(is.MaxTxNum, other.MaxTxNum)
	is.PruneCountTx += other.PruneCountTx
	is.PruneCountValues += other.PruneCountValues
}

func (iit *InvertedIndexRoTx) Warmup(ctx context.Context) (cleanup func()) {
	ctx, cancel := context.WithCancel(ctx)
	wg := &errgroup.Group{}
	wg.Go(func() error {
		backup.WarmupTable(ctx, iit.ii.db, iit.ii.indexTable, log.LvlDebug, 4)
		return nil
	})
	wg.Go(func() error {
		backup.WarmupTable(ctx, iit.ii.db, iit.ii.indexKeysTable, log.LvlDebug, 4)
		return nil
	})
	return func() {
		cancel()
		_ = wg.Wait()
	}
}

// [txFrom; txTo)
// forced - prune even if CanPrune returns false, so its true only when we do Unwind.
func (iit *InvertedIndexRoTx) Prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced, withWarmup bool, fn func(key []byte, txnum []byte) error) (stat *InvertedIndexPruneStat, err error) {
	stat = &InvertedIndexPruneStat{MinTxNum: math.MaxUint64}
	if !forced && !iit.CanPrune(rwTx) {
		return stat, nil
	}

	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()
	defer func(t time.Time) { mxPruneTookIndex.ObserveDuration(t) }(time.Now())

	if withWarmup {
		cleanup := iit.Warmup(ctx)
		defer cleanup()
	}

	if limit == 0 {
		limit = math.MaxUint64
	}

	ii := iit.ii
	//defer func() {
	//	ii.logger.Error("[snapshots] prune index",
	//		"name", ii.filenameBase,
	//		"forced", forced,
	//		"pruned tx", fmt.Sprintf("%.2f-%.2f", float64(minTxnum)/float64(iit.ii.aggregationStep), float64(maxTxnum)/float64(iit.ii.aggregationStep)),
	//		"pruned values", pruneCount,
	//		"tx until limit", limit)
	//}()

	keysCursor, err := rwTx.RwCursorDupSort(ii.indexKeysTable)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()
	keysCursorForDel, err := rwTx.RwCursorDupSort(ii.indexKeysTable)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursorForDel.Close()
	idxC, err := rwTx.RwCursorDupSort(ii.indexTable)
	if err != nil {
		return nil, err
	}
	defer idxC.Close()
	idxValuesCount, err := idxC.Count()
	if err != nil {
		return nil, err
	}
	indexWithValues := idxValuesCount != 0 || fn != nil

	collector := etl.NewCollector("prune idx "+ii.filenameBase, ii.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize/8), ii.logger)
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
		if err = rwTx.Delete(ii.indexKeysTable, k); err != nil {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if !indexWithValues {
		return stat, nil
	}

	idxCForDeletes, err := rwTx.RwCursorDupSort(ii.indexTable)
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

func (iit *InvertedIndexRoTx) DebugEFAllValuesAreInRange(ctx context.Context) error {
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

	for _, item := range iit.files {
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

// FrozenInvertedIdxIter allows iteration over range of tx numbers
// Iteration is not implmented via callback function, because there is often
// a requirement for interators to be composable (for example, to implement AND and OR for indices)
// FrozenInvertedIdxIter must be closed after use to prevent leaking of resources like cursor
type FrozenInvertedIdxIter struct {
	key                  []byte
	startTxNum, endTxNum int
	limit                int
	orderAscend          order.By

	efIt       iter.Uno[uint64]
	indexTable string
	stack      []ctxItem

	nextN   uint64
	hasNext bool
	err     error

	ef *eliasfano32.EliasFano
}

func (it *FrozenInvertedIdxIter) Close() {
	for _, item := range it.stack {
		item.reader.Close()
	}
}

func (it *FrozenInvertedIdxIter) advance() {
	if it.hasNext {
		it.advanceInFiles()
	}
}

func (it *FrozenInvertedIdxIter) HasNext() bool {
	if it.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if it.limit == 0 { // limit reached
		return false
	}
	return it.hasNext
}

func (it *FrozenInvertedIdxIter) Next() (uint64, error) { return it.next(), nil }

func (it *FrozenInvertedIdxIter) next() uint64 {
	it.limit--
	n := it.nextN
	it.advance()
	return n
}

func (it *FrozenInvertedIdxIter) advanceInFiles() {
	for {
		for it.efIt == nil {
			if len(it.stack) == 0 {
				it.hasNext = false
				return
			}
			item := it.stack[len(it.stack)-1]
			it.stack = it.stack[:len(it.stack)-1]
			offset, ok := item.reader.TwoLayerLookup(it.key)
			if !ok {
				continue
			}
			g := item.getter
			g.Reset(offset)
			k, _ := g.NextUncompressed()
			if bytes.Equal(k, it.key) {
				eliasVal, _ := g.NextUncompressed()
				it.ef.Reset(eliasVal)
				if it.orderAscend {
					efiter := it.ef.Iterator()
					if it.startTxNum > 0 {
						efiter.Seek(uint64(it.startTxNum))
					}
					it.efIt = efiter
				} else {
					it.efIt = it.ef.ReverseIterator()
				}
			}
		}

		//TODO: add seek method
		//Asc:  [from, to) AND from < to
		//Desc: [from, to) AND from > to
		if it.orderAscend {
			for it.efIt.HasNext() {
				n, err := it.efIt.Next()
				if err != nil {
					it.err = err
					return
				}
				isBeforeRange := int(n) < it.startTxNum
				if isBeforeRange { //skip
					continue
				}
				isAfterRange := it.endTxNum >= 0 && int(n) >= it.endTxNum
				if isAfterRange { // terminate
					it.hasNext = false
					return
				}
				it.hasNext = true
				it.nextN = n
				return
			}
		} else {
			for it.efIt.HasNext() {
				n, err := it.efIt.Next()
				if err != nil {
					it.err = err
					return
				}
				isAfterRange := it.startTxNum >= 0 && int(n) > it.startTxNum
				if isAfterRange { //skip
					continue
				}
				isBeforeRange := it.endTxNum >= 0 && int(n) <= it.endTxNum
				if isBeforeRange { // terminate
					it.hasNext = false
					return
				}
				it.hasNext = true
				it.nextN = n
				return
			}
		}
		it.efIt = nil // Exhausted this iterator
	}
}

// RecentInvertedIdxIter allows iteration over range of tx numbers
// Iteration is not implmented via callback function, because there is often
// a requirement for interators to be composable (for example, to implement AND and OR for indices)
type RecentInvertedIdxIter struct {
	key                  []byte
	startTxNum, endTxNum int
	limit                int
	orderAscend          order.By

	roTx       kv.Tx
	cursor     kv.CursorDupSort
	indexTable string

	nextN   uint64
	hasNext bool
	err     error

	bm *roaring64.Bitmap
}

func (it *RecentInvertedIdxIter) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
	bitmapdb.ReturnToPool64(it.bm)
}

func (it *RecentInvertedIdxIter) advanceInDB() {
	var v []byte
	var err error
	if it.cursor == nil {
		if it.cursor, err = it.roTx.CursorDupSort(it.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		var k []byte
		if k, _, err = it.cursor.SeekExact(it.key); err != nil {
			panic(err)
		}
		if k == nil {
			it.hasNext = false
			return
		}
		//Asc:  [from, to) AND from < to
		//Desc: [from, to) AND from > to
		var keyBytes [8]byte
		if it.startTxNum > 0 {
			binary.BigEndian.PutUint64(keyBytes[:], uint64(it.startTxNum))
		}
		if v, err = it.cursor.SeekBothRange(it.key, keyBytes[:]); err != nil {
			panic(err)
		}
		if v == nil {
			if !it.orderAscend {
				_, v, _ = it.cursor.PrevDup()
				if err != nil {
					panic(err)
				}
			}
			if v == nil {
				it.hasNext = false
				return
			}
		}
	} else {
		if it.orderAscend {
			_, v, err = it.cursor.NextDup()
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
		} else {
			_, v, err = it.cursor.PrevDup()
			if err != nil {
				panic(err)
			}
		}
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
	if it.orderAscend {
		for ; v != nil; _, v, err = it.cursor.NextDup() {
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
			n := binary.BigEndian.Uint64(v)
			if it.endTxNum >= 0 && int(n) >= it.endTxNum {
				it.hasNext = false
				return
			}
			if int(n) >= it.startTxNum {
				it.hasNext = true
				it.nextN = n
				return
			}
		}
	} else {
		for ; v != nil; _, v, err = it.cursor.PrevDup() {
			if err != nil {
				// TODO pass error properly around
				panic(err)
			}
			n := binary.BigEndian.Uint64(v)
			if int(n) <= it.endTxNum {
				it.hasNext = false
				return
			}
			if it.startTxNum >= 0 && int(n) <= it.startTxNum {
				it.hasNext = true
				it.nextN = n
				return
			}
		}
	}

	it.hasNext = false
}

func (it *RecentInvertedIdxIter) advance() {
	if it.hasNext {
		it.advanceInDB()
	}
}

func (it *RecentInvertedIdxIter) HasNext() bool {
	if it.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if it.limit == 0 { // limit reached
		return false
	}
	return it.hasNext
}

func (it *RecentInvertedIdxIter) Next() (uint64, error) {
	if it.err != nil {
		return 0, it.err
	}
	it.limit--
	n := it.nextN
	it.advance()
	return n, nil
}

type InvertedIterator1 struct {
	roTx           kv.Tx
	cursor         kv.CursorDupSort
	indexTable     string
	key            []byte
	h              ReconHeap
	nextKey        []byte
	nextFileKey    []byte
	nextDbKey      []byte
	endTxNum       uint64
	startTxNum     uint64
	startTxKey     [8]byte
	hasNextInDb    bool
	hasNextInFiles bool
}

func (it *InvertedIterator1) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
}

func (it *InvertedIterator1) advanceInFiles() {
	for it.h.Len() > 0 {
		top := heap.Pop(&it.h).(*ReconItem)
		key := top.key
		val, _ := top.g.Next(nil)
		if top.g.HasNext() {
			top.key, _ = top.g.Next(nil)
			heap.Push(&it.h, top)
		}
		if !bytes.Equal(key, it.key) {
			ef, _ := eliasfano32.ReadEliasFano(val)
			min := ef.Get(0)
			max := ef.Max()
			if min < it.endTxNum && max >= it.startTxNum { // Intersection of [min; max) and [it.startTxNum; it.endTxNum)
				it.key = key
				it.nextFileKey = key
				return
			}
		}
	}
	it.hasNextInFiles = false
}

func (it *InvertedIterator1) advanceInDb() {
	var k, v []byte
	var err error
	if it.cursor == nil {
		if it.cursor, err = it.roTx.CursorDupSort(it.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		if k, _, err = it.cursor.First(); err != nil {
			// TODO pass error properly around
			panic(err)
		}
	} else {
		if k, _, err = it.cursor.NextNoDup(); err != nil {
			panic(err)
		}
	}
	for k != nil {
		if v, err = it.cursor.SeekBothRange(k, it.startTxKey[:]); err != nil {
			panic(err)
		}
		if v != nil {
			txNum := binary.BigEndian.Uint64(v)
			if txNum < it.endTxNum {
				it.nextDbKey = append(it.nextDbKey[:0], k...)
				return
			}
		}
		if k, _, err = it.cursor.NextNoDup(); err != nil {
			panic(err)
		}
	}
	it.cursor.Close()
	it.cursor = nil
	it.hasNextInDb = false
}

func (it *InvertedIterator1) advance() {
	if it.hasNextInFiles {
		if it.hasNextInDb {
			c := bytes.Compare(it.nextFileKey, it.nextDbKey)
			if c < 0 {
				it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
				it.advanceInFiles()
			} else if c > 0 {
				it.nextKey = append(it.nextKey[:0], it.nextDbKey...)
				it.advanceInDb()
			} else {
				it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
				it.advanceInDb()
				it.advanceInFiles()
			}
		} else {
			it.nextKey = append(it.nextKey[:0], it.nextFileKey...)
			it.advanceInFiles()
		}
	} else if it.hasNextInDb {
		it.nextKey = append(it.nextKey[:0], it.nextDbKey...)
		it.advanceInDb()
	} else {
		it.nextKey = nil
	}
}

func (it *InvertedIterator1) HasNext() bool {
	return it.hasNextInFiles || it.hasNextInDb || it.nextKey != nil
}

func (it *InvertedIterator1) Next(keyBuf []byte) []byte {
	result := append(keyBuf, it.nextKey...)
	it.advance()
	return result
}

func (iit *InvertedIndexRoTx) IterateChangedKeys(startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator1 {
	var ii1 InvertedIterator1
	ii1.hasNextInDb = true
	ii1.roTx = roTx
	ii1.indexTable = iit.ii.indexTable
	for _, item := range iit.files {
		if item.endTxNum <= startTxNum {
			continue
		}
		if item.startTxNum >= endTxNum {
			break
		}
		if item.endTxNum >= endTxNum {
			ii1.hasNextInDb = false
		}
		g := NewArchiveGetter(item.src.decompressor.MakeGetter(), iit.ii.compression)
		if g.HasNext() {
			key, _ := g.Next(nil)
			heap.Push(&ii1.h, &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key})
			ii1.hasNextInFiles = true
		}
	}
	binary.BigEndian.PutUint64(ii1.startTxKey[:], startTxNum)
	ii1.startTxNum = startTxNum
	ii1.endTxNum = endTxNum
	ii1.advanceInDb()
	ii1.advanceInFiles()
	ii1.advance()
	return ii1
}

// collate [stepFrom, stepTo)
func (ii *InvertedIndex) collate(ctx context.Context, step uint64, roTx kv.Tx) (InvertedIndexCollation, error) {
	stepTo := step + 1
	txFrom, txTo := step*ii.aggregationStep, stepTo*ii.aggregationStep
	start := time.Now()
	defer mxCollateTookIndex.ObserveDuration(start)

	keysCursor, err := roTx.CursorDupSort(ii.indexKeysTable)
	if err != nil {
		return InvertedIndexCollation{}, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()

	collector := etl.NewCollector("collate idx "+ii.filenameBase, ii.iiCfg.dirs.Tmp, etl.NewSortableBuffer(CollateETLRAM), ii.logger)
	defer collector.Close()
	collector.LogLvl(log.LvlTrace)

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.Next() {
		if err != nil {
			return InvertedIndexCollation{}, fmt.Errorf("iterate over %s keys cursor: %w", ii.filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo { // [txFrom; txTo)
			break
		}
		if err := collector.Collect(v, k); err != nil {
			return InvertedIndexCollation{}, fmt.Errorf("collect %s history key [%x]=>txn %d [%x]: %w", ii.filenameBase, k, txNum, k, err)
		}
		select {
		case <-ctx.Done():
			return InvertedIndexCollation{}, ctx.Err()
		default:
		}
	}

	var (
		coll = InvertedIndexCollation{
			iiPath: ii.efFilePath(step, stepTo),
		}
		closeComp bool
	)
	defer func() {
		if closeComp {
			coll.Close()
		}
	}()

	comp, err := seg.NewCompressor(ctx, "collate idx "+ii.filenameBase, coll.iiPath, ii.dirs.Tmp, seg.MinPatternScore, ii.compressWorkers, log.LvlTrace, ii.logger)
	if err != nil {
		return InvertedIndexCollation{}, fmt.Errorf("create %s compressor: %w", ii.filenameBase, err)
	}
	coll.writer = NewArchiveWriter(comp, ii.compression)

	var (
		prevEf      []byte
		prevKey     []byte
		initialized bool
		bitmap      = bitmapdb.NewBitmap64()
	)
	defer bitmapdb.ReturnToPool64(bitmap)

	loadBitmapsFunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		txNum := binary.BigEndian.Uint64(v)
		if !initialized {
			prevKey = append(prevKey[:0], k...)
			initialized = true
		}

		if bytes.Equal(prevKey, k) {
			bitmap.Add(txNum)
			prevKey = append(prevKey[:0], k...)
			return nil
		}

		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			ef.AddOffset(it.Next())
		}
		bitmap.Clear()
		ef.Build()

		prevEf = ef.AppendBytes(prevEf[:0])

		if err = coll.writer.AddWord(prevKey); err != nil {
			return fmt.Errorf("add %s efi index key [%x]: %w", ii.filenameBase, prevKey, err)
		}
		if err = coll.writer.AddWord(prevEf); err != nil {
			return fmt.Errorf("add %s efi index val: %w", ii.filenameBase, err)
		}

		prevKey = append(prevKey[:0], k...)
		txNum = binary.BigEndian.Uint64(v)
		bitmap.Add(txNum)

		return nil
	}

	err = collector.Load(nil, "", loadBitmapsFunc, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return InvertedIndexCollation{}, err
	}
	if !bitmap.IsEmpty() {
		if err = loadBitmapsFunc(nil, make([]byte, 8), nil, nil); err != nil {
			return InvertedIndexCollation{}, err
		}
	}

	closeComp = false
	return coll, nil
}

type InvertedFiles struct {
	decomp    *seg.Decompressor
	index     *recsplit.Index
	existence *ExistenceFilter
}

func (sf InvertedFiles) CleanupOnError() {
	if sf.decomp != nil {
		sf.decomp.Close()
	}
	if sf.index != nil {
		sf.index.Close()
	}
}

type InvertedIndexCollation struct {
	iiPath string
	writer ArchiveWriter
}

func (ic InvertedIndexCollation) Close() {
	if ic.writer != nil {
		ic.writer.Close()
	}
}

// buildFiles - `step=N` means build file `[N:N+1)` which is equal to [N:N+1)
func (ii *InvertedIndex) buildFiles(ctx context.Context, step uint64, coll InvertedIndexCollation, ps *background.ProgressSet) (InvertedFiles, error) {
	var (
		decomp    *seg.Decompressor
		index     *recsplit.Index
		existence *ExistenceFilter
		err       error
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
			if existence != nil {
				existence.Close()
			}
		}
	}()

	if assert.Enable {
		if coll.iiPath == "" && reflect.ValueOf(coll.writer).IsNil() {
			panic("assert: collation is not initialized " + ii.filenameBase)
		}
	}

	{
		p := ps.AddNew(path.Base(coll.iiPath), 1)
		if err = coll.writer.Compress(); err != nil {
			ps.Delete(p)
			return InvertedFiles{}, fmt.Errorf("compress %s: %w", ii.filenameBase, err)
		}
		coll.Close()
		ps.Delete(p)
	}

	if decomp, err = seg.NewDecompressor(coll.iiPath); err != nil {
		return InvertedFiles{}, fmt.Errorf("open %s decompressor: %w", ii.filenameBase, err)
	}

	if err := ii.buildMapIdx(ctx, step, step+1, decomp, ps); err != nil {
		return InvertedFiles{}, fmt.Errorf("build %s efi: %w", ii.filenameBase, err)
	}
	if index, err = recsplit.OpenIndex(ii.efAccessorFilePath(step, step+1)); err != nil {
		return InvertedFiles{}, err
	}

	closeComp = false
	return InvertedFiles{decomp: decomp, index: index, existence: existence}, nil
}

func (ii *InvertedIndex) buildMapIdx(ctx context.Context, fromStep, toStep uint64, data *seg.Decompressor, ps *background.ProgressSet) error {
	idxPath := ii.efAccessorFilePath(fromStep, toStep)
	cfg := recsplit.RecSplitArgs{
		Enums:              true,
		LessFalsePositives: true,

		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     ii.dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       ii.salt,
		NoFsync:    ii.noFsync,
	}
	return buildIndex(ctx, data, ii.compression, idxPath, false, cfg, ps, ii.logger)
}

func (ii *InvertedIndex) integrateDirtyFiles(sf InvertedFiles, txNumFrom, txNumTo uint64) {
	fi := newFilesItem(txNumFrom, txNumTo, ii.aggregationStep)
	fi.decompressor = sf.decomp
	fi.index = sf.index
	fi.existence = sf.existence
	ii.dirtyFiles.Set(fi)
}

func (ii *InvertedIndex) collectFilesStat() (filesCount, filesSize, idxSize uint64) {
	if ii.dirtyFiles == nil {
		return 0, 0, 0
	}
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
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

func (ii *InvertedIndex) stepsRangeInDBAsStr(tx kv.Tx) string {
	a1, a2 := ii.stepsRangeInDB(tx)
	return fmt.Sprintf("%s: %.1f", ii.filenameBase, a2-a1)
}
func (ii *InvertedIndex) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	fst, _ := kv.FirstKey(tx, ii.indexKeysTable)
	if len(fst) > 0 {
		from = float64(binary.BigEndian.Uint64(fst)) / float64(ii.aggregationStep)
	}
	lst, _ := kv.LastKey(tx, ii.indexKeysTable)
	if len(lst) > 0 {
		to = float64(binary.BigEndian.Uint64(lst)) / float64(ii.aggregationStep)
	}
	if to == 0 {
		to = from
	}
	return from, to
}
