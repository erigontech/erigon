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
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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

	// files - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in AggregatorV3
	//
	// roFiles derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// MakeContext() using roFiles in zero-copy way
	files   *btree2.BTreeG[*filesItem]
	roFiles atomic.Pointer[[]ctxItem]

	indexKeysTable  string // txnNum_u64 -> key (k+auto_increment)
	indexTable      string // k -> txnNum_u64 , Needs to be table with DupSort
	filenameBase    string
	aggregationStep uint64

	//TODO: re-visit this check - maybe we don't need it. It's abot kill in the middle of merge
	integrityCheck func(fromStep, toStep uint64) bool

	withExistenceIndex bool

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

func NewInvertedIndex(cfg iiCfg, aggregationStep uint64, filenameBase, indexKeysTable, indexTable string, withExistenceIndex bool, integrityCheck func(fromStep uint64, toStep uint64) bool, logger log.Logger) (*InvertedIndex, error) {
	if cfg.dirs.SnapDomain == "" {
		panic("empty `dirs` varialbe")
	}
	ii := InvertedIndex{
		iiCfg:              cfg,
		files:              btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		aggregationStep:    aggregationStep,
		filenameBase:       filenameBase,
		indexKeysTable:     indexKeysTable,
		indexTable:         indexTable,
		compressWorkers:    1,
		integrityCheck:     integrityCheck,
		withExistenceIndex: withExistenceIndex,
		logger:             logger,
		compression:        CompressNone,
	}
	ii.indexList = withHashMap
	if ii.withExistenceIndex {
		ii.indexList |= withExistence
	}

	ii.roFiles.Store(&[]ctxItem{})

	return &ii, nil
}

func (ii *InvertedIndex) efExistenceIdxFilePath(fromStep, toStep uint64) string {
	return filepath.Join(ii.dirs.SnapAccessors, fmt.Sprintf("v1-%s.%d-%d.efei", ii.filenameBase, fromStep, toStep))
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
		return fmt.Errorf("InvertedIndex(%s).openFiles: %w", ii.filenameBase, err)
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

		if _, has := ii.files.Get(newFile); has {
			continue
		}

		ii.files.Set(newFile)
	}
	return garbageFiles
}

type idxList int

var (
	withBTree     idxList = 0b1
	withHashMap   idxList = 0b10
	withExistence idxList = 0b100
)

func ctxFiles(files *btree2.BTreeG[*filesItem], l idxList, trace bool) (roItems []ctxItem) {
	roFiles := make([]ctxItem, 0, files.Len())
	if trace {
		log.Warn("[dbg] roFiles01", "amount", files.Len())
	}
	files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.canDelete.Load() {
				if trace {
					log.Warn("[dbg] roFiles0", "f", item.decompressor.FileName())
				}
				continue
			}

			// TODO: need somehow handle this case, but indices do not open in tests TestFindMergeRangeCornerCases
			if item.decompressor == nil {
				if trace {
					log.Warn("[dbg] roFiles1", "from", item.startTxNum, "to", item.endTxNum)
				}
				continue
			}
			if (l&withBTree != 0) && item.bindex == nil {
				if trace {
					log.Warn("[dbg] roFiles2", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("btindex nil: %s", item.decompressor.FileName()))
				continue
			}
			if (l&withHashMap != 0) && item.index == nil {
				if trace {
					log.Warn("[dbg] roFiles3", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("index nil: %s", item.decompressor.FileName()))
				continue
			}
			if (l&withExistence != 0) && item.existence == nil {
				if trace {
					log.Warn("[dbg] roFiles4", "f", item.decompressor.FileName())
				}
				//panic(fmt.Errorf("existence nil: %s", item.decompressor.FileName()))
				continue
			}

			// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
			// see super-set file, just drop sub-set files from list
			for len(roFiles) > 0 && roFiles[len(roFiles)-1].src.isSubsetOf(item) {
				if trace {
					log.Warn("[dbg] roFiles5", "f", roFiles[len(roFiles)-1].src.decompressor.FileName())
				}
				roFiles[len(roFiles)-1].src = nil
				roFiles = roFiles[:len(roFiles)-1]
			}
			roFiles = append(roFiles, ctxItem{
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				i:          len(roFiles),
				src:        item,
			})
		}
		return true
	})
	if roFiles == nil {
		roFiles = []ctxItem{}
	}
	return roFiles
}

func (ii *InvertedIndex) reCalcRoFiles() {
	roFiles := ctxFiles(ii.files, ii.indexList, false)
	ii.roFiles.Store(&roFiles)
}

func (ii *InvertedIndex) missedIdxFiles() (l []*filesItem) {
	ii.files.Walk(func(items []*filesItem) bool {
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
func (ii *InvertedIndex) missedExistenceFilterFiles() (l []*filesItem) {
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
			if !dir.FileExist(ii.efExistenceIdxFilePath(fromStep, toStep)) {
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
func (ii *InvertedIndex) buildExistenceFilter(ctx context.Context, item *filesItem, ps *background.ProgressSet) (err error) {
	if item.decompressor == nil {
		return fmt.Errorf("buildExistenceFilter: passed item with nil decompressor %s %d-%d", ii.filenameBase, item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
	}
	if !ii.withExistenceIndex {
		return nil
	}
	fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
	idxPath := ii.efExistenceIdxFilePath(fromStep, toStep)
	return buildIdxFilter(ctx, item.decompressor, ii.compression, idxPath, ii.salt, ps, ii.logger, ii.noFsync)
}

func buildIdxFilter(ctx context.Context, d *seg.Decompressor, compressed FileCompression, idxPath string, salt *uint32, ps *background.ProgressSet, logger log.Logger, noFsync bool) error {
	g := NewArchiveGetter(d.MakeGetter(), compressed)
	_, fileName := filepath.Split(idxPath)
	count := d.Count() / 2

	p := ps.AddNew(fileName, uint64(count))
	defer ps.Delete(p)
	defer d.EnableReadAhead().DisableReadAhead()

	idxFilter, err := NewExistenceFilter(uint64(count), idxPath)
	if err != nil {
		return err
	}
	if noFsync {
		idxFilter.DisableFsync()
	}
	hasher := murmur3.New128WithSeed(*salt)

	key := make([]byte, 0, 256)
	g.Reset(0)
	for g.HasNext() {
		key, _ = g.Next(key[:0])
		hasher.Reset()
		hasher.Write(key) //nolint:errcheck
		hi, _ := hasher.Sum128()
		idxFilter.AddHash(hi)

		// Skip value
		g.Skip()

		p.Processed.Add(1)
	}
	if err := idxFilter.Build(); err != nil {
		return err
	}

	return nil
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (ii *InvertedIndex) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	for _, item := range ii.missedIdxFiles() {
		item := item
		g.Go(func() error {
			return ii.buildEfi(ctx, item, ps)
		})
	}

	for _, item := range ii.missedExistenceFilterFiles() {
		item := item
		g.Go(func() error {
			return ii.buildExistenceFilter(ctx, item, ps)
		})
	}
}

func (ii *InvertedIndex) openFiles() error {
	var err error
	var invalidFileItems []*filesItem
	invalidFileItemsLock := sync.Mutex{}
	ii.files.Walk(func(items []*filesItem) bool {
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
					ii.logger.Warn("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
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
			if item.existence == nil && ii.withExistenceIndex {
				fPath := ii.efExistenceIdxFilePath(fromStep, toStep)
				if dir.FileExist(fPath) {
					if item.existence, err = OpenExistenceFilter(fPath); err != nil {
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
		ii.files.Delete(item)
	}

	ii.reCalcRoFiles()
	return nil
}

func (ii *InvertedIndex) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	ii.files.Walk(func(items []*filesItem) bool {
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
		if item.decompressor != nil {
			item.decompressor.Close()
			item.decompressor = nil
		}
		if item.index != nil {
			item.index.Close()
			item.index = nil
		}
		if item.existence != nil {
			item.existence.Close()
			item.existence = nil
		}
		ii.files.Delete(item)
	}
}

func (ii *InvertedIndex) Close() {
	ii.closeWhatNotInList([]string{})
	ii.reCalcRoFiles()
}

// DisableFsync - just for tests
func (ii *InvertedIndex) DisableFsync() { ii.noFsync = true }

func (ic *InvertedIndexContext) Files() (res []string) {
	for _, item := range ic.files {
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

func (ic *InvertedIndexContext) NewWriter() *invertedIndexBufferedWriter {
	return ic.newWriter(ic.ii.dirs.Tmp, false)
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

func (ic *InvertedIndexContext) newWriter(tmpdir string, discard bool) *invertedIndexBufferedWriter {
	w := &invertedIndexBufferedWriter{
		discard:         discard,
		tmpdir:          tmpdir,
		filenameBase:    ic.ii.filenameBase,
		aggregationStep: ic.ii.aggregationStep,

		indexKeysTable: ic.ii.indexKeysTable,
		indexTable:     ic.ii.indexTable,
		// etl collector doesn't fsync: means if have enough ram, all files produced by all collectors will be in ram
		indexKeys: etl.NewCollector(ic.ii.indexKeysTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), ic.ii.logger),
		index:     etl.NewCollector(ic.ii.indexTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), ic.ii.logger),
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

func (ii *InvertedIndex) MakeContext() *InvertedIndexContext {
	files := *ii.roFiles.Load()
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}
	return &InvertedIndexContext{
		ii:    ii,
		files: files,
	}
}
func (ic *InvertedIndexContext) Close() {
	if ic.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := ic.files
	ic.files = nil
	for i := 0; i < len(files); i++ {
		if files[i].src.frozen {
			continue
		}
		refCnt := files[i].src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && files[i].src.canDelete.Load() {
			if ic.ii.filenameBase == traceFileLife {
				ic.ii.logger.Warn(fmt.Sprintf("[agg] real remove at ctx close: %s", files[i].src.decompressor.FileName()))
			}
			files[i].src.closeFilesAndRemove()
		}
	}

	for _, r := range ic.readers {
		r.Close()
	}
}

type InvertedIndexContext struct {
	ii      *InvertedIndex
	files   []ctxItem // have no garbage (overlaps, etc...)
	getters []ArchiveGetter
	readers []*recsplit.IndexReader

	_hasher murmur3.Hash128
}

func (ic *InvertedIndexContext) statelessHasher() murmur3.Hash128 {
	if ic._hasher == nil {
		ic._hasher = murmur3.New128WithSeed(*ic.ii.salt)
	}
	return ic._hasher
}
func (ic *InvertedIndexContext) hashKey(k []byte) (hi, lo uint64) {
	hasher := ic.statelessHasher()
	ic._hasher.Reset()
	_, _ = hasher.Write(k) //nolint:errcheck
	return hasher.Sum128()
}

func (ic *InvertedIndexContext) statelessGetter(i int) ArchiveGetter {
	if ic.getters == nil {
		ic.getters = make([]ArchiveGetter, len(ic.files))
	}
	r := ic.getters[i]
	if r == nil {
		g := ic.files[i].src.decompressor.MakeGetter()
		r = NewArchiveGetter(g, ic.ii.compression)
		ic.getters[i] = r
	}
	return r
}
func (ic *InvertedIndexContext) statelessIdxReader(i int) *recsplit.IndexReader {
	if ic.readers == nil {
		ic.readers = make([]*recsplit.IndexReader, len(ic.files))
	}
	r := ic.readers[i]
	if r == nil {
		r = ic.files[i].src.index.GetReaderFromPool()
		ic.readers[i] = r
	}
	return r
}

func (ic *InvertedIndexContext) Seek(key []byte, txNum uint64) (found bool, equalOrHigherTxNum uint64) {
	hi, lo := ic.hashKey(key)

	for i := 0; i < len(ic.files); i++ {
		if ic.files[i].endTxNum <= txNum {
			continue
		}
		if ic.ii.withExistenceIndex && ic.files[i].src.existence != nil {
			if !ic.files[i].src.existence.ContainsHash(hi) {
				continue
			}
		}
		offset, ok := ic.statelessIdxReader(i).TwoLayerLookupByHash(hi, lo)
		if !ok {
			continue
		}

		g := ic.statelessGetter(i)
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

// IdxRange - return range of txNums for given `key`
// is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)
func (ic *InvertedIndexContext) IdxRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.U64, error) {
	frozenIt, err := ic.iterateRangeFrozen(key, startTxNum, endTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	recentIt, err := ic.recentIterateRange(key, startTxNum, endTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	return iter.Union[uint64](frozenIt, recentIt, asc, limit), nil
}

func (ic *InvertedIndexContext) recentIterateRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.U64, error) {
	//optimization: return empty pre-allocated iterator if range is frozen
	if asc {
		isFrozenRange := len(ic.files) > 0 && endTxNum >= 0 && ic.files[len(ic.files)-1].endTxNum >= uint64(endTxNum)
		if isFrozenRange {
			return iter.EmptyU64, nil
		}
	} else {
		isFrozenRange := len(ic.files) > 0 && startTxNum >= 0 && ic.files[len(ic.files)-1].endTxNum >= uint64(startTxNum)
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
	it, err := roTx.RangeDupSort(ic.ii.indexTable, key, from, to, asc, limit)
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
func (ic *InvertedIndexContext) iterateRangeFrozen(key []byte, startTxNum, endTxNum int, asc order.By, limit int) (*FrozenInvertedIdxIter, error) {
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
		indexTable:  ic.ii.indexTable,
		orderAscend: asc,
		limit:       limit,
		ef:          eliasfano32.NewEliasFano(1, 1),
	}
	if asc {
		for i := len(ic.files) - 1; i >= 0; i-- {
			// [from,to) && from < to
			if endTxNum >= 0 && int(ic.files[i].startTxNum) >= endTxNum {
				continue
			}
			if startTxNum >= 0 && ic.files[i].endTxNum <= uint64(startTxNum) {
				break
			}
			if ic.files[i].src.index.KeyCount() == 0 {
				continue
			}
			it.stack = append(it.stack, ic.files[i])
			it.stack[len(it.stack)-1].getter = it.stack[len(it.stack)-1].src.decompressor.MakeGetter()
			it.stack[len(it.stack)-1].reader = it.stack[len(it.stack)-1].src.index.GetReaderFromPool()
			it.hasNext = true
		}
	} else {
		for i := 0; i < len(ic.files); i++ {
			// [from,to) && from > to
			if endTxNum >= 0 && int(ic.files[i].endTxNum) <= endTxNum {
				continue
			}
			if startTxNum >= 0 && ic.files[i].startTxNum > uint64(startTxNum) {
				break
			}
			if ic.files[i].src.index == nil { // assert
				err := fmt.Errorf("why file has not index: %s\n", ic.files[i].src.decompressor.FileName())
				panic(err)
			}
			if ic.files[i].src.index.KeyCount() == 0 {
				continue
			}
			it.stack = append(it.stack, ic.files[i])
			it.stack[len(it.stack)-1].getter = it.stack[len(it.stack)-1].src.decompressor.MakeGetter()
			it.stack[len(it.stack)-1].reader = it.stack[len(it.stack)-1].src.index.GetReaderFromPool()
			it.hasNext = true
		}
	}
	it.advance()
	return it, nil
}

func (ic *InvertedIndexContext) smallestTxNum(tx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(tx, ic.ii.indexKeysTable)
	if len(fst) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		return cmp.Min(fstInDb, math.MaxUint64)
	}
	return math.MaxUint64
}

func (ic *InvertedIndexContext) highestTxNum(tx kv.Tx) uint64 {
	lst, _ := kv.LastKey(tx, ic.ii.indexKeysTable)
	if len(lst) > 0 {
		lstInDb := binary.BigEndian.Uint64(lst)
		return cmp.Max(lstInDb, 0)
	}
	return 0
}

func (ic *InvertedIndexContext) CanPrune(tx kv.Tx) bool {
	return ic.smallestTxNum(tx) < ic.maxTxNumInFiles(false)
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

func (ic *InvertedIndexContext) Warmup(ctx context.Context) (cleanup func()) {
	ctx, cancel := context.WithCancel(ctx)
	wg := &errgroup.Group{}
	wg.Go(func() error {
		backup.WarmupTable(ctx, ic.ii.db, ic.ii.indexTable, log.LvlDebug, 4)
		return nil
	})
	wg.Go(func() error {
		backup.WarmupTable(ctx, ic.ii.db, ic.ii.indexKeysTable, log.LvlDebug, 4)
		return nil
	})
	return func() {
		cancel()
		_ = wg.Wait()
	}
}

// [txFrom; txTo)
// forced - prune even if CanPrune returns false, so its true only when we do Unwind.
func (ic *InvertedIndexContext) Prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced, withWarmup bool, fn func(key []byte, txnum []byte) error) (stat *InvertedIndexPruneStat, err error) {
	stat = &InvertedIndexPruneStat{MinTxNum: math.MaxUint64}
	if !forced && !ic.CanPrune(rwTx) {
		return stat, nil
	}

	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()
	defer func(t time.Time) { mxPruneTookIndex.ObserveDuration(t) }(time.Now())

	if withWarmup {
		cleanup := ic.Warmup(ctx)
		defer cleanup()
	}

	ii := ic.ii
	//defer func() {
	//	ii.logger.Error("[snapshots] prune index",
	//		"name", ii.filenameBase,
	//		"forced", forced,
	//		"pruned tx", fmt.Sprintf("%.2f-%.2f", float64(minTxnum)/float64(ic.ii.aggregationStep), float64(maxTxnum)/float64(ic.ii.aggregationStep)),
	//		"pruned values", pruneCount,
	//		"tx until limit", limit)
	//}()

	// do not collect and sort keys if it's History index
	var indexWithHistoryValues bool
	{
		itc, err := rwTx.CursorDupSort(ii.indexTable)
		if err != nil {
			return nil, err
		}
		idxValuesCount, err := itc.Count()
		itc.Close()
		if err != nil {
			return nil, err
		}
		indexWithHistoryValues = idxValuesCount == 0 && fn != nil
	}

	keysCursor, err := rwTx.RwCursorDupSort(ii.indexKeysTable)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	k, v, err := keysCursor.Seek(txKey[:])
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, nil
	}

	txFrom = binary.BigEndian.Uint64(k)
	if limit == 0 {
		limit = math.MaxUint64
	}
	if txFrom >= txTo {
		return nil, nil
	}

	collector := etl.NewCollector("snapshots", ii.dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize), ii.logger)
	defer collector.Close()
	collector.LogLvl(log.LvlDebug)
	collector.SortAndFlushInBackground(true)

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	for ; k != nil; k, v, err = keysCursor.NextNoDup() {
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

		for ; v != nil; _, v, err = keysCursor.NextDup() {
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", ii.filenameBase, err)
			}
			if !indexWithHistoryValues {
				if err := collector.Collect(v, nil); err != nil {
					return nil, err
				}
			}
			if fn != nil {
				if err := fn(v, k); err != nil {
					return nil, err
				}
			}
			stat.PruneCountValues++
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

	if indexWithHistoryValues {
		// empty indexTable, no need to collect and prune keys out of there
		return stat, nil
	}

	idxCForDeletes, err := rwTx.RwCursorDupSort(ii.indexTable)
	if err != nil {
		return nil, err
	}
	defer idxCForDeletes.Close()
	idxC, err := rwTx.RwCursorDupSort(ii.indexTable)
	if err != nil {
		return nil, err
	}
	defer idxC.Close()

	binary.BigEndian.PutUint64(txKey[:], stat.MinTxNum)
	err = collector.Load(rwTx, "", func(key, _ []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		for txnm, err := idxC.SeekBothRange(key, txKey[:]); txnm != nil; _, txnm, err = idxC.NextDup() {
			if err != nil {
				return err
			}

			txNum := binary.BigEndian.Uint64(txnm)
			if txNum < stat.MinTxNum {
				continue // to bigger txnums
			}
			if txNum > stat.MaxTxNum {
				return nil //  go to next key
			}
			if _, _, err = idxCForDeletes.SeekBothExact(key, txnm); err != nil {
				return err
			}
			if err = idxCForDeletes.DeleteCurrent(); err != nil {
				return err
			}
			mxPruneSizeIndex.Inc()

			select {
			case <-logEvery.C:
				ii.logger.Info("[snapshots] prune index", "name", ii.filenameBase, "pruned tx", stat.PruneCountTx,
					"pruned values", stat.PruneCountValues,
					"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(ii.aggregationStep), float64(txNum)/float64(ii.aggregationStep)))
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	}, etl.TransformArgs{})

	return stat, err
}

func (ic *InvertedIndexContext) DebugEFAllValuesAreInRange(ctx context.Context) error {
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

	for _, item := range ic.files {
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

	efIt       iter.Unary[uint64]
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

func (ic *InvertedIndexContext) IterateChangedKeys(startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator1 {
	var ii1 InvertedIterator1
	ii1.hasNextInDb = true
	ii1.roTx = roTx
	ii1.indexTable = ic.ii.indexTable
	for _, item := range ic.files {
		if item.endTxNum <= startTxNum {
			continue
		}
		if item.startTxNum >= endTxNum {
			break
		}
		if item.endTxNum >= endTxNum {
			ii1.hasNextInDb = false
		}
		g := NewArchiveGetter(item.src.decompressor.MakeGetter(), ic.ii.compression)
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
func (ii *InvertedIndex) collate(ctx context.Context, step uint64, roTx kv.Tx) (map[string]*roaring64.Bitmap, error) {
	stepTo := step + 1
	txFrom, txTo := step*ii.aggregationStep, stepTo*ii.aggregationStep
	start := time.Now()
	defer mxCollateTook.ObserveDuration(start)

	keysCursor, err := roTx.CursorDupSort(ii.indexKeysTable)
	if err != nil {
		return nil, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()
	indexBitmaps := map[string]*roaring64.Bitmap{}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.Next() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s keys cursor: %w", ii.filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo { // [txFrom; txTo)
			break
		}
		bitmap, ok := indexBitmaps[string(v)]
		if !ok {
			bitmap = bitmapdb.NewBitmap64()
			indexBitmaps[string(v)] = bitmap
		}
		bitmap.Add(txNum)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	return indexBitmaps, nil
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

// buildFiles - `step=N` means build file `[N:N+1)` which is equal to [N:N+1)
func (ii *InvertedIndex) buildFiles(ctx context.Context, step uint64, bitmaps map[string]*roaring64.Bitmap, ps *background.ProgressSet) (InvertedFiles, error) {
	var (
		decomp    *seg.Decompressor
		index     *recsplit.Index
		existence *ExistenceFilter
		comp      *seg.Compressor
		err       error
	)
	mxRunningFilesBuilding.Inc()
	defer mxRunningFilesBuilding.Dec()
	closeComp := true
	defer func() {
		if closeComp {
			if comp != nil {
				comp.Close()
			}
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
	datPath := ii.efFilePath(step, step+1)
	keys := make([]string, 0, len(bitmaps))
	for key := range bitmaps {
		keys = append(keys, key)
	}

	slices.Sort(keys)
	{
		p := ps.AddNew(path.Base(datPath), 1)
		defer ps.Delete(p)
		comp, err = seg.NewCompressor(ctx, "snapshots", datPath, ii.dirs.Tmp, seg.MinPatternScore, ii.compressWorkers, log.LvlTrace, ii.logger)
		if err != nil {
			return InvertedFiles{}, fmt.Errorf("create %s compressor: %w", ii.filenameBase, err)
		}
		writer := NewArchiveWriter(comp, ii.compression)
		var buf []byte
		for _, key := range keys {
			if err = writer.AddWord([]byte(key)); err != nil {
				return InvertedFiles{}, fmt.Errorf("add %s key [%x]: %w", ii.filenameBase, key, err)
			}
			bitmap := bitmaps[key]
			ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
			it := bitmap.Iterator()
			for it.HasNext() {
				ef.AddOffset(it.Next())
			}
			ef.Build()
			buf = ef.AppendBytes(buf[:0])
			if err = writer.AddWord(buf); err != nil {
				return InvertedFiles{}, fmt.Errorf("add %s val: %w", ii.filenameBase, err)
			}
		}
		if err = comp.Compress(); err != nil {
			return InvertedFiles{}, fmt.Errorf("compress %s: %w", ii.filenameBase, err)
		}
		comp.Close()
		comp = nil
		ps.Delete(p)
	}
	if decomp, err = seg.NewDecompressor(datPath); err != nil {
		return InvertedFiles{}, fmt.Errorf("open %s decompressor: %w", ii.filenameBase, err)
	}

	if err := ii.buildMapIdx(ctx, step, step+1, decomp, ps); err != nil {
		return InvertedFiles{}, fmt.Errorf("build %s efi: %w", ii.filenameBase, err)
	}
	if index, err = recsplit.OpenIndex(ii.efAccessorFilePath(step, step+1)); err != nil {
		return InvertedFiles{}, err
	}

	if ii.withExistenceIndex {
		idxPath2 := ii.efExistenceIdxFilePath(step, step+1)
		if existence, err = buildIndexFilterThenOpen(ctx, decomp, ii.compression, idxPath2, ii.dirs.Tmp, ii.salt, ps, ii.logger, ii.noFsync); err != nil {
			return InvertedFiles{}, fmt.Errorf("build %s efei: %w", ii.filenameBase, err)
		}
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
	}
	return buildIndex(ctx, data, ii.compression, idxPath, false, cfg, ps, ii.logger, ii.noFsync)
}

func (ii *InvertedIndex) integrateFiles(sf InvertedFiles, txNumFrom, txNumTo uint64) {
	defer ii.reCalcRoFiles()

	if asserts && ii.withExistenceIndex && sf.existence == nil {
		panic(fmt.Errorf("assert: no existence index: %s", sf.decomp.FileName()))
	}

	fi := newFilesItem(txNumFrom, txNumTo, ii.aggregationStep)
	fi.decompressor = sf.decomp
	fi.index = sf.index
	fi.existence = sf.existence
	ii.files.Set(fi)
}

func (ii *InvertedIndex) collectFilesStat() (filesCount, filesSize, idxSize uint64) {
	if ii.files == nil {
		return 0, 0, 0
	}
	ii.files.Walk(func(items []*filesItem) bool {
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
