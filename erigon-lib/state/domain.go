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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common/background"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/recsplit"
)

// filesItem corresponding to a pair of files (.dat and .idx)
type filesItem struct {
	decompressor *compress.Decompressor
	index        *recsplit.Index
	bindex       *BtIndex
	startTxNum   uint64
	endTxNum     uint64

	// Frozen: file of size StepsInBiggestFile. Completely immutable.
	// Cold: file of size < StepsInBiggestFile. Immutable, but can be closed/removed after merge to bigger file.
	// Hot: Stored in DB. Providing Snapshot-Isolation by CopyOnWrite.
	frozen   bool         // immutable, don't need atomic
	refcount atomic.Int32 // only for `frozen=false`

	// file can be deleted in 2 cases: 1. when `refcount == 0 && canDelete == true` 2. on app startup when `file.isSubsetOfFrozenFile()`
	// other processes (which also reading files, may have same logic)
	canDelete atomic.Bool
}

func newFilesItem(startTxNum, endTxNum uint64, stepSize uint64) *filesItem {
	startStep := startTxNum / stepSize
	endStep := endTxNum / stepSize
	frozen := endStep-startStep == StepsInBiggestFile
	return &filesItem{startTxNum: startTxNum, endTxNum: endTxNum, frozen: frozen}
}

func (i *filesItem) isSubsetOf(j *filesItem) bool {
	return (j.startTxNum <= i.startTxNum && i.endTxNum <= j.endTxNum) && (j.startTxNum != i.startTxNum || i.endTxNum != j.endTxNum)
}

func filesItemLess(i, j *filesItem) bool {
	if i.endTxNum == j.endTxNum {
		return i.startTxNum > j.startTxNum
	}
	return i.endTxNum < j.endTxNum
}
func (i *filesItem) closeFilesAndRemove() {
	if i.decompressor != nil {
		i.decompressor.Close()
		// paranoic-mode on: don't delete frozen files
		if !i.frozen {
			if err := os.Remove(i.decompressor.FilePath()); err != nil {
				log.Trace("close", "err", err, "file", i.decompressor.FileName())
			}
		}
		i.decompressor = nil
	}
	if i.index != nil {
		i.index.Close()
		// paranoic-mode on: don't delete frozen files
		if !i.frozen {
			if err := os.Remove(i.index.FilePath()); err != nil {
				log.Trace("close", "err", err, "file", i.index.FileName())
			}
		}
		i.index = nil
	}
	if i.bindex != nil {
		i.bindex.Close()
		if err := os.Remove(i.bindex.FilePath()); err != nil {
			log.Trace("close", "err", err, "file", i.bindex.FileName())
		}
		i.bindex = nil
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

	HistoryQueries *atomic.Uint64
	TotalQueries   *atomic.Uint64
	EfSearchTime   time.Duration
	DataSize       uint64
	IndexSize      uint64
	FilesCount     uint64
}

func (ds *DomainStats) Accumulate(other DomainStats) {
	ds.HistoryQueries.Add(other.HistoryQueries.Load())
	ds.TotalQueries.Add(other.TotalQueries.Load())
	ds.EfSearchTime += other.EfSearchTime
	ds.IndexSize += other.IndexSize
	ds.DataSize += other.DataSize
	ds.FilesCount += other.FilesCount
}

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
type Domain struct {
	/*
	   not large:
	    	keys: key -> ^step
	    	vals: key -> ^step+value (DupSort)
	   large:
	    	keys: key -> ^step
	   	    vals: key + ^step -> value
	*/

	*History
	files *btree2.BTreeG[*filesItem] // thread-safe, but maybe need 1 RWLock for all trees in AggregatorV3
	// roFiles derivative from field `file`, but without garbage (canDelete=true, overlaps, etc...)
	// MakeContext() using this field in zero-copy way
	roFiles     atomic.Pointer[[]ctxItem]
	defaultDc   *DomainContext
	keysTable   string // key -> invertedStep , invertedStep = ^(txNum / aggregationStep), Needs to be table with DupSort
	valsTable   string // key + invertedStep -> values
	stats       DomainStats
	mergesCount uint64

	garbageFiles []*filesItem // files that exist on disk, but ignored on opening folder - because they are garbage
	logger       log.Logger
}

func NewDomain(dir, tmpdir string, aggregationStep uint64,
	filenameBase, keysTable, valsTable, indexKeysTable, historyValsTable, indexTable string,
	compressVals, largeValues bool, logger log.Logger) (*Domain, error) {
	d := &Domain{
		keysTable: keysTable,
		valsTable: valsTable,
		files:     btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		stats:     DomainStats{HistoryQueries: &atomic.Uint64{}, TotalQueries: &atomic.Uint64{}},
		logger:    logger,
	}
	d.roFiles.Store(&[]ctxItem{})

	var err error
	if d.History, err = NewHistory(dir, tmpdir, aggregationStep, filenameBase, indexKeysTable, indexTable, historyValsTable, compressVals, []string{"kv"}, largeValues, logger); err != nil {
		return nil, err
	}

	return d, nil
}

// LastStepInDB - return the latest available step in db (at-least 1 value in such step)
func (d *Domain) LastStepInDB(tx kv.Tx) (lstInDb uint64) {
	lst, _ := kv.FirstKey(tx, d.valsTable)
	if len(lst) > 0 {
		lstInDb = ^binary.BigEndian.Uint64(lst[len(lst)-8:])
	}
	return lstInDb
}

func (d *Domain) StartWrites() {
	d.defaultDc = d.MakeContext()
	d.History.StartWrites()
}

func (d *Domain) FinishWrites() {
	d.defaultDc.Close()
	d.History.FinishWrites()
}

// OpenList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (d *Domain) OpenList(fNames []string) error {
	if err := d.History.OpenList(fNames); err != nil {
		return err
	}
	return d.openList(fNames)
}

func (d *Domain) openList(fNames []string) error {
	d.closeWhatNotInList(fNames)
	d.garbageFiles = d.scanStateFiles(fNames)
	if err := d.openFiles(); err != nil {
		return fmt.Errorf("History.OpenList: %s, %w", d.filenameBase, err)
	}
	return nil
}

func (d *Domain) OpenFolder() error {
	files, err := d.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return d.OpenList(files)
}

func (d *Domain) GetAndResetStats() DomainStats {
	r := d.stats
	r.DataSize, r.IndexSize, r.FilesCount = d.collectFilesStats()

	d.stats = DomainStats{}
	return r
}

func (d *Domain) scanStateFiles(fileNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^" + d.filenameBase + ".([0-9]+)-([0-9]+).kv$")
	var err error
Loop:
	for _, name := range fileNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 3 {
			if len(subs) != 0 {
				d.logger.Warn("File ignored by domain scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			d.logger.Warn("File ignored by domain scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			d.logger.Warn("File ignored by domain scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			d.logger.Warn("File ignored by domain scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*d.aggregationStep, endStep*d.aggregationStep
		var newFile = newFilesItem(startTxNum, endTxNum, d.aggregationStep)

		for _, ext := range d.integrityFileExtensions {
			requiredFile := fmt.Sprintf("%s.%d-%d.%s", d.filenameBase, startStep, endStep, ext)
			if !dir.FileExist(filepath.Join(d.dir, requiredFile)) {
				d.logger.Debug(fmt.Sprintf("[snapshots] skip %s because %s doesn't exists", name, requiredFile))
				garbageFiles = append(garbageFiles, newFile)
				continue Loop
			}
		}

		if _, has := d.files.Get(newFile); has {
			continue
		}

		addNewFile := true
		var subSets []*filesItem
		d.files.Walk(func(items []*filesItem) bool {
			for _, item := range items {
				if item.isSubsetOf(newFile) {
					subSets = append(subSets, item)
					continue
				}

				if newFile.isSubsetOf(item) {
					if item.frozen {
						addNewFile = false
						garbageFiles = append(garbageFiles, newFile)
					}
					continue
				}
			}
			return true
		})
		if addNewFile {
			d.files.Set(newFile)
		}
	}
	return garbageFiles
}

func (d *Domain) openFiles() (err error) {
	var totalKeys uint64

	invalidFileItems := make([]*filesItem, 0)
	d.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				continue
			}
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			datPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, fromStep, toStep))
			if !dir.FileExist(datPath) {
				invalidFileItems = append(invalidFileItems, item)
				continue
			}
			if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				return false
			}

			if item.index != nil {
				continue
			}
			idxPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))
			if dir.FileExist(idxPath) {
				if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
					d.logger.Debug("InvertedIndex.openFiles: %w, %s", err, idxPath)
					return false
				}
				totalKeys += item.index.KeyCount()
			}
			if item.bindex == nil {
				bidxPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.bt", d.filenameBase, fromStep, toStep))
				if item.bindex, err = OpenBtreeIndexWithDecompressor(bidxPath, 2048, item.decompressor); err != nil {
					d.logger.Debug("InvertedIndex.openFiles: %w, %s", err, bidxPath)
					return false
				}
				//totalKeys += item.bindex.KeyCount()
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	for _, item := range invalidFileItems {
		d.files.Delete(item)
	}

	d.reCalcRoFiles()
	return nil
}

func (d *Domain) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	d.files.Walk(func(items []*filesItem) bool {
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
		if item.bindex != nil {
			item.bindex.Close()
			item.bindex = nil
		}
		d.files.Delete(item)
	}
}

func (d *Domain) reCalcRoFiles() {
	roFiles := ctxFiles(d.files)
	d.roFiles.Store(&roFiles)
}

func (d *Domain) Close() {
	d.History.Close()
	d.closeWhatNotInList([]string{})
	d.reCalcRoFiles()
}

func (dc *DomainContext) get(key []byte, fromTxNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	//var invertedStep [8]byte
	dc.d.stats.TotalQueries.Add(1)

	invertedStep := dc.numBuf
	binary.BigEndian.PutUint64(invertedStep[:], ^(fromTxNum / dc.d.aggregationStep))
	keyCursor, err := roTx.CursorDupSort(dc.d.keysTable)
	if err != nil {
		return nil, false, err
	}
	defer keyCursor.Close()
	foundInvStep, err := keyCursor.SeekBothRange(key, invertedStep[:])
	if err != nil {
		return nil, false, err
	}
	if len(foundInvStep) == 0 {
		dc.d.stats.HistoryQueries.Add(1)
		return dc.readFromFiles(key, fromTxNum)
	}
	//keySuffix := make([]byte, len(key)+8)
	copy(dc.keyBuf[:], key)
	copy(dc.keyBuf[len(key):], foundInvStep)
	v, err := roTx.GetOne(dc.d.valsTable, dc.keyBuf[:len(key)+8])
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (dc *DomainContext) Get(key1, key2 []byte, roTx kv.Tx) ([]byte, error) {
	//key := make([]byte, len(key1)+len(key2))
	copy(dc.keyBuf[:], key1)
	copy(dc.keyBuf[len(key1):], key2)
	// keys larger than 52 bytes will panic
	v, _, err := dc.get(dc.keyBuf[:len(key1)+len(key2)], dc.d.txNum, roTx)
	return v, err
}

func (d *Domain) update(key, original []byte) error {
	var invertedStep [8]byte
	binary.BigEndian.PutUint64(invertedStep[:], ^(d.txNum / d.aggregationStep))
	if err := d.tx.Put(d.keysTable, key, invertedStep[:]); err != nil {
		return err
	}
	return nil
}

func (d *Domain) Put(key1, key2, val []byte) error {
	key := make([]byte, len(key1)+len(key2))
	copy(key, key1)
	copy(key[len(key1):], key2)
	original, _, err := d.defaultDc.get(key, d.txNum, d.tx)
	if err != nil {
		return err
	}
	if bytes.Equal(original, val) {
		return nil
	}
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `original`` slice is invalidated
	if err = d.History.AddPrevValue(key1, key2, original); err != nil {
		return err
	}
	if err = d.update(key, original); err != nil {
		return err
	}
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err = d.tx.Put(d.valsTable, keySuffix, val); err != nil {
		return err
	}
	return nil
}

func (d *Domain) Delete(key1, key2 []byte) error {
	key := make([]byte, len(key1)+len(key2))
	copy(key, key1)
	copy(key[len(key1):], key2)
	original, found, err := d.defaultDc.get(key, d.txNum, d.tx)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	// This call to update needs to happen before d.tx.Delete() later, because otherwise the content of `original`` slice is invalidated
	if err = d.History.AddPrevValue(key1, key2, original); err != nil {
		return err
	}
	if err = d.update(key, original); err != nil {
		return err
	}
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err = d.tx.Delete(d.valsTable, keySuffix); err != nil {
		return err
	}
	return nil
}

type CursorType uint8

const (
	FILE_CURSOR CursorType = iota
	DB_CURSOR
)

// CursorItem is the item in the priority queue used to do merge interation
// over storage of a given account
type CursorItem struct {
	c        kv.CursorDupSort
	dg       *compress.Getter
	dg2      *compress.Getter
	key      []byte
	val      []byte
	endTxNum uint64
	t        CursorType // Whether this item represents state file or DB record, or tree
	reverse  bool
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

// filesItem corresponding to a pair of files (.dat and .idx)
type ctxItem struct {
	getter     *compress.Getter
	reader     *recsplit.IndexReader
	startTxNum uint64
	endTxNum   uint64

	i   int
	src *filesItem
}

type ctxLocalityIdx struct {
	reader *recsplit.IndexReader
	bm     *bitmapdb.FixedSizeBitmaps
	file   *ctxItem
}

func ctxItemLess(i, j ctxItem) bool { //nolint
	if i.endTxNum == j.endTxNum {
		return i.startTxNum > j.startTxNum
	}
	return i.endTxNum < j.endTxNum
}

// DomainContext allows accesing the same domain from multiple go-routines
type DomainContext struct {
	d       *Domain
	files   []ctxItem
	getters []*compress.Getter
	readers []*BtIndex
	hc      *HistoryContext
	keyBuf  [60]byte // 52b key and 8b for inverted step
	numBuf  [8]byte
}

func (dc *DomainContext) statelessGetter(i int) *compress.Getter {
	if dc.getters == nil {
		dc.getters = make([]*compress.Getter, len(dc.files))
	}
	r := dc.getters[i]
	if r == nil {
		r = dc.files[i].src.decompressor.MakeGetter()
		dc.getters[i] = r
	}
	return r
}

func (dc *DomainContext) statelessBtree(i int) *BtIndex {
	if dc.readers == nil {
		dc.readers = make([]*BtIndex, len(dc.files))
	}
	r := dc.readers[i]
	if r == nil {
		r = dc.files[i].src.bindex
		dc.readers[i] = r
	}
	return r
}

func (d *Domain) collectFilesStats() (datsz, idxsz, files uint64) {
	d.History.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil {
				return false
			}
			datsz += uint64(item.decompressor.Size())
			idxsz += uint64(item.index.Size())
			files += 2
		}
		return true
	})

	d.files.Walk(func(items []*filesItem) bool {
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

func (d *Domain) MakeContext() *DomainContext {
	dc := &DomainContext{
		d:     d,
		hc:    d.History.MakeContext(),
		files: *d.roFiles.Load(),
	}
	for _, item := range dc.files {
		if !item.src.frozen {
			item.src.refcount.Add(1)
		}
	}

	return dc
}

// Collation is the set of compressors created after aggregation
type Collation struct {
	valuesComp   *compress.Compressor
	historyComp  *compress.Compressor
	indexBitmaps map[string]*roaring64.Bitmap
	valuesPath   string
	historyPath  string
	valuesCount  int
	historyCount int
}

func (c Collation) Close() {
	if c.valuesComp != nil {
		c.valuesComp.Close()
	}
	if c.historyComp != nil {
		c.historyComp.Close()
	}
}

type kvpair struct {
	k, v []byte
}

func (d *Domain) writeCollationPair(valuesComp *compress.Compressor, pairs chan kvpair) (count int, err error) {
	for kv := range pairs {
		if err = valuesComp.AddUncompressedWord(kv.k); err != nil {
			return count, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, kv.k, err)
		}
		mxCollationSize.Inc()
		count++ // Only counting keys, not values
		if err = valuesComp.AddUncompressedWord(kv.v); err != nil {
			return count, fmt.Errorf("add %s values val [%x]=>[%x]: %w", d.filenameBase, kv.k, kv.v, err)
		}
	}
	return count, nil
}

// nolint
func (d *Domain) aggregate(ctx context.Context, step uint64, txFrom, txTo uint64, tx kv.Tx, ps *background.ProgressSet) (err error) {
	mxRunningCollations.Inc()
	start := time.Now()
	collation, err := d.collateStream(ctx, step, txFrom, txTo, tx)
	mxRunningCollations.Dec()
	mxCollateTook.UpdateDuration(start)

	mxCollationSize.Set(uint64(collation.valuesComp.Count()))
	mxCollationSizeHist.Set(uint64(collation.historyComp.Count()))

	if err != nil {
		collation.Close()
		//return fmt.Errorf("domain collation %q has failed: %w", d.filenameBase, err)
		return err
	}

	mxRunningMerges.Inc()

	start = time.Now()
	sf, err := d.buildFiles(ctx, step, collation, ps)
	collation.Close()
	defer sf.Close()

	if err != nil {
		sf.Close()
		mxRunningMerges.Dec()
		return
	}

	mxRunningMerges.Dec()

	d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
	d.stats.LastFileBuildingTook = time.Since(start)
	return nil
}

// collate gathers domain changes over the specified step, using read-only transaction,
// and returns compressors, elias fano, and bitmaps
// [txFrom; txTo)
func (d *Domain) collateStream(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx) (Collation, error) {
	started := time.Now()
	defer func() {
		d.stats.LastCollationTook = time.Since(started)
	}()

	hCollation, err := d.History.collate(step, txFrom, txTo, roTx)
	if err != nil {
		return Collation{}, err
	}

	var valuesComp *compress.Compressor
	closeComp := true
	defer func() {
		if closeComp {
			if valuesComp != nil {
				valuesComp.Close()
			}
		}
	}()

	valuesPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, step, step+1))
	if valuesComp, err = compress.NewCompressor(context.Background(), "collate values", valuesPath, d.tmpdir, compress.MinPatternScore, 1, log.LvlTrace, d.logger); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}

	keysCursor, err := roTx.CursorDupSort(d.keysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	var (
		k, v     []byte
		pos      uint64
		valCount int
		pairs    = make(chan kvpair, 1024)
	)

	//totalKeys, err := keysCursor.Count()
	//if err != nil {
	//	return Collation{}, fmt.Errorf("failed to obtain keys count for domain %q", d.filenameBase)
	//}

	eg, _ := errgroup.WithContext(ctx)
	eg.Go(func() error {
		valCount, err = d.writeCollationPair(valuesComp, pairs)
		return err
	})

	var (
		stepBytes = make([]byte, 8)
		keySuffix = make([]byte, 256+8)
	)
	binary.BigEndian.PutUint64(stepBytes, ^step)

	for k, _, err = keysCursor.First(); err == nil && k != nil; k, _, err = keysCursor.NextNoDup() {
		pos++

		if v, err = keysCursor.LastDup(); err != nil {
			return Collation{}, fmt.Errorf("find last %s key for aggregation step k=[%x]: %w", d.filenameBase, k, err)
		}
		if bytes.Equal(v, stepBytes) {
			copy(keySuffix, k)
			copy(keySuffix[len(k):], v)
			ks := len(k) + len(v)

			v, err := roTx.GetOne(d.valsTable, keySuffix[:ks])
			if err != nil {
				return Collation{}, fmt.Errorf("find last %s value for aggregation step k=[%x]: %w", d.filenameBase, k, err)
			}

			select {
			case <-ctx.Done():
				return Collation{}, ctx.Err()
			default:
			}

			pairs <- kvpair{k: k, v: v}
		}
	}
	close(pairs)
	if err != nil {
		return Collation{}, fmt.Errorf("iterate over %s keys cursor: %w", d.filenameBase, err)
	}

	if err := eg.Wait(); err != nil {
		return Collation{}, fmt.Errorf("collate over %s keys cursor: %w", d.filenameBase, err)
	}

	closeComp = false
	return Collation{
		valuesPath:   valuesPath,
		valuesComp:   valuesComp,
		valuesCount:  valCount,
		historyPath:  hCollation.historyPath,
		historyComp:  hCollation.historyComp,
		historyCount: hCollation.historyCount,
		indexBitmaps: hCollation.indexBitmaps,
	}, nil
}

// collate gathers domain changes over the specified step, using read-only transaction,
// and returns compressors, elias fano, and bitmaps
// [txFrom; txTo)
func (d *Domain) collate(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx, logEvery *time.Ticker) (Collation, error) {
	started := time.Now()
	defer func() {
		d.stats.LastCollationTook = time.Since(started)
	}()

	hCollation, err := d.History.collate(step, txFrom, txTo, roTx)
	if err != nil {
		return Collation{}, err
	}
	var valuesComp *compress.Compressor
	closeComp := true
	defer func() {
		if closeComp {
			hCollation.Close()
			if valuesComp != nil {
				valuesComp.Close()
			}
		}
	}()
	valuesPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, step, step+1))
	if valuesComp, err = compress.NewCompressor(context.Background(), "collate values", valuesPath, d.tmpdir, compress.MinPatternScore, 1, log.LvlTrace, d.logger); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}
	keysCursor, err := roTx.CursorDupSort(d.keysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	var (
		k, v        []byte
		pos         uint64
		valuesCount uint
	)

	//TODO: use prorgesSet
	//totalKeys, err := keysCursor.Count()
	//if err != nil {
	//	return Collation{}, fmt.Errorf("failed to obtain keys count for domain %q", d.filenameBase)
	//}
	for k, _, err = keysCursor.First(); err == nil && k != nil; k, _, err = keysCursor.NextNoDup() {
		if err != nil {
			return Collation{}, err
		}
		pos++
		select {
		case <-ctx.Done():
			d.logger.Warn("[snapshots] collate domain cancelled", "name", d.filenameBase, "err", ctx.Err())
			return Collation{}, ctx.Err()
		default:
		}

		if v, err = keysCursor.LastDup(); err != nil {
			return Collation{}, fmt.Errorf("find last %s key for aggregation step k=[%x]: %w", d.filenameBase, k, err)
		}
		s := ^binary.BigEndian.Uint64(v)
		if s == step {
			keySuffix := make([]byte, len(k)+8)
			copy(keySuffix, k)
			copy(keySuffix[len(k):], v)
			v, err := roTx.GetOne(d.valsTable, keySuffix)
			if err != nil {
				return Collation{}, fmt.Errorf("find last %s value for aggregation step k=[%x]: %w", d.filenameBase, k, err)
			}
			if err = valuesComp.AddUncompressedWord(k); err != nil {
				return Collation{}, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, k, err)
			}
			valuesCount++ // Only counting keys, not values
			if err = valuesComp.AddUncompressedWord(v); err != nil {
				return Collation{}, fmt.Errorf("add %s values val [%x]=>[%x]: %w", d.filenameBase, k, v, err)
			}
		}
	}
	if err != nil {
		return Collation{}, fmt.Errorf("iterate over %s keys cursor: %w", d.filenameBase, err)
	}
	closeComp = false
	return Collation{
		valuesPath:   valuesPath,
		valuesComp:   valuesComp,
		valuesCount:  int(valuesCount),
		historyPath:  hCollation.historyPath,
		historyComp:  hCollation.historyComp,
		historyCount: hCollation.historyCount,
		indexBitmaps: hCollation.indexBitmaps,
	}, nil
}

type StaticFiles struct {
	valuesDecomp    *compress.Decompressor
	valuesIdx       *recsplit.Index
	valuesBt        *BtIndex
	historyDecomp   *compress.Decompressor
	historyIdx      *recsplit.Index
	efHistoryDecomp *compress.Decompressor
	efHistoryIdx    *recsplit.Index
}

func (sf StaticFiles) Close() {
	if sf.valuesDecomp != nil {
		sf.valuesDecomp.Close()
	}
	if sf.valuesIdx != nil {
		sf.valuesIdx.Close()
	}
	if sf.valuesBt != nil {
		sf.valuesBt.Close()
	}
	if sf.historyDecomp != nil {
		sf.historyDecomp.Close()
	}
	if sf.historyIdx != nil {
		sf.historyIdx.Close()
	}
	if sf.efHistoryDecomp != nil {
		sf.efHistoryDecomp.Close()
	}
	if sf.efHistoryIdx != nil {
		sf.efHistoryIdx.Close()
	}
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (d *Domain) buildFiles(ctx context.Context, step uint64, collation Collation, ps *background.ProgressSet) (StaticFiles, error) {
	hStaticFiles, err := d.History.buildFiles(ctx, step, HistoryCollation{
		historyPath:  collation.historyPath,
		historyComp:  collation.historyComp,
		historyCount: collation.historyCount,
		indexBitmaps: collation.indexBitmaps,
	}, ps)
	if err != nil {
		return StaticFiles{}, err
	}
	valuesComp := collation.valuesComp
	var valuesDecomp *compress.Decompressor
	var valuesIdx *recsplit.Index
	closeComp := true
	defer func() {
		if closeComp {
			hStaticFiles.Close()
			if valuesComp != nil {
				valuesComp.Close()
			}
			if valuesDecomp != nil {
				valuesDecomp.Close()
			}
			if valuesIdx != nil {
				valuesIdx.Close()
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
	if valuesDecomp, err = compress.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}

	valuesIdxFileName := fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, step, step+1)
	valuesIdxPath := filepath.Join(d.dir, valuesIdxFileName)
	{
		p := ps.AddNew(valuesIdxFileName, uint64(valuesDecomp.Count()*2))
		defer ps.Delete(p)
		if valuesIdx, err = buildIndexThenOpen(ctx, valuesDecomp, valuesIdxPath, d.tmpdir, collation.valuesCount, false, p, d.logger, d.noFsync); err != nil {
			return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
		}
	}

	var bt *BtIndex
	{
		btFileName := strings.TrimSuffix(valuesIdxFileName, "kvi") + "bt"
		btPath := filepath.Join(d.dir, btFileName)
		p := ps.AddNew(btFileName, uint64(valuesDecomp.Count()*2))
		defer ps.Delete(p)
		bt, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesDecomp, p, d.tmpdir, d.logger)
		if err != nil {
			return StaticFiles{}, fmt.Errorf("build %s values bt idx: %w", d.filenameBase, err)
		}
	}

	closeComp = false
	return StaticFiles{
		valuesDecomp:    valuesDecomp,
		valuesIdx:       valuesIdx,
		valuesBt:        bt,
		historyDecomp:   hStaticFiles.historyDecomp,
		historyIdx:      hStaticFiles.historyIdx,
		efHistoryDecomp: hStaticFiles.efHistoryDecomp,
		efHistoryIdx:    hStaticFiles.efHistoryIdx,
	}, nil
}

func (d *Domain) missedIdxFiles() (l []*filesItem) {
	d.files.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			if !dir.FileExist(filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.bt", d.filenameBase, fromStep, toStep))) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (d *Domain) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) (err error) {
	d.History.BuildMissedIndices(ctx, g, ps)
	d.InvertedIndex.BuildMissedIndices(ctx, g, ps)
	for _, item := range d.missedIdxFiles() {
		//TODO: build .kvi
		fitem := item
		g.Go(func() error {
			idxPath := filepath.Join(fitem.decompressor.FilePath(), fitem.decompressor.FileName())
			idxPath = strings.TrimSuffix(idxPath, "kv") + "bt"

			p := ps.AddNew("fixme", uint64(fitem.decompressor.Count()))
			defer ps.Delete(p)
			if err := BuildBtreeIndexWithDecompressor(idxPath, fitem.decompressor, p, d.tmpdir, d.logger); err != nil {
				return fmt.Errorf("failed to build btree index for %s:  %w", fitem.decompressor.FileName(), err)
			}
			return nil
		})
	}
	return nil
}

func buildIndexThenOpen(ctx context.Context, d *compress.Decompressor, idxPath, tmpdir string, count int, values bool, p *background.Progress, logger log.Logger, noFsync bool) (*recsplit.Index, error) {
	if err := buildIndex(ctx, d, idxPath, tmpdir, count, values, p, logger, noFsync); err != nil {
		return nil, err
	}
	return recsplit.OpenIndex(idxPath)
}

func buildIndex(ctx context.Context, d *compress.Decompressor, idxPath, tmpdir string, count int, values bool, p *background.Progress, logger log.Logger, noFsync bool) error {
	var rs *recsplit.RecSplit
	var err error
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpdir,
		IndexFile:  idxPath,
	}, logger); err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)
	if noFsync {
		rs.DisableFsync()
	}
	defer d.EnableMadvNormal().DisableReadAhead()

	word := make([]byte, 0, 256)
	var keyPos, valPos uint64
	g := d.MakeGetter()
	for {
		if err := ctx.Err(); err != nil {
			logger.Warn("recsplit index building cancelled", "err", err)
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

func (d *Domain) integrateFiles(sf StaticFiles, txNumFrom, txNumTo uint64) {
	d.History.integrateFiles(HistoryFiles{
		historyDecomp:   sf.historyDecomp,
		historyIdx:      sf.historyIdx,
		efHistoryDecomp: sf.efHistoryDecomp,
		efHistoryIdx:    sf.efHistoryIdx,
	}, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, d.aggregationStep)
	fi.decompressor = sf.valuesDecomp
	fi.index = sf.valuesIdx
	fi.bindex = sf.valuesBt
	d.files.Set(fi)

	d.reCalcRoFiles()
}

// [txFrom; txTo)
func (d *Domain) prune(ctx context.Context, step uint64, txFrom, txTo, limit uint64, logEvery *time.Ticker) error {
	defer func(t time.Time) { d.stats.LastPruneTook = time.Since(t) }(time.Now())
	mxPruningProgress.Inc()
	defer mxPruningProgress.Dec()

	var (
		_state    = "scan steps"
		pos       atomic.Uint64
		totalKeys uint64
	)

	keysCursor, err := d.tx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("%s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	totalKeys, err = keysCursor.Count()
	if err != nil {
		return fmt.Errorf("get count of %s keys: %w", d.filenameBase, err)
	}

	var (
		k, v, stepBytes []byte
		keyMaxSteps     = make(map[string]uint64)
		c               = 0
	)
	stepBytes = make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^step)

	for k, v, err = keysCursor.First(); err == nil && k != nil; k, v, err = keysCursor.Next() {
		if bytes.Equal(v, stepBytes) {
			c++
			kl, vl, err := keysCursor.PrevDup()
			if err != nil {
				break
			}
			if kl == nil && vl == nil {
				continue
			}
			s := ^binary.BigEndian.Uint64(vl)
			if s > step {
				_, vn, err := keysCursor.NextDup()
				if err != nil {
					break
				}
				if bytes.Equal(vn, stepBytes) {
					if err := keysCursor.DeleteCurrent(); err != nil {
						return fmt.Errorf("prune key %x: %w", k, err)
					}
					mxPruneSize.Inc()
					keyMaxSteps[string(k)] = s
				}
			}
		}
		pos.Add(1)

		if ctx.Err() != nil {
			d.logger.Warn("[snapshots] prune domain cancelled", "name", d.filenameBase, "err", ctx.Err())
			return ctx.Err()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			d.logger.Info("[snapshots] prune domain", "name", d.filenameBase,
				"stage", _state,
				"range", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(d.aggregationStep), float64(txTo)/float64(d.aggregationStep)),
				"progress", fmt.Sprintf("%.2f%%", (float64(pos.Load())/float64(totalKeys))*100))
		default:
		}
	}
	if err != nil {
		return fmt.Errorf("iterate of %s keys: %w", d.filenameBase, err)
	}

	pos.Store(0)
	// It is important to clean up tables in a specific order
	// First keysTable, because it is the first one access in the `get` function, i.e. if the record is deleted from there, other tables will not be accessed
	var valsCursor kv.RwCursor
	if valsCursor, err = d.tx.RwCursor(d.valsTable); err != nil {
		return fmt.Errorf("%s vals cursor: %w", d.filenameBase, err)
	}
	defer valsCursor.Close()

	for k, _, err := valsCursor.First(); err == nil && k != nil; k, _, err = valsCursor.Next() {
		if bytes.HasSuffix(k, stepBytes) {
			if _, ok := keyMaxSteps[string(k)]; !ok {
				continue
			}
			if err := valsCursor.DeleteCurrent(); err != nil {
				return fmt.Errorf("prune val %x: %w", k, err)
			}
			mxPruneSize.Inc()
		}
		pos.Add(1)
		//_prog = 100 * (float64(pos) / float64(totalKeys))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			d.logger.Info("[snapshots] prune domain", "name", d.filenameBase, "step", step)
			//"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(d.aggregationStep), float64(txTo)/float64(d.aggregationStep)))
		default:
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s vals: %w", d.filenameBase, err)
	}

	defer func(t time.Time) { d.stats.LastPruneHistTook = time.Since(t) }(time.Now())

	if err = d.History.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return fmt.Errorf("prune history at step %d [%d, %d): %w", step, txFrom, txTo, err)
	}
	return nil
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

// nolint
func (d *Domain) warmup(ctx context.Context, txFrom, limit uint64, tx kv.Tx) error {
	domainKeysCursor, err := tx.CursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", d.filenameBase, err)
	}
	defer domainKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	idxC, err := tx.CursorDupSort(d.keysTable)
	if err != nil {
		return err
	}
	defer idxC.Close()
	valsC, err := tx.Cursor(d.valsTable)
	if err != nil {
		return err
	}
	defer valsC.Close()
	k, v, err := domainKeysCursor.Seek(txKey[:])
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	txFrom = binary.BigEndian.Uint64(k)
	txTo := txFrom + d.aggregationStep
	if limit != math.MaxUint64 && limit != 0 {
		txTo = txFrom + limit
	}
	for ; err == nil && k != nil; k, v, err = domainKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		_, _, _ = valsC.Seek(v[len(v)-8:])
		_, _ = idxC.SeekBothRange(v[:len(v)-8], k)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s domain keys: %w", d.filenameBase, err)
	}

	return d.History.warmup(ctx, txFrom, limit, tx)
}

var COMPARE_INDEXES = false // if true, will compare values from Btree and INvertedIndex

func (dc *DomainContext) readFromFiles(filekey []byte, fromTxNum uint64) ([]byte, bool, error) {
	var val []byte
	var found bool

	for i := len(dc.files) - 1; i >= 0; i-- {
		if dc.files[i].endTxNum < fromTxNum {
			break
		}
		reader := dc.statelessBtree(i)
		if reader.Empty() {
			continue
		}
		cur, err := reader.Seek(filekey)
		if err != nil {
			//return nil, false, nil //TODO: uncomment me
			return nil, false, err
		}
		if cur == nil {
			continue
		}

		if bytes.Equal(cur.Key(), filekey) {
			val = cur.Value()
			found = true
			break
		}
	}
	return val, found, nil
}

// historyBeforeTxNum searches history for a value of specified key before txNum
// second return value is true if the value is found in the history (even if it is nil)
func (dc *DomainContext) historyBeforeTxNum(key []byte, txNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	dc.d.stats.HistoryQueries.Add(1)

	v, found, err := dc.hc.GetNoState(key, txNum)
	if err != nil {
		return nil, false, err
	}
	if found {
		return v, true, nil
	}

	var anyItem bool
	var topState ctxItem
	for _, item := range dc.hc.ic.files {
		if item.endTxNum < txNum {
			continue
		}
		anyItem = true
		topState = item
		break
	}
	if anyItem {
		// If there were no changes but there were history files, the value can be obtained from value files
		var val []byte
		for i := len(dc.files) - 1; i >= 0; i-- {
			if dc.files[i].startTxNum > topState.startTxNum {
				continue
			}
			reader := dc.statelessBtree(i)
			if reader.Empty() {
				continue
			}
			cur, err := reader.Seek(key)
			if err != nil {
				dc.d.logger.Warn("failed to read history before from file", "key", key, "err", err)
				return nil, false, err
			}
			if cur == nil {
				continue
			}
			if bytes.Equal(cur.Key(), key) {
				val = cur.Value()
				break
			}
		}
		return val, true, nil
	}
	// Value not found in history files, look in the recent history
	if roTx == nil {
		return nil, false, fmt.Errorf("roTx is nil")
	}
	return dc.hc.getNoStateFromDB(key, txNum, roTx)
}

// GetBeforeTxNum does not always require usage of roTx. If it is possible to determine
// historical value based only on static files, roTx will not be used.
func (dc *DomainContext) GetBeforeTxNum(key []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	v, hOk, err := dc.historyBeforeTxNum(key, txNum, roTx)
	if err != nil {
		return nil, err
	}
	if hOk {
		// if history returned marker of key creation
		// domain must return nil
		if len(v) == 0 {
			return nil, nil
		}
		return v, nil
	}
	if v, _, err = dc.get(key, txNum-1, roTx); err != nil {
		return nil, err
	}
	return v, nil
}

func (dc *DomainContext) Close() {
	for _, item := range dc.files {
		if item.src.frozen {
			continue
		}
		refCnt := item.src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && item.src.canDelete.Load() {
			item.src.closeFilesAndRemove()
		}
	}
	dc.hc.Close()
}

// IteratePrefix iterates over key-value pairs of the domain that start with given prefix
// Such iteration is not intended to be used in public API, therefore it uses read-write transaction
// inside the domain. Another version of this for public API use needs to be created, that uses
// roTx instead and supports ending the iterations before it reaches the end.
func (dc *DomainContext) IteratePrefix(prefix []byte, it func(k, v []byte)) error {
	dc.d.stats.HistoryQueries.Add(1)

	var cp CursorHeap
	heap.Init(&cp)
	var k, v []byte
	var err error
	keysCursor, err := dc.d.tx.CursorDupSort(dc.d.keysTable)
	if err != nil {
		return err
	}
	defer keysCursor.Close()
	if k, v, err = keysCursor.Seek(prefix); err != nil {
		return err
	}
	if bytes.HasPrefix(k, prefix) {
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		step := ^binary.BigEndian.Uint64(v)
		txNum := step * dc.d.aggregationStep
		if v, err = dc.d.tx.GetOne(dc.d.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(&cp, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), c: keysCursor, endTxNum: txNum, reverse: true})
	}

	for i, item := range dc.files {
		bg := dc.statelessBtree(i)
		if bg.Empty() {
			continue
		}

		cursor, err := bg.Seek(prefix)
		if err != nil {
			continue
		}

		g := dc.statelessGetter(i)
		key := cursor.Key()
		if bytes.HasPrefix(key, prefix) {
			val := cursor.Value()
			heap.Push(&cp, &CursorItem{t: FILE_CURSOR, key: key, val: val, dg: g, endTxNum: item.endTxNum, reverse: true})
		}
	}
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			switch ci1.t {
			case FILE_CURSOR:
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.Next(ci1.key[:0])
					if bytes.HasPrefix(ci1.key, prefix) {
						ci1.val, _ = ci1.dg.Next(ci1.val[:0])
						heap.Fix(&cp, 0)
					} else {
						heap.Pop(&cp)
					}
				} else {
					heap.Pop(&cp)
				}
			case DB_CURSOR:
				k, v, err = ci1.c.NextNoDup()
				if err != nil {
					return err
				}
				if k != nil && bytes.HasPrefix(k, prefix) {
					ci1.key = common.Copy(k)
					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = dc.d.tx.GetOne(dc.d.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = common.Copy(v)
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			}
		}
		if len(lastVal) > 0 {
			it(lastKey, lastVal)
		}
	}
	return nil
}
