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

	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/recsplit"
)

// filesItem corresponding to a pair of files (.dat and .idx)
type filesItem struct {
	decompressor *compress.Decompressor
	index        *recsplit.Index
	bindex       *BtIndex
	bm           *bitmapdb.FixedSizeBitmaps
	startTxNum   uint64
	endTxNum     uint64

	// Frozen: file of size StepsInColdFile. Completely immutable.
	// Cold: file of size < StepsInColdFile. Immutable, but can be closed/removed after merge to bigger file.
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
	frozen := endStep-startStep == StepsInColdFile
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
				log.Trace("remove after close", "err", err, "file", i.decompressor.FileName())
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

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
//
// Data-Existence in .kv vs .v files:
//  1. key doesn’t exists, then create: .kv - yes, .v - yes
//  2. acc exists, then update/delete:  .kv - yes, .v - yes
//  3. acc doesn’t exists, then delete: .kv - no,  .v - no
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
	roFiles   atomic.Pointer[[]ctxItem]
	defaultDc *DomainContext
	keysTable string // key -> invertedStep , invertedStep = ^(txNum / aggregationStep), Needs to be table with DupSort
	valsTable string // key + invertedStep -> values
	stats     DomainStats
	wal       *domainWAL

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
		stats:     DomainStats{FilesQueries: &atomic.Uint64{}, TotalQueries: &atomic.Uint64{}},
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

func (d *Domain) DiscardHistory() {
	d.History.DiscardHistory()
	d.defaultDc = d.MakeContext()
	// can't discard domain wal - it required, but can discard history
	d.wal = d.newWriter(d.tmpdir, true, false)
}

func (d *Domain) StartUnbufferedWrites() {
	d.defaultDc = d.MakeContext()
	d.wal = d.newWriter(d.tmpdir, false, false)
	d.History.StartUnbufferedWrites()
}

func (d *Domain) StartWrites() {
	d.defaultDc = d.MakeContext()
	d.wal = d.newWriter(d.tmpdir, true, false)
	d.History.StartWrites()
}

func (d *Domain) FinishWrites() {
	if d.defaultDc != nil {
		d.defaultDc.Close()
	}
	if d.wal != nil {
		d.wal.close()
		d.wal = nil
	}
	d.History.FinishWrites()
}

// OpenList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (d *Domain) OpenList(coldNames, warmNames []string) error {
	if err := d.History.OpenList(coldNames, warmNames); err != nil {
		return err
	}
	return d.openList(coldNames)
}

func (d *Domain) openList(coldNames []string) error {
	d.closeWhatNotInList(coldNames)
	d.garbageFiles = d.scanStateFiles(coldNames)
	if err := d.openFiles(); err != nil {
		return fmt.Errorf("Domain.OpenList: %s, %w", d.filenameBase, err)
	}
	return nil
}

func (d *Domain) OpenFolder() error {
	files, warmNames, err := d.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return d.OpenList(files, warmNames)
}

func (d *Domain) GetAndResetStats() DomainStats {
	r := d.stats
	r.DataSize, r.IndexSize, r.FilesCount = d.collectFilesStats()

	d.stats = DomainStats{FilesQueries: &atomic.Uint64{}, TotalQueries: &atomic.Uint64{}}
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

			if item.index == nil {
				idxPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))
				if dir.FileExist(idxPath) {
					if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
						d.logger.Debug("InvertedIndex.openFiles: %w, %s", err, idxPath)
						return false
					}
					totalKeys += item.index.KeyCount()
				}
			}
			if item.bindex == nil {
				bidxPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.bt", d.filenameBase, fromStep, toStep))
				if dir.FileExist(bidxPath) {
					if item.bindex, err = OpenBtreeIndexWithDecompressor(bidxPath, DefaultBtreeM, item.decompressor); err != nil {
						d.logger.Debug("InvertedIndex.openFiles: %w, %s", err, bidxPath)
						return false
					}
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

func (d *Domain) PutWithPrev(key1, key2, val, preval []byte) error {
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `preval`` slice is invalidated
	if err := d.History.AddPrevValue(key1, key2, preval); err != nil {
		return err
	}
	return d.wal.addValue(key1, key2, val)
}

func (d *Domain) DeleteWithPrev(key1, key2, prev []byte) (err error) {
	// This call to update needs to happen before d.tx.Delete() later, because otherwise the content of `original`` slice is invalidated
	if err := d.History.AddPrevValue(key1, key2, prev); err != nil {
		return err
	}
	return d.wal.addValue(key1, key2, nil)
}

func (d *Domain) update(key []byte) error {
	var invertedStep [8]byte
	binary.BigEndian.PutUint64(invertedStep[:], ^(d.txNum / d.aggregationStep))
	//fmt.Printf("put: %s, %x, %x\n", d.filenameBase, key, invertedStep[:])
	if err := d.tx.Put(d.keysTable, key, invertedStep[:]); err != nil {
		return err
	}
	return nil
}

func (d *Domain) put(key, val []byte) error {
	if err := d.update(key); err != nil {
		return err
	}
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	//fmt.Printf("put2: %s, %x, %x\n", d.filenameBase, keySuffix, val)
	return d.tx.Put(d.valsTable, keySuffix, val)
}

// Deprecated
func (d *Domain) Put(key1, key2, val []byte) error {
	key := common.Append(key1, key2)
	original, _, err := d.defaultDc.getLatest(key, d.tx)
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
	return d.put(key, val)
}

// Deprecated
func (d *Domain) Delete(key1, key2 []byte) error {
	key := common.Append(key1, key2)
	original, found, err := d.defaultDc.getLatest(key, d.tx)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return d.DeleteWithPrev(key1, key2, original)
}

func (d *Domain) newWriter(tmpdir string, buffered, discard bool) *domainWAL {
	w := &domainWAL{d: d,
		tmpdir:      tmpdir,
		buffered:    buffered,
		discard:     discard,
		aux:         make([]byte, 0, 128),
		largeValues: d.largeValues,
	}

	if buffered {
		w.values = etl.NewCollector(d.valsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), d.logger)
		w.values.LogLvl(log.LvlTrace)
		w.keys = etl.NewCollector(d.keysTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), d.logger)
		w.keys.LogLvl(log.LvlTrace)
	}
	return w
}

type domainWAL struct {
	d           *Domain
	keys        *etl.Collector
	values      *etl.Collector
	aux         []byte
	tmpdir      string
	buffered    bool
	discard     bool
	largeValues bool
}

func (h *domainWAL) close() {
	if h == nil { // allow dobule-close
		return
	}
	if h.keys != nil {
		h.keys.Close()
	}
	if h.values != nil {
		h.values.Close()
	}
}

func (h *domainWAL) flush(ctx context.Context, tx kv.RwTx) error {
	if h.discard || !h.buffered {
		return nil
	}
	if err := h.keys.Load(tx, h.d.keysTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := h.values.Load(tx, h.d.valsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	return nil
}

func (h *domainWAL) addValue(key1, key2, value []byte) error {
	if h.discard {
		return nil
	}

	offt, kl := 8, len(key1)+len(key2)
	fullkey := h.aux[:kl+offt]
	copy(fullkey, key1)
	copy(fullkey[len(key1):], key2)

	istep := ^(h.d.txNum / h.d.aggregationStep)
	binary.BigEndian.PutUint64(fullkey[kl:], istep)

	if h.largeValues {
		if !h.buffered {
			if err := h.d.tx.Put(h.d.keysTable, fullkey[:kl], fullkey[kl:]); err != nil {
				return err
			}
			if err := h.d.tx.Put(h.d.valsTable, fullkey, value); err != nil {
				return err
			}
			return nil
		}

		if err := h.keys.Collect(fullkey[:kl], fullkey[kl:]); err != nil {
			return err
		}
		if err := h.values.Collect(fullkey, value); err != nil {
			return err
		}

		return nil
	}

	if !h.buffered {
		if err := h.d.tx.Put(h.d.keysTable, fullkey[kl:], fullkey[:kl]); err != nil {
			return err
		}
		if err := h.d.tx.Put(h.d.valsTable, fullkey[:kl], common.Append(fullkey[kl:], value)); err != nil {
			return err
		}
		return nil
	}
	if err := h.keys.Collect(fullkey[kl:], fullkey[:kl]); err != nil {
		return err
	}
	if err := h.values.Collect(fullkey[:kl], common.Append(fullkey[kl:], value)); err != nil {
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
	c        kv.CursorDupSort
	iter     btree2.MapIter[string, []byte]
	dg       *compress.Getter
	dg2      *compress.Getter
	btCursor *Cursor
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
	reader          *recsplit.IndexReader
	file            *ctxItem
	aggregationStep uint64
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

	kBuf, vBuf []byte
	//loc *ctxLocalityIdx
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
		//loc:   d.domainLocalityIndex.MakeContext(),
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
	HistoryCollation
	valuesComp  *compress.Compressor
	valuesPath  string
	valuesCount int
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

// collate gathers domain changes over the specified step, using read-only transaction,
// and returns compressors, elias fano, and bitmaps
// [txFrom; txTo)
func (d *Domain) collate(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx) (Collation, error) {
	started := time.Now()
	defer func() {
		d.stats.LastCollationTook = time.Since(started)
	}()
	mxRunningCollations.Inc()
	defer mxRunningCollations.Dec()
	defer mxCollateTook.UpdateDuration(started)

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
	if valuesComp, err = compress.NewCompressor(context.Background(), "collate values", valuesPath, d.tmpdir, compress.MinPatternScore, d.compressWorkers, log.LvlTrace, d.logger); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}

	keysCursor, err := roTx.CursorDupSort(d.keysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	var (
		pos      uint64
		valCount int
		pairs    = make(chan kvpair, 1024)
	)

	eg, _ := errgroup.WithContext(ctx)
	defer eg.Wait()
	eg.Go(func() (errInternal error) {
		valCount, errInternal = d.writeCollationPair(valuesComp, pairs)
		return errInternal
	})

	var (
		stepBytes = make([]byte, 8)
		keySuffix = make([]byte, 256+8)
	)
	binary.BigEndian.PutUint64(stepBytes, ^step)
	if err := func() error {
		defer close(pairs)

		if !d.largeValues {
			panic("implement me")
		}
		for k, stepInDB, err := keysCursor.First(); k != nil; k, stepInDB, err = keysCursor.Next() {
			if err != nil {
				return err
			}
			pos++
			if ^binary.BigEndian.Uint64(stepInDB) != step {
				continue
			}

			copy(keySuffix, k)
			copy(keySuffix[len(k):], stepInDB)
			v, err := roTx.GetOne(d.valsTable, keySuffix[:len(k)+8])
			if err != nil {
				return fmt.Errorf("find last %s value for aggregation step k=[%x]: %w", d.filenameBase, k, err)
			}
			pairs <- kvpair{k: k, v: v}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	}(); err != nil {
		return Collation{}, fmt.Errorf("iterate over %s keys cursor: %w", d.filenameBase, err)
	}
	if err := eg.Wait(); err != nil {
		return Collation{}, fmt.Errorf("collate over %s keys cursor: %w", d.filenameBase, err)
	}

	closeComp = false
	return Collation{
		HistoryCollation: hCollation,
		valuesPath:       valuesPath,
		valuesComp:       valuesComp,
		valuesCount:      valCount,
	}, nil
}

type StaticFiles struct {
	HistoryFiles
	valuesDecomp *compress.Decompressor
	valuesIdx    *recsplit.Index
	valuesBt     *BtIndex
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
	start := time.Now()
	defer func() {
		d.stats.LastFileBuildingTook = time.Since(start)
	}()

	hStaticFiles, err := d.History.buildFiles(ctx, step, collation.HistoryCollation, ps)
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
		HistoryFiles: hStaticFiles,
		valuesDecomp: valuesDecomp,
		valuesIdx:    valuesIdx,
		valuesBt:     bt,
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
func (d *Domain) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	d.History.BuildMissedIndices(ctx, g, ps)
	for _, item := range d.missedIdxFiles() {
		//TODO: build .kvi
		fitem := item
		g.Go(func() error {
			idxPath := fitem.decompressor.FilePath()
			idxPath = strings.TrimSuffix(idxPath, "kv") + "bt"

			p := ps.AddNew(fitem.decompressor.FileName(), uint64(fitem.decompressor.Count()))
			defer ps.Delete(p)

			if err := BuildBtreeIndexWithDecompressor(idxPath, fitem.decompressor, p, d.tmpdir, d.logger); err != nil {
				return fmt.Errorf("failed to build btree index for %s:  %w", fitem.decompressor.FileName(), err)
			}
			return nil
		})
	}
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
			keyPos = g.Skip()

			p.Processed.Add(1)
		}
		if err = rs.Build(); err != nil {
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
	d.History.integrateFiles(sf.HistoryFiles, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, d.aggregationStep)
	fi.decompressor = sf.valuesDecomp
	fi.index = sf.valuesIdx
	fi.bindex = sf.valuesBt
	d.files.Set(fi)

	d.reCalcRoFiles()
}

// unwind is similar to prune but the difference is that it restores domain values from the history as of txFrom
func (d *Domain) unwind(ctx context.Context, step, txFrom, txTo, limit uint64, f func(step uint64, k, v []byte) error) error {
	keysCursorForDeletes, err := d.tx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", d.filenameBase, err)
	}
	defer keysCursorForDeletes.Close()
	keysCursor, err := d.tx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	var k, v []byte
	var valsC kv.RwCursor
	var valsCDup kv.RwCursorDupSort

	if d.largeValues {
		valsC, err = d.tx.RwCursor(d.valsTable)
		if err != nil {
			return err
		}
		defer valsC.Close()
	} else {
		valsCDup, err = d.tx.RwCursorDupSort(d.valsTable)
		if err != nil {
			return err
		}
		defer valsCDup.Close()
	}

	//fmt.Printf("unwind %s txs [%d; %d) step %d\n", d.filenameBase, txFrom, txTo, step)
	mc := d.MakeContext()
	defer mc.Close()

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^step)

	restore := d.newWriter(filepath.Join(d.tmpdir, "prune_"+d.filenameBase), true, false)

	for k, v, err = keysCursor.First(); err == nil && k != nil; k, v, err = keysCursor.Next() {
		if !bytes.Equal(v, stepBytes) {
			continue
		}

		edgeRecords, err := d.History.unwindKey(k, txFrom, d.tx)
		//fmt.Printf("unwind %x to tx %d edges %+v\n", k, txFrom, edgeRecords)
		if err != nil {
			return err
		}
		switch len(edgeRecords) {
		case 1: // its value should be nil, actual value is in domain, BUT if txNum exactly match, need to restore
			//fmt.Printf("recent %x txn %d '%x'\n", k, edgeRecords[0].TxNum, edgeRecords[0].Value)
			if edgeRecords[0].TxNum == txFrom && edgeRecords[0].Value != nil {
				d.SetTxNum(edgeRecords[0].TxNum)
				if err := restore.addValue(k, nil, edgeRecords[0].Value); err != nil {
					return err
				}
			} else if edgeRecords[0].TxNum < txFrom {
				continue
			}
		case 2: // here one first value is before txFrom (holds txNum when value was set) and second is after (actual value at that txNum)
			l, r := edgeRecords[0], edgeRecords[1]
			if r.TxNum >= txFrom /*&& l.TxNum < txFrom*/ && r.Value != nil {
				d.SetTxNum(l.TxNum)
				if err := restore.addValue(k, nil, r.Value); err != nil {
					return err
				}
			} else {
				continue
			}
			//fmt.Printf("restore %x txn [%d, %d] '%x' '%x'\n", k, l.TxNum, r.TxNum, l.Value, r.Value)
		}

		seek := common.Append(k, stepBytes)
		if d.largeValues {
			kk, vv, err := valsC.SeekExact(seek)
			if err != nil {
				return err
			}
			if f != nil {
				if err := f(step, kk, vv); err != nil {
					return err
				}
			}
			if kk != nil {
				//fmt.Printf("rm large value %x v %x\n", kk, vv)
				if err = valsC.DeleteCurrent(); err != nil {
					return err
				}
			}
		} else {
			vv, err := valsCDup.SeekBothRange(seek, nil)
			if err != nil {
				return err
			}
			if f != nil {
				if err := f(step, k, vv); err != nil {
					return err
				}
			}
			//dups, err := valsCDup.CountDuplicates()
			//if err != nil {
			//	return err
			//}
			//
			//fmt.Printf("rm %d dupes %x v %x\n", dups, seek, vv)
			if err = valsCDup.DeleteCurrentDuplicates(); err != nil {
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
	if err != nil {
		return fmt.Errorf("iterate over %s domain keys: %w", d.filenameBase, err)
	}

	if err = restore.flush(ctx, d.tx); err != nil {
		return err
	}

	logEvery := time.NewTicker(time.Second * 30)
	defer logEvery.Stop()

	if err := d.History.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return fmt.Errorf("prune history at step %d [%d, %d): %w", step, txFrom, txTo, err)
	}
	return nil
}

// history prunes keys in range [txFrom; txTo), domain prunes whole step.
func (d *Domain) prune(ctx context.Context, step, txFrom, txTo, limit uint64, logEvery *time.Ticker) error {
	mxPruneTook.Update(d.stats.LastPruneTook.Seconds())
	if d.filenameBase == "accounts" {
		log.Warn("[dbg] prune", "step", step)
	}
	keysCursorForDeletes, err := d.tx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", d.filenameBase, err)
	}
	defer keysCursorForDeletes.Close()
	keysCursor, err := d.tx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	var k, v []byte
	var valsC kv.RwCursor
	var valsCDup kv.RwCursorDupSort
	if d.largeValues {
		valsC, err = d.tx.RwCursor(d.valsTable)
		if err != nil {
			return err
		}
		defer valsC.Close()
	} else {
		valsCDup, err = d.tx.RwCursorDupSort(d.valsTable)
		if err != nil {
			return err
		}
		defer valsCDup.Close()
	}

	mc := d.MakeContext()
	defer mc.Close()

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^step)

	for k, v, err = keysCursor.First(); k != nil; k, v, err = keysCursor.Next() {
		if err != nil {
			return fmt.Errorf("iterate over %s domain keys: %w", d.filenameBase, err)
		}
		if ^binary.BigEndian.Uint64(v) > step {
			continue
		}
		//fmt.Printf("prune: %x, %d,%d\n", k, ^binary.BigEndian.Uint64(v), step)
		err = d.tx.Delete(d.valsTable, common.Append(k, v))
		if err != nil {
			return err
		}
		mxPruneSize.Inc()
		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if _, _, err = keysCursorForDeletes.SeekBothExact(k, v); err != nil {
			return err
		}
		if err = keysCursorForDeletes.DeleteCurrent(); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			d.logger.Info("[snapshots] prune domain", "name", d.filenameBase, "step", step)
			//"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(d.aggregationStep), float64(txTo)/float64(d.aggregationStep)))
		default:
		}
	}

	if err := d.History.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return fmt.Errorf("prune history at step %d [%d, %d): %w", step, txFrom, txTo, err)
	}
	mxPruneHistTook.Update(d.stats.LastPruneHistTook.Seconds())
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
	for ; k != nil; k, v, err = domainKeysCursor.Next() {
		if err != nil {
			return fmt.Errorf("iterate over %s domain keys: %w", d.filenameBase, err)
		}
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

	return d.History.warmup(ctx, txFrom, limit, tx)
}

func (d *Domain) Rotate() flusher {
	hf := d.History.Rotate()
	if d.wal != nil {
		hf.d = d.wal
		d.wal = d.newWriter(d.wal.tmpdir, d.wal.buffered, d.wal.discard)
	}
	return hf
}

var COMPARE_INDEXES = false // if true, will compare values from Btree and INvertedIndex

func (dc *DomainContext) getBeforeTxNumFromFiles(filekey []byte, fromTxNum uint64) (v []byte, found bool, err error) {
	dc.d.stats.FilesQueries.Add(1)
	var k []byte
	var ok bool
	for i := len(dc.files) - 1; i >= 0; i-- {
		if dc.files[i].endTxNum < fromTxNum {
			break
		}
		k, v, ok, err = dc.statelessBtree(i).Get(filekey, k[:0], v[:0])
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		found = true
		break
	}
	return v, found, nil
}

func (dc *DomainContext) getLatestFromFiles2(filekey []byte) (v []byte, found bool, err error) {
	dc.d.stats.FilesQueries.Add(1)

	// find what has LocalityIndex
	lastIndexedTxNum := dc.hc.ic.coldLocality.indexedTo()
	// grind non-indexed files
	var ok bool
	for i := len(dc.files) - 1; i >= 0; i-- {
		if dc.files[i].src.endTxNum <= lastIndexedTxNum {
			break
		}

		dc.kBuf, dc.vBuf, ok, err = dc.statelessBtree(i).Get(filekey, dc.kBuf[:0], dc.vBuf[:0])
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		found = true
		if COMPARE_INDEXES {
			rd := recsplit.NewIndexReader(dc.files[i].src.index)
			oft := rd.Lookup(filekey)
			gt := dc.statelessGetter(i)
			gt.Reset(oft)
			var kk, vv []byte
			if gt.HasNext() {
				kk, _ = gt.Next(nil)
				vv, _ = gt.Next(nil)
			}
			fmt.Printf("key: %x, val: %x\n", kk, vv)
			if !bytes.Equal(vv, v) {
				panic("not equal")
			}
		}

		if found {
			return common.Copy(dc.vBuf), true, nil
		}
		return nil, false, nil
	}

	// still not found, search in indexed cold shards
	return dc.getLatestFromColdFiles(filekey)
}
func (dc *DomainContext) getLatestFromFiles(filekey []byte) (v []byte, found bool, err error) {
	dc.d.stats.FilesQueries.Add(1)

	if v, found, err = dc.getLatestFromWarmFiles(filekey); err != nil {
		return nil, false, err
	} else if found {
		return v, true, nil
	}

	// sometimes there is a gap between indexed cold files and indexed warm files. just grind them.
	// possible reasons:
	// - no locality indices at all
	// - cold locality index is "lazy"-built
	// corner cases:
	// - cold and warm segments can overlap
	lastColdIndexedTxNum := dc.hc.ic.coldLocality.indexedTo()
	firstWarmIndexedTxNum := dc.hc.ic.warmLocality.indexedFrom()
	if firstWarmIndexedTxNum == 0 && len(dc.files) > 0 {
		firstWarmIndexedTxNum = dc.files[len(dc.files)-1].endTxNum
	}
	if firstWarmIndexedTxNum > lastColdIndexedTxNum {
		for i := len(dc.files) - 1; i >= 0; i-- {
			isUseful := dc.files[i].startTxNum >= lastColdIndexedTxNum && dc.files[i].endTxNum <= firstWarmIndexedTxNum
			if !isUseful {
				continue
			}
			var ok bool
			dc.kBuf, dc.vBuf, ok, err = dc.statelessBtree(i).Get(filekey, dc.kBuf[:0], dc.vBuf[:0])
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}
			return common.Copy(dc.vBuf), true, nil
		}
	}

	// still not found, search in indexed cold shards
	return dc.getLatestFromColdFiles(filekey)
}

func (dc *DomainContext) getLatestFromWarmFiles(filekey []byte) ([]byte, bool, error) {
	exactWarmStep, ok, err := dc.hc.ic.warmLocality.lookupLatest(filekey)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	// grind non-indexed files
	exactTxNum := exactWarmStep * dc.d.aggregationStep
	for i := len(dc.files) - 1; i >= 0; i-- {
		isUseful := dc.files[i].startTxNum <= exactTxNum && dc.files[i].endTxNum > exactTxNum
		if !isUseful {
			continue
		}

		dc.kBuf, dc.vBuf, ok, err = dc.statelessBtree(i).Get(filekey, dc.kBuf[:0], dc.vBuf[:0])
		if err != nil {
			return nil, false, err
		}
		if !ok {
			break
		}
		return common.Copy(dc.vBuf), true, nil
	}
	return nil, false, nil
}

func (dc *DomainContext) getLatestFromColdFiles(filekey []byte) (v []byte, found bool, err error) {
	exactColdShard, ok, err := dc.hc.ic.coldLocality.lookupLatest(filekey)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	dc.kBuf, dc.vBuf, ok, err = dc.statelessBtree(int(exactColdShard)).Get(filekey, dc.kBuf[:0], dc.vBuf[:0])
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, err
	}
	return common.Copy(dc.vBuf), true, nil
}

// historyBeforeTxNum searches history for a value of specified key before txNum
// second return value is true if the value is found in the history (even if it is nil)
func (dc *DomainContext) historyBeforeTxNum(key []byte, txNum uint64, roTx kv.Tx) (v []byte, found bool, err error) {
	dc.d.stats.FilesQueries.Add(1)

	{
		v, found, err = dc.hc.GetNoState(key, txNum)
		if err != nil {
			return nil, false, err
		}
		if found {
			return v, true, nil
		}
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
		var k []byte
		var ok bool
		for i := len(dc.files) - 1; i >= 0; i-- {
			if dc.files[i].startTxNum > topState.startTxNum {
				continue
			}
			k, v, ok, err = dc.statelessBtree(i).Get(key, k[:0], v[:0])
			if err != nil {
				return nil, false, err
			}
			if !ok {
				continue
			}
			found = true
			break
		}
		return v, found, nil
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
	if v, _, err = dc.getBeforeTxNum(key, txNum, roTx); err != nil {
		return nil, err
	}
	return v, nil
}

func (dc *DomainContext) Close() {
	if dc.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := dc.files
	dc.files = nil
	for _, item := range files {
		if item.src.frozen {
			continue
		}
		refCnt := item.src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && item.src.canDelete.Load() {
			item.src.closeFilesAndRemove()
		}
	}
	//for _, r := range dc.readers {
	//	r.Close()
	//}
	dc.hc.Close()
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

func (dc *DomainContext) getBeforeTxNum(key []byte, fromTxNum uint64, roTx kv.Tx) ([]byte, bool, error) {
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
		v, found, err := dc.getBeforeTxNumFromFiles(key, fromTxNum)
		if err != nil {
			return nil, false, err
		}
		return v, found, nil
	}
	copy(dc.keyBuf[:], key)
	copy(dc.keyBuf[len(key):], foundInvStep)
	v, err := roTx.GetOne(dc.d.valsTable, dc.keyBuf[:len(key)+8])
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (dc *DomainContext) getLatest(key []byte, roTx kv.Tx) ([]byte, bool, error) {
	dc.d.stats.TotalQueries.Add(1)

	foundInvStep, err := roTx.GetOne(dc.d.keysTable, key) // reads first DupSort value
	if err != nil {
		return nil, false, err
	}
	if foundInvStep == nil {
		v, found, err := dc.getLatestFromFiles(key)
		if err != nil {
			return nil, false, err
		}
		return v, found, nil
	}
	if !dc.d.largeValues {
		panic("implement me")
	}
	copy(dc.keyBuf[:], key)
	copy(dc.keyBuf[len(key):], foundInvStep)
	v, err := roTx.GetOne(dc.d.valsTable, dc.keyBuf[:len(key)+8])
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (dc *DomainContext) GetLatest(key1, key2 []byte, roTx kv.Tx) ([]byte, bool, error) {
	copy(dc.keyBuf[:], key1)
	copy(dc.keyBuf[len(key1):], key2)
	return dc.getLatest(dc.keyBuf[:len(key1)+len(key2)], roTx)
}

func (dc *DomainContext) IteratePrefix(roTx kv.Tx, prefix []byte, it func(k, v []byte)) error {
	dc.d.stats.TotalQueries.Add(1)

	var cp CursorHeap
	heap.Init(&cp)
	var k, v []byte
	var err error

	keysCursor, err := roTx.CursorDupSort(dc.d.keysTable)
	if err != nil {
		return err
	}
	defer keysCursor.Close()
	if k, v, err = keysCursor.Seek(prefix); err != nil {
		return err
	}
	if k != nil && bytes.HasPrefix(k, prefix) {
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		step := ^binary.BigEndian.Uint64(v)
		txNum := step * dc.d.aggregationStep
		if v, err = roTx.GetOne(dc.d.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(&cp, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), c: keysCursor, endTxNum: txNum, reverse: true})
	}

	for i, item := range dc.files {
		cursor, err := dc.statelessBtree(i).Seek(prefix)
		if err != nil {
			return err
		}
		if cursor == nil {
			continue
		}

		dc.d.stats.FilesQueries.Add(1)
		key := cursor.Key()
		if key != nil && bytes.HasPrefix(key, prefix) {
			val := cursor.Value()
			heap.Push(&cp, &CursorItem{t: FILE_CURSOR, key: key, val: val, btCursor: cursor, endTxNum: item.endTxNum, reverse: true})
		}
	}

	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)

		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(&cp).(*CursorItem)
			switch ci1.t {
			case FILE_CURSOR:
				if ci1.btCursor.Next() {
					ci1.key = ci1.btCursor.Key()
					if ci1.key != nil && bytes.HasPrefix(ci1.key, prefix) {
						ci1.val = ci1.btCursor.Value()
						heap.Push(&cp, ci1)
					}
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
					if v, err = roTx.GetOne(dc.d.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = common.Copy(v)
					heap.Push(&cp, ci1)
				}
			}
		}
		if len(lastVal) > 0 {
			it(lastKey, lastVal)
		}
	}
	return nil
}

func (dc *DomainContext) DomainRange(tx kv.Tx, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it iter.KV, err error) {
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
	histStateIt, err := dc.hc.WalkAsOf(ts, fromKey, toKey, tx, limit)
	if err != nil {
		return nil, err
	}
	lastestStateIt, err := dc.DomainRangeLatest(tx, fromKey, toKey, limit)
	if err != nil {
		return nil, err
	}
	return iter.UnionKV(histStateIt, lastestStateIt, limit), nil
}

func (dc *DomainContext) IteratePrefix2(roTx kv.Tx, fromKey, toKey []byte, limit int) (iter.KV, error) {
	return dc.DomainRangeLatest(roTx, fromKey, toKey, limit)
}

func (dc *DomainContext) DomainRangeLatest(roTx kv.Tx, fromKey, toKey []byte, limit int) (iter.KV, error) {
	fit := &DomainLatestIterFile{from: fromKey, to: toKey, limit: limit, dc: dc,
		roTx:         roTx,
		idxKeysTable: dc.d.keysTable,
		h:            &CursorHeap{},
	}
	if err := fit.init(dc); err != nil {
		return nil, err
	}
	return fit, nil
}

type DomainLatestIterFile struct {
	dc *DomainContext

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
func (hi *DomainLatestIterFile) init(dc *DomainContext) error {
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
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		step := ^binary.BigEndian.Uint64(v)
		txNum := step * dc.d.aggregationStep
		if v, err = hi.roTx.GetOne(dc.d.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(hi.h, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), c: keysCursor, endTxNum: txNum, reverse: true})
	}

	for i, item := range dc.files {
		btCursor, err := dc.statelessBtree(i).Seek(hi.from)
		if err != nil {
			return err
		}
		if btCursor == nil {
			continue
		}

		key := btCursor.Key()
		if key != nil && (hi.to == nil || bytes.Compare(key, hi.to) < 0) {
			val := btCursor.Value()
			heap.Push(hi.h, &CursorItem{t: FILE_CURSOR, key: key, val: val, btCursor: btCursor, endTxNum: item.endTxNum, reverse: true})
		}
	}
	return hi.advanceInFiles()
}

func (hi *DomainLatestIterFile) advanceInFiles() error {
	for hi.h.Len() > 0 {
		lastKey := common.Copy((*hi.h)[0].key)
		lastVal := common.Copy((*hi.h)[0].val)

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
	ad1, ad2 := d.stepsRangeInDB(tx)
	return fmt.Sprintf("%s:(%.0f-%.0f, %.0f-%.0f)", d.filenameBase, ad1, ad2, a1, a2)
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
	return from, to
}

func (dc *DomainContext) Files() (res []string) {
	for _, item := range dc.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return append(res, dc.hc.Files()...)
}
