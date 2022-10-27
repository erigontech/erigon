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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"

	"github.com/ledgerwatch/erigon-lib/common/dir"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

var (
	historyValCountKey = []byte("ValCount")
)

// filesItem corresponding to a pair of files (.dat and .idx)
type filesItem struct {
	decompressor *compress.Decompressor
	index        *recsplit.Index
	startTxNum   uint64
	endTxNum     uint64
}

func (i *filesItem) isSubsetOf(j *filesItem) bool {
	return j.startTxNum <= i.startTxNum && i.endTxNum <= j.endTxNum
}

func filesItemLess(i, j *filesItem) bool {
	if i.endTxNum == j.endTxNum {
		return i.startTxNum > j.startTxNum
	}
	return i.endTxNum < j.endTxNum
}

type DomainStats struct {
	MergesCount    uint64
	HistoryQueries uint64
	EfSearchTime   time.Duration
	DataSize       uint64
	IndexSize      uint64
	FilesCount     uint64
}

func (ds *DomainStats) Accumulate(other DomainStats) {
	ds.HistoryQueries += other.HistoryQueries
	ds.EfSearchTime += other.EfSearchTime
	ds.IndexSize += other.IndexSize
	ds.DataSize += other.DataSize
	ds.FilesCount += other.FilesCount
}

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
type Domain struct {
	*History
	files       *btree.BTreeG[*filesItem] // Static files pertaining to this domain, items are of type `filesItem`
	defaultDc   *DomainContext
	keysTable   string // key -> invertedStep , invertedStep = ^(txNum / aggregationStep), Needs to be table with DupSort
	valsTable   string // key + invertedStep -> values
	stats       DomainStats
	prefixLen   int // Number of bytes in the keys that can be used for prefix iteration
	mergesCount uint64
}

func NewDomain(
	dir, tmpdir string,
	aggregationStep uint64,
	filenameBase string,
	keysTable string,
	valsTable string,
	indexKeysTable string,
	historyValsTable string,
	settingsTable string,
	indexTable string,
	prefixLen int,
	compressVals bool,
) (*Domain, error) {
	d := &Domain{
		keysTable: keysTable,
		valsTable: valsTable,
		prefixLen: prefixLen,
		files:     btree.NewG[*filesItem](32, filesItemLess),
	}
	var err error
	if d.History, err = NewHistory(dir, tmpdir, aggregationStep, filenameBase, indexKeysTable, indexTable, historyValsTable, settingsTable, compressVals); err != nil {
		return nil, err
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	d.scanStateFiles(files)
	if err = d.openFiles(); err != nil {
		return nil, err
	}
	d.defaultDc = d.MakeContext()
	return d, nil
}

func (d *Domain) GetAndResetStats() DomainStats {
	r := d.stats
	d.stats = DomainStats{}
	return r
}

func (d *Domain) scanStateFiles(files []fs.DirEntry) {
	re := regexp.MustCompile("^" + d.filenameBase + ".([0-9]+)-([0-9]+).kv$")
	var err error
	var uselessFiles []string
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 3 {
			if len(subs) != 0 {
				log.Warn("File ignored by domain scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			log.Warn("File ignored by domain scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by domain scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			log.Warn("File ignored by domain scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*d.aggregationStep, endStep*d.aggregationStep
		var item = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		{
			var subSet, superSet *filesItem
			d.files.DescendLessOrEqual(item, func(it *filesItem) bool {
				if it.isSubsetOf(item) {
					subSet = it
				} else if item.isSubsetOf(it) {
					superSet = it
				}
				return true
			})
			if subSet != nil {
				d.files.Delete(subSet)
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, subSet.startTxNum/d.aggregationStep, subSet.endTxNum/d.aggregationStep),
					fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, subSet.startTxNum/d.aggregationStep, subSet.endTxNum/d.aggregationStep),
				)
			}
			if superSet != nil {
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, startStep, endStep),
					fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, startStep, endStep),
				)
				continue
			}
		}
		{
			var subSet, superSet *filesItem
			d.files.AscendGreaterOrEqual(item, func(it *filesItem) bool {
				if it.isSubsetOf(item) {
					subSet = it
				} else if item.isSubsetOf(it) {
					superSet = it
				}
				return false
			})
			if subSet != nil {
				d.files.Delete(subSet)
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, subSet.startTxNum/d.aggregationStep, subSet.endTxNum/d.aggregationStep),
					fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, subSet.startTxNum/d.aggregationStep, subSet.endTxNum/d.aggregationStep),
				)
			}
			if superSet != nil {
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, startStep, endStep),
					fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, startStep, endStep),
				)
				continue
			}
		}
		d.files.ReplaceOrInsert(item)
	}
	if len(uselessFiles) > 0 {
		log.Info("[snapshots] history can delete", "files", strings.Join(uselessFiles, ","))
	}

}

func (d *Domain) openFiles() error {
	var err error
	var totalKeys uint64

	invalidFileItems := make([]*filesItem, 0)
	d.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
		datPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, fromStep, toStep))
		if !dir.FileExist(datPath) {
			invalidFileItems = append(invalidFileItems, item)
			return true
		}
		if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			return false
		}

		if item.index == nil {
			idxPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))
			if dir.FileExist(idxPath) {
				if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
					log.Debug("InvertedIndex.openFiles: %w, %s", err, idxPath)
					return false
				}
				totalKeys += item.index.KeyCount()
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
	return nil
}

func (d *Domain) closeFiles() {
	d.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (d *Domain) Close() {
	// Closing state files only after background aggregation goroutine is finished
	d.History.Close()
	d.closeFiles()
}

func (dc *DomainContext) get(key []byte, fromTxNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	var invertedStep [8]byte
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
		atomic.AddUint64(&dc.d.stats.HistoryQueries, 1)
		v, found := dc.readFromFiles(key, fromTxNum)
		return v, found, nil
	}
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	copy(keySuffix[len(key):], foundInvStep)
	v, err := roTx.GetOne(dc.d.valsTable, keySuffix)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (dc *DomainContext) Get(key1, key2 []byte, roTx kv.Tx) ([]byte, error) {
	key := make([]byte, len(key1)+len(key2))
	copy(key, key1)
	copy(key[len(key1):], key2)
	v, _, err := dc.get(key, dc.d.txNum, roTx)
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
	*ch = old[0 : n-1]
	return x
}

// filesItem corresponding to a pair of files (.dat and .idx)
type ctxItem struct {
	getter     *compress.Getter
	reader     *recsplit.IndexReader
	startTxNum uint64
	endTxNum   uint64
}

func ctxItemLess(i, j ctxItem) bool {
	if i.endTxNum == j.endTxNum {
		return i.startTxNum > j.startTxNum
	}
	return i.endTxNum < j.endTxNum
}

// DomainContext allows accesing the same domain from multiple go-routines
type DomainContext struct {
	d     *Domain
	files *btree.BTreeG[ctxItem]
	hc    *HistoryContext
}

func (d *Domain) MakeContext() *DomainContext {
	dc := &DomainContext{d: d}
	dc.hc = d.History.MakeContext()
	bt := btree.NewG[ctxItem](32, ctxItemLess)
	dc.files = bt
	var datsz, idxsz, files uint64

	d.files.Ascend(func(item *filesItem) bool {
		if item.index == nil {
			return false
		}
		getter := item.decompressor.MakeGetter()
		datsz += uint64(getter.Size())
		idxsz += uint64(item.index.Size())
		files += 2

		bt.ReplaceOrInsert(ctxItem{
			startTxNum: item.startTxNum,
			endTxNum:   item.endTxNum,
			getter:     getter,
			reader:     recsplit.NewIndexReader(item.index),
		})
		return true
	})
	d.stats.DataSize, d.stats.IndexSize, d.stats.FilesCount = datsz, idxsz, files
	return dc
}

// IteratePrefix iterates over key-value pairs of the domain that start with given prefix
// The length of the prefix has to match the `prefixLen` parameter used to create the domain
// Such iteration is not intended to be used in public API, therefore it uses read-write transaction
// inside the domain. Another version of this for public API use needs to be created, that uses
// roTx instead and supports ending the iterations before it reaches the end.
func (dc *DomainContext) IteratePrefix(prefix []byte, it func(k, v []byte)) error {
	if len(prefix) != dc.d.prefixLen {
		return fmt.Errorf("wrong prefix length, this %s domain supports prefixLen %d, given [%x]", dc.d.filenameBase, dc.d.prefixLen, prefix)
	}
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
	dc.files.Ascend(func(item ctxItem) bool {
		if item.reader.Empty() {
			return true
		}
		offset := item.reader.Lookup(prefix)
		// Creating dedicated getter because the one in the item may be used to delete storage, for example
		g := item.getter
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(prefix); !keyMatch {
				return true
			}
			g.Skip()
		}
		if g.HasNext() {
			key, _ := g.Next(nil)
			if bytes.HasPrefix(key, prefix) {
				val, _ := g.Next(nil)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, key: key, val: val, dg: g, endTxNum: item.endTxNum, reverse: true})
			}
		}
		return true
	})
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

// collate gathers domain changes over the specified step, using read-only transaction,
// and returns compressors, elias fano, and bitmaps
// [txFrom; txTo)
func (d *Domain) collate(step, txFrom, txTo uint64, roTx kv.Tx, logEvery *time.Ticker) (Collation, error) {
	hCollation, err := d.History.collate(step, txFrom, txTo, roTx, logEvery)
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
	if valuesComp, err = compress.NewCompressor(context.Background(), "collate values", valuesPath, d.tmpdir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}
	keysCursor, err := roTx.CursorDupSort(d.keysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()
	var prefix []byte // Track prefix to insert it before entries
	var k, v []byte
	valuesCount := 0
	for k, _, err = keysCursor.First(); err == nil && k != nil; k, _, err = keysCursor.NextNoDup() {
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
			if d.prefixLen > 0 && (prefix == nil || !bytes.HasPrefix(k, prefix)) {
				prefix = append(prefix[:0], k[:d.prefixLen]...)
				if err = valuesComp.AddUncompressedWord(prefix); err != nil {
					return Collation{}, fmt.Errorf("add %s values prefix [%x]: %w", d.filenameBase, prefix, err)
				}
				if err = valuesComp.AddUncompressedWord(nil); err != nil {
					return Collation{}, fmt.Errorf("add %s values prefix val [%x]: %w", d.filenameBase, prefix, err)
				}
				valuesCount++
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
		valuesCount:  valuesCount,
		historyPath:  hCollation.historyPath,
		historyComp:  hCollation.historyComp,
		historyCount: hCollation.historyCount,
		indexBitmaps: hCollation.indexBitmaps,
	}, nil
}

type StaticFiles struct {
	valuesDecomp    *compress.Decompressor
	valuesIdx       *recsplit.Index
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
func (d *Domain) buildFiles(ctx context.Context, step uint64, collation Collation) (StaticFiles, error) {
	hStaticFiles, err := d.History.buildFiles(ctx, step, HistoryCollation{
		historyPath:  collation.historyPath,
		historyComp:  collation.historyComp,
		historyCount: collation.historyCount,
		indexBitmaps: collation.indexBitmaps,
	})
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
	valuesIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, step, step+1))
	if err = valuesComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s values: %w", d.filenameBase, err)
	}
	valuesComp.Close()
	valuesComp = nil
	if valuesDecomp, err = compress.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}
	if valuesIdx, err = buildIndex(ctx, valuesDecomp, valuesIdxPath, d.tmpdir, collation.valuesCount, false); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
	}
	closeComp = false
	return StaticFiles{
		valuesDecomp:    valuesDecomp,
		valuesIdx:       valuesIdx,
		historyDecomp:   hStaticFiles.historyDecomp,
		historyIdx:      hStaticFiles.historyIdx,
		efHistoryDecomp: hStaticFiles.efHistoryDecomp,
		efHistoryIdx:    hStaticFiles.efHistoryIdx,
	}, nil
}

func (d *Domain) missedIdxFiles() (l []*filesItem) {
	d.files.Ascend(func(item *filesItem) bool { // don't run slow logic while iterating on btree
		fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
		if !dir.FileExist(filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))) {
			l = append(l, item)
		}
		return true
	})
	return l
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (d *Domain) BuildMissedIndices(ctx context.Context, sem *semaphore.Weighted) (err error) {
	if err := d.History.BuildMissedIndices(ctx, sem); err != nil {
		return err
	}
	for _, item := range d.missedIdxFiles() {
		//TODO: build .kvi
		_ = item
	}
	return d.openFiles()
}

func buildIndex(ctx context.Context, d *compress.Decompressor, idxPath, tmpdir string, count int, values bool) (*recsplit.Index, error) {
	_, fName := filepath.Split(idxPath)
	log.Debug("[snapshots] build idx", "file", fName)
	var rs *recsplit.RecSplit
	var err error
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpdir,
		IndexFile:  idxPath,
	}); err != nil {
		return nil, fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlDebug)
	defer d.EnableMadvNormal().DisableReadAhead()

	word := make([]byte, 0, 256)
	var keyPos, valPos uint64
	g := d.MakeGetter()
	for {
		g.Reset(0)
		for g.HasNext() {
			word, valPos = g.Next(word[:0])
			if values {
				if err = rs.AddKey(word, valPos); err != nil {
					return nil, fmt.Errorf("add idx key [%x]: %w", word, err)
				}
			} else {
				if err = rs.AddKey(word, keyPos); err != nil {
					return nil, fmt.Errorf("add idx key [%x]: %w", word, err)
				}
			}
			// Skip value
			keyPos = g.Skip()
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return nil, fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	var idx *recsplit.Index
	if idx, err = recsplit.OpenIndex(idxPath); err != nil {
		return nil, fmt.Errorf("open idx: %w", err)
	}
	return idx, nil
}

func (d *Domain) integrateFiles(sf StaticFiles, txNumFrom, txNumTo uint64) {
	d.History.integrateFiles(HistoryFiles{
		historyDecomp:   sf.historyDecomp,
		historyIdx:      sf.historyIdx,
		efHistoryDecomp: sf.efHistoryDecomp,
		efHistoryIdx:    sf.efHistoryIdx,
	}, txNumFrom, txNumTo)
	d.files.ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.valuesDecomp,
		index:        sf.valuesIdx,
	})
}

// [txFrom; txTo)
func (d *Domain) prune(step uint64, txFrom, txTo, limit uint64, logEvery *time.Ticker) error {
	// It is important to clean up tables in a specific order
	// First keysTable, because it is the first one access in the `get` function, i.e. if the record is deleted from there, other tables will not be accessed
	keysCursor, err := d.tx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("%s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()
	var k, v []byte
	keyMaxSteps := make(map[string]uint64)

	for k, v, err = keysCursor.First(); err == nil && k != nil; k, v, err = keysCursor.Next() {
		s := ^binary.BigEndian.Uint64(v)
		if maxS, seen := keyMaxSteps[string(k)]; !seen || s > maxS {
			keyMaxSteps[string(k)] = s
		}
	}
	if err != nil {
		return fmt.Errorf("iterate of %s keys: %w", d.filenameBase, err)
	}

	for k, v, err = keysCursor.First(); err == nil && k != nil; k, v, err = keysCursor.Next() {
		s := ^binary.BigEndian.Uint64(v)
		if s == step {
			if maxS := keyMaxSteps[string(k)]; maxS <= step {
				continue
			}
			if err = keysCursor.DeleteCurrent(); err != nil {
				return fmt.Errorf("clean up %s for [%x]=>[%x]: %w", d.filenameBase, k, v, err)
			}
			//fmt.Printf("domain prune key %x [s%d]\n", string(k), s)
		}
	}
	if err != nil {
		return fmt.Errorf("iterate of %s keys: %w", d.filenameBase, err)
	}
	var valsCursor kv.RwCursor
	if valsCursor, err = d.tx.RwCursor(d.valsTable); err != nil {
		return fmt.Errorf("%s vals cursor: %w", d.filenameBase, err)
	}
	defer valsCursor.Close()
	for k, _, err = valsCursor.First(); err == nil && k != nil; k, _, err = valsCursor.Next() {
		s := ^binary.BigEndian.Uint64(k[len(k)-8:])
		if s == step {
			if maxS := keyMaxSteps[string(k[:len(k)-8])]; maxS <= step {
				continue
			}
			if err = valsCursor.DeleteCurrent(); err != nil {
				return fmt.Errorf("clean up %s for [%x]: %w", d.filenameBase, k, err)
			}
			//fmt.Printf("domain prune value for %x (invs %x) [s%d]\n", string(k),k[len(k)-8):], s)
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s vals: %w", d.filenameBase, err)
	}

	if err = d.History.prune(context.TODO(), txFrom, txTo, limit, logEvery); err != nil {
		return fmt.Errorf("prune history at step %d [%d, %d): %w", step, txFrom, txTo, err)
	}
	return nil
}

func (dc *DomainContext) readFromFiles(filekey []byte, fromTxNum uint64) ([]byte, bool) {
	var val []byte
	var found bool

	dc.files.Descend(func(item ctxItem) bool {
		if item.endTxNum < fromTxNum {
			return false
		}
		if item.reader.Empty() {
			return true
		}
		offset := item.reader.Lookup(filekey)
		g := item.getter
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(filekey); keyMatch {
				val, _ = g.Next(nil)
				found = true
				return false
			}
		}
		return true
	})
	return val, found
}

// historyBeforeTxNum searches history for a value of specified key before txNum
// second return value is true if the value is found in the history (even if it is nil)
func (dc *DomainContext) historyBeforeTxNum(key []byte, txNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	var search ctxItem
	search.startTxNum = txNum
	search.endTxNum = txNum
	var foundTxNum uint64
	var foundEndTxNum uint64
	var foundStartTxNum uint64
	var found bool
	var anyItem bool // Whether any filesItem has been looked at in the loop below
	var topState ctxItem
	dc.files.AscendGreaterOrEqual(search, func(i ctxItem) bool {
		topState = i
		return false
	})
	dc.hc.indexFiles.AscendGreaterOrEqual(search, func(item ctxItem) bool {
		anyItem = true
		offset := item.reader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		if k, _ := g.NextUncompressed(); bytes.Equal(k, key) {
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			//start := time.Now()
			n, ok := ef.Search(txNum)
			//d.stats.EfSearchTime += time.Since(start)
			if ok {
				foundTxNum = n
				foundEndTxNum = item.endTxNum
				foundStartTxNum = item.startTxNum
				found = true
				return false
			} else if item.endTxNum > txNum && item.endTxNum >= topState.endTxNum {
				return false
			}
		}
		return true
	})
	if !found {
		if anyItem {
			// If there were no changes but there were history files, the value can be obtained from value files
			var val []byte
			dc.files.DescendLessOrEqual(topState, func(item ctxItem) bool {
				if item.reader.Empty() {
					return true
				}
				offset := item.reader.Lookup(key)
				g := item.getter
				g.Reset(offset)
				if g.HasNext() {
					if k, _ := g.NextUncompressed(); bytes.Equal(k, key) {
						if dc.d.compressVals {
							val, _ = g.Next(nil)
						} else {
							val, _ = g.NextUncompressed()
						}
						return false
					}
				}
				return true
			})
			return val, true, nil
		}
		// Value not found in history files, look in the recent history
		if roTx == nil {
			return nil, false, fmt.Errorf("roTx is nil")
		}
		indexCursor, err := roTx.CursorDupSort(dc.d.indexTable)
		if err != nil {
			return nil, false, err
		}
		defer indexCursor.Close()
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], txNum)
		var foundTxNumVal []byte
		if foundTxNumVal, err = indexCursor.SeekBothRange(key, txKey[:]); err != nil {
			return nil, false, err
		}
		if foundTxNumVal != nil {
			var historyKeysCursor kv.CursorDupSort
			if historyKeysCursor, err = roTx.CursorDupSort(dc.d.indexKeysTable); err != nil {
				return nil, false, err
			}
			defer historyKeysCursor.Close()
			var vn []byte
			if vn, err = historyKeysCursor.SeekBothRange(foundTxNumVal, key); err != nil {
				return nil, false, err
			}
			valNum := binary.BigEndian.Uint64(vn[len(vn)-8:])
			if valNum == 0 {
				// This is special valNum == 0, which is empty value
				return nil, true, nil
			}
			var v []byte
			if v, err = roTx.GetOne(dc.d.historyValsTable, vn[len(vn)-8:]); err != nil {
				return nil, false, err
			}
			return v, true, nil
		}
		return nil, false, nil
	}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], foundTxNum)
	var historyItem ctxItem
	search.startTxNum = foundStartTxNum
	search.endTxNum = foundEndTxNum
	historyItem, ok := dc.hc.historyFiles.Get(search)
	if !ok {
		return nil, false, fmt.Errorf("no %s file found for [%x]", dc.d.filenameBase, key)
	}
	offset := historyItem.reader.Lookup2(txKey[:], key)
	g := historyItem.getter
	g.Reset(offset)
	if dc.d.compressVals {
		v, _ := g.Next(nil)
		return v, true, nil
	}
	v, _ := g.NextUncompressed()
	return v, true, nil
}

// GetBeforeTxNum does not always require usage of roTx. If it is possible to determine
// historical value based only on static files, roTx will not be used.
func (dc *DomainContext) GetBeforeTxNum(key []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	v, hOk, err := dc.historyBeforeTxNum(key, txNum, roTx)
	if err != nil {
		return nil, err
	}
	if hOk {
		return v, nil
	}
	if v, _, err = dc.get(key, txNum-1, roTx); err != nil {
		return nil, err
	}
	return v, nil
}
