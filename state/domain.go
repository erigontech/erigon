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
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

var (
	historyValCountKey = []byte("ValCount")
)

// filesItem corresponding to a pair of files (.dat and .idx)
type filesItem struct {
	startTxNum   uint64
	endTxNum     uint64
	decompressor *compress.Decompressor
	index        *recsplit.Index
}

func filesItemLess(i, j *filesItem) bool {
	if i.endTxNum == j.endTxNum {
		return i.startTxNum > j.startTxNum
	}
	return i.endTxNum < j.endTxNum
}

type FileType int

const (
	Values FileType = iota
	History1
	EfHistory
	NumberOfTypes
)

func (ft FileType) String() string {
	switch ft {
	case Values:
		return "values"
	case History1:
		return "history"
	case EfHistory:
		return "efhistory"
	default:
		panic(fmt.Sprintf("unknown file type: %d", ft))
	}
}

func ParseFileType(s string) (FileType, bool) {
	switch s {
	case "values":
		return Values, true
	case "history":
		return History1, true
	case "efhistory":
		return EfHistory, true
	default:
		return NumberOfTypes, false
	}
}

type DomainStats struct {
	HistoryQueries int
	EfSearchTime   time.Duration
}

func (ds *DomainStats) Accumulate(other DomainStats) {
	ds.HistoryQueries += other.HistoryQueries
	ds.EfSearchTime += other.EfSearchTime
}

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
type Domain struct {
	dir              string // Directory where static files are created
	aggregationStep  uint64
	filenameBase     string
	keysTable        string // Needs to be table with DupSort
	valsTable        string
	historyKeysTable string // Needs to be table with DupSort
	historyValsTable string
	settingsTable    string // Table containing just one record - counter of value number (keys in the historyValsTable)
	indexTable       string // Needs to be table with DupSort
	tx               kv.RwTx
	txNum            uint64
	files            [NumberOfTypes]*btree.BTreeG[*filesItem] // Static files pertaining to this domain, items are of type `filesItem`
	prefixLen        int                                      // Number of bytes in the keys that can be used for prefix iteration
	compressVals     bool
	stats            DomainStats
	defaultDc        *DomainContext
}

func NewDomain(
	dir string,
	aggregationStep uint64,
	filenameBase string,
	keysTable string,
	valsTable string,
	historyKeysTable string,
	historyValsTable string,
	settingsTable string,
	indexTable string,
	prefixLen int,
	compressVals bool,
) (*Domain, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	d := &Domain{
		dir:              dir,
		aggregationStep:  aggregationStep,
		filenameBase:     filenameBase,
		keysTable:        keysTable,
		valsTable:        valsTable,
		historyKeysTable: historyKeysTable,
		historyValsTable: historyValsTable,
		settingsTable:    settingsTable,
		indexTable:       indexTable,
		prefixLen:        prefixLen,
		compressVals:     compressVals,
	}
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		d.files[fType] = btree.NewG[*filesItem](32, filesItemLess)
	}
	d.scanStateFiles(files)
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		if err = d.openFiles(fType); err != nil {
			return nil, err
		}
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
	typeStrings := make([]string, NumberOfTypes)
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		typeStrings[fType] = fType.String()
	}
	re := regexp.MustCompile(d.filenameBase + "-(" + strings.Join(typeStrings, "|") + ").([0-9]+)-([0-9]+).(dat|idx)")
	var err error
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			if len(subs) != 0 {
				log.Warn("File ignored by doman scan, more than 5 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startTxNum, endTxNum uint64
		if startTxNum, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by domain scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endTxNum, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			log.Warn("File ignored by domain scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startTxNum > endTxNum {
			log.Warn("File ignored by domain scan, startTxNum > endTxNum", "name", name)
			continue
		}
		fType, ok := ParseFileType(subs[1])
		if !ok {
			log.Warn("File ignored by domain scan, type unknown", "type", subs[1])
		}
		var item = &filesItem{startTxNum: startTxNum * d.aggregationStep, endTxNum: endTxNum * d.aggregationStep}
		var foundI *filesItem
		d.files[fType].AscendGreaterOrEqual(&filesItem{startTxNum: endTxNum * d.aggregationStep, endTxNum: endTxNum * d.aggregationStep}, func(it *filesItem) bool {
			if it.endTxNum == endTxNum {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startTxNum > startTxNum {
			//log.Info("Load state file", "name", name, "type", fType.String(), "startTxNum", startTxNum*d.aggregationStep, "endTxNum", endTxNum*d.aggregationStep)
			d.files[fType].ReplaceOrInsert(item)
		}
	}
}

func (d *Domain) openFiles(fType FileType) error {
	var err error
	var totalKeys uint64
	d.files[fType].Ascend(func(item *filesItem) bool {
		datPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.dat", d.filenameBase, fType.String(), item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep))
		if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			return false
		}
		idxPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.idx", d.filenameBase, fType.String(), item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep))
		if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
			return false
		}
		totalKeys += item.index.KeyCount()
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *Domain) closeFiles(fType FileType) {
	d.files[fType].Ascend(func(item *filesItem) bool {
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
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		d.closeFiles(fType)
	}
}

func (d *Domain) SetTx(tx kv.RwTx) {
	d.tx = tx
}

func (d *Domain) SetTxNum(txNum uint64) {
	d.txNum = txNum
}

func (dc *DomainContext) get(key []byte, roTx kv.Tx) ([]byte, bool, error) {
	var invertedStep [8]byte
	binary.BigEndian.PutUint64(invertedStep[:], ^(dc.d.txNum / dc.d.aggregationStep))
	keyCursor, err := roTx.CursorDupSort(dc.d.keysTable)
	if err != nil {
		return nil, false, err
	}
	defer keyCursor.Close()
	foundInvStep, err := keyCursor.SeekBothRange(key, invertedStep[:])
	if err != nil {
		return nil, false, err
	}
	if foundInvStep == nil {
		v, found := dc.readFromFiles(Values, key)
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

func (dc *DomainContext) Get(key []byte, roTx kv.Tx) ([]byte, error) {
	v, _, err := dc.get(key, roTx)
	return v, err
}

func (d *Domain) update(key, original []byte) error {
	var invertedStep [8]byte
	binary.BigEndian.PutUint64(invertedStep[:], ^(d.txNum / d.aggregationStep))
	if err := d.tx.Put(d.keysTable, key, invertedStep[:]); err != nil {
		return err
	}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], d.txNum)
	historyKey := make([]byte, len(key)+8)
	copy(historyKey, key)
	if len(original) > 0 {
		val, err := d.tx.GetOne(d.settingsTable, historyValCountKey)
		if err != nil {
			return err
		}
		var valNum uint64
		if len(val) > 0 {
			valNum = binary.BigEndian.Uint64(val)
		}
		valNum++
		binary.BigEndian.PutUint64(historyKey[len(key):], valNum)
		if err = d.tx.Put(d.settingsTable, historyValCountKey, historyKey[len(key):]); err != nil {
			return err
		}
		if err = d.tx.Put(d.historyValsTable, historyKey[len(key):], original); err != nil {
			return err
		}
	}
	if err := d.tx.Put(d.historyKeysTable, txKey[:], historyKey); err != nil {
		return err
	}
	if err := d.tx.Put(d.indexTable, key, txKey[:]); err != nil {
		return err
	}
	return nil
}

func (d *Domain) Put(key, val []byte) error {
	original, _, err := d.defaultDc.get(key, d.tx)
	if err != nil {
		return err
	}
	if bytes.Equal(original, val) {
		return nil
	}
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `original`` slice is invalidated
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

func (d *Domain) Delete(key []byte) error {
	original, found, err := d.defaultDc.get(key, d.tx)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	// This call to update needs to happen before d.tx.Delete() later, because otherwise the content of `original`` slice is invalidated
	if err = d.update(key, original); err != nil {
		return err
	}
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	if err = d.tx.Delete(d.valsTable, keySuffix, nil); err != nil {
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
	t        CursorType // Whether this item represents state file or DB record, or tree
	reverse  bool
	endTxNum uint64
	key, val []byte
	dg, dg2  *compress.Getter
	c        kv.CursorDupSort
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
	startTxNum uint64
	endTxNum   uint64
	getter     *compress.Getter
	reader     *recsplit.IndexReader
}

func ctxItemLess(i, j *ctxItem) bool {
	if i.endTxNum == j.endTxNum {
		return i.startTxNum > j.startTxNum
	}
	return i.endTxNum < j.endTxNum
}

// DomainContext allows accesing the same domain from multiple go-routines
type DomainContext struct {
	d     *Domain
	files [NumberOfTypes]*btree.BTreeG[*ctxItem]
}

func (d *Domain) MakeContext() *DomainContext {
	dc := &DomainContext{d: d}
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		bt := btree.NewG[*ctxItem](32, ctxItemLess)
		dc.files[fType] = bt
		d.files[fType].Ascend(func(item *filesItem) bool {
			bt.ReplaceOrInsert(&ctxItem{
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				getter:     item.decompressor.MakeGetter(),
				reader:     recsplit.NewIndexReader(item.index),
			})
			return true
		})
	}
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
	dc.files[Values].Ascend(func(item *ctxItem) bool {
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
	valuesPath   string
	valuesComp   *compress.Compressor
	valuesCount  int
	historyPath  string
	historyComp  *compress.Compressor
	historyCount int
	indexBitmaps map[string]*roaring64.Bitmap
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
func (d *Domain) collate(step, txFrom, txTo uint64, roTx kv.Tx) (Collation, error) {
	var valuesComp, historyComp *compress.Compressor
	var err error
	closeComp := true
	defer func() {
		if closeComp {
			if valuesComp != nil {
				valuesComp.Close()
			}
			if historyComp != nil {
				historyComp.Close()
			}
		}
	}()
	valuesPath := filepath.Join(d.dir, fmt.Sprintf("%s-values.%d-%d.dat", d.filenameBase, step, step+1))
	if valuesComp, err = compress.NewCompressor(context.Background(), "collate values", valuesPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
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
	historyPath := filepath.Join(d.dir, fmt.Sprintf("%s-history.%d-%d.dat", d.filenameBase, step, step+1))
	if historyComp, err = compress.NewCompressor(context.Background(), "collate history", historyPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
		return Collation{}, fmt.Errorf("create %s history compressor: %w", d.filenameBase, err)
	}
	historyKeysCursor, err := roTx.CursorDupSort(d.historyKeysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s history cursor: %w", d.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	indexBitmaps := map[string]*roaring64.Bitmap{}
	historyCount := 0
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var val []byte
	var historyKey []byte
	for k, v, err = historyKeysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		historyKey = append(append(historyKey[:0], k...), v[:len(v)-8]...)
		if err = historyComp.AddUncompressedWord(historyKey); err != nil {
			return Collation{}, fmt.Errorf("add %s history key [%x]: %w", d.filenameBase, k, err)
		}
		valNum := binary.BigEndian.Uint64(v[len(v)-8:])
		if valNum == 0 {
			val = nil
		} else {
			if val, err = roTx.GetOne(d.historyValsTable, v[len(v)-8:]); err != nil {
				return Collation{}, fmt.Errorf("get %s history val [%x]=>%d: %w", d.filenameBase, k, valNum, err)
			}
		}
		if err = historyComp.AddUncompressedWord(val); err != nil {
			return Collation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", d.filenameBase, k, val, err)
		}
		historyCount++
		var bitmap *roaring64.Bitmap
		var ok bool
		if bitmap, ok = indexBitmaps[string(v[:len(v)-8])]; !ok {
			bitmap = roaring64.New()
			indexBitmaps[string(v[:len(v)-8])] = bitmap
		}
		bitmap.Add(txNum)
	}
	if err != nil {
		return Collation{}, fmt.Errorf("iterate over %s history cursor: %w", d.filenameBase, err)
	}
	closeComp = false
	return Collation{
		valuesPath:   valuesPath,
		valuesComp:   valuesComp,
		valuesCount:  valuesCount,
		historyPath:  historyPath,
		historyComp:  historyComp,
		historyCount: historyCount,
		indexBitmaps: indexBitmaps,
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
func (d *Domain) buildFiles(step uint64, collation Collation) (StaticFiles, error) {
	valuesComp := collation.valuesComp
	historyComp := collation.historyComp
	var valuesDecomp, historyDecomp, efHistoryDecomp *compress.Decompressor
	var valuesIdx, historyIdx, efHistoryIdx *recsplit.Index
	var efHistoryComp *compress.Compressor
	closeComp := true
	defer func() {
		if closeComp {
			if valuesComp != nil {
				valuesComp.Close()
			}
			if valuesDecomp != nil {
				valuesDecomp.Close()
			}
			if valuesIdx != nil {
				valuesIdx.Close()
			}
			if historyComp != nil {
				historyComp.Close()
			}
			if historyDecomp != nil {
				historyDecomp.Close()
			}
			if historyIdx != nil {
				historyIdx.Close()
			}
			if efHistoryComp != nil {
				efHistoryComp.Close()
			}
			if efHistoryDecomp != nil {
				efHistoryDecomp.Close()
			}
			if efHistoryIdx != nil {
				efHistoryIdx.Close()
			}
		}
	}()
	valuesIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s-values.%d-%d.idx", d.filenameBase, step, step+1))
	var err error
	if err = valuesComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s values: %w", d.filenameBase, err)
	}
	valuesComp.Close()
	valuesComp = nil
	if valuesDecomp, err = compress.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}
	if valuesIdx, err = buildIndex(valuesDecomp, valuesIdxPath, d.dir, collation.valuesCount, false /* values */); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
	}
	historyIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s-history.%d-%d.idx", d.filenameBase, step, step+1))
	if err = historyComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s history: %w", d.filenameBase, err)
	}
	historyComp.Close()
	historyComp = nil
	if historyDecomp, err = compress.NewDecompressor(collation.historyPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s history decompressor: %w", d.filenameBase, err)
	}
	if historyIdx, err = buildIndex(historyDecomp, historyIdxPath, d.dir, collation.historyCount, true /* values */); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s history idx: %w", d.filenameBase, err)
	}
	// Build history ef
	efHistoryPath := filepath.Join(d.dir, fmt.Sprintf("%s-efhistory.%d-%d.dat", d.filenameBase, step, step+1))
	efHistoryComp, err = compress.NewCompressor(context.Background(), "ef history", efHistoryPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug)
	if err != nil {
		return StaticFiles{}, fmt.Errorf("create %s ef history compressor: %w", d.filenameBase, err)
	}
	var buf []byte
	keys := make([]string, 0, len(collation.indexBitmaps))
	for key := range collation.indexBitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err = efHistoryComp.AddUncompressedWord([]byte(key)); err != nil {
			return StaticFiles{}, fmt.Errorf("add %s ef history key [%x]: %w", d.filenameBase, key, err)
		}
		bitmap := collation.indexBitmaps[key]
		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			ef.AddOffset(it.Next())
		}
		ef.Build()
		buf = ef.AppendBytes(buf[:0])
		if err = efHistoryComp.AddUncompressedWord(buf); err != nil {
			return StaticFiles{}, fmt.Errorf("add %s ef history val: %w", d.filenameBase, err)
		}
	}
	if err = efHistoryComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s ef history: %w", d.filenameBase, err)
	}
	efHistoryComp.Close()
	efHistoryComp = nil
	if efHistoryDecomp, err = compress.NewDecompressor(efHistoryPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s ef history decompressor: %w", d.filenameBase, err)
	}
	efHistoryIdxPath := filepath.Join(d.dir, fmt.Sprintf("%s-efhistory.%d-%d.idx", d.filenameBase, step, step+1))
	if efHistoryIdx, err = buildIndex(efHistoryDecomp, efHistoryIdxPath, d.dir, len(keys), false /* values */); err != nil {
		return StaticFiles{}, fmt.Errorf("build %s ef history idx: %w", d.filenameBase, err)
	}
	closeComp = false
	return StaticFiles{
		valuesDecomp:    valuesDecomp,
		valuesIdx:       valuesIdx,
		historyDecomp:   historyDecomp,
		historyIdx:      historyIdx,
		efHistoryDecomp: efHistoryDecomp,
		efHistoryIdx:    efHistoryIdx,
	}, nil
}

func buildIndex(d *compress.Decompressor, idxPath, dir string, count int, values bool) (*recsplit.Index, error) {
	var rs *recsplit.RecSplit
	var err error
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     dir,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: idxPath,
	}); err != nil {
		return nil, fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
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
	d.files[Values].ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.valuesDecomp,
		index:        sf.valuesIdx,
	})
	d.files[History1].ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.historyDecomp,
		index:        sf.historyIdx,
	})
	d.files[EfHistory].ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.efHistoryDecomp,
		index:        sf.efHistoryIdx,
	})
}

// [txFrom; txTo)
func (d *Domain) prune(step uint64, txFrom, txTo uint64) error {
	// It is important to clean up tables in a specific order
	// First keysTable, because it is the first one access in the `get` function, i.e. if the record is deleted from there, other tables will not be accessed
	keysCursor, err := d.tx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("%s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()
	var k, v []byte
	for k, v, err = keysCursor.First(); err == nil && k != nil; k, v, err = keysCursor.Next() {
		s := ^binary.BigEndian.Uint64(v)
		if s == step {
			if err = keysCursor.DeleteCurrent(); err != nil {
				return fmt.Errorf("clean up %s for [%x]=>[%x]: %w", d.filenameBase, k, v, err)
			}
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
			if err = valsCursor.DeleteCurrent(); err != nil {
				return fmt.Errorf("clean up %s for [%x]: %w", d.filenameBase, k, err)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s vals: %w", d.filenameBase, err)
	}
	historyKeysCursor, err := d.tx.RwCursorDupSort(d.historyKeysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", d.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	for k, v, err = historyKeysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		if err = d.tx.Delete(d.historyValsTable, v[len(v)-8:], nil); err != nil {
			return err
		}
		if err = d.tx.Delete(d.indexTable, v[:len(v)-8], k); err != nil {
			return err
		}
		// This DeleteCurrent needs to the the last in the loop iteration, because it invalidates k and v
		if err = historyKeysCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s history keys: %w", d.filenameBase, err)
	}
	return nil
}

func (dc *DomainContext) readFromFiles(fType FileType, filekey []byte) ([]byte, bool) {
	var val []byte
	var found bool
	dc.files[fType].Descend(func(item *ctxItem) bool {
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
	var topState *ctxItem
	dc.files[Values].AscendGreaterOrEqual(&search, func(i *ctxItem) bool {
		topState = i
		return false
	})
	dc.files[EfHistory].AscendGreaterOrEqual(&search, func(item *ctxItem) bool {
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
			dc.files[Values].DescendLessOrEqual(topState, func(item *ctxItem) bool {
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
			if historyKeysCursor, err = roTx.CursorDupSort(dc.d.historyKeysTable); err != nil {
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
	var historyItem *ctxItem
	search.startTxNum = foundStartTxNum
	search.endTxNum = foundEndTxNum
	historyItem, ok := dc.files[History1].Get(&search)
	if !ok || historyItem == nil {
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
	if v, _, err = dc.get(key, roTx); err != nil {
		return nil, err
	}
	return v, nil
}
