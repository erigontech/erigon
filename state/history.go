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
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	btree2 "github.com/tidwall/btree"
	atomic2 "go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type History struct {
	*InvertedIndex

	// Files:
	//  .v - list of values
	//  .vi - txNum+key -> offset in .v
	files *btree2.BTreeG[*filesItem] // thread-safe, but maybe need 1 RWLock for all trees in AggregatorV3

	// roFiles derivative from field `file`, but without garbage (canDelete=true, overlaps, etc...)
	// MakeContext() using this field in zero-copy way
	roFiles atomic2.Pointer[[]ctxItem]

	historyValsTable        string // key1+key2+txnNum -> oldValue , stores values BEFORE change
	settingsTable           string
	compressWorkers         int
	compressVals            bool
	integrityFileExtensions []string

	wal *historyWAL
}

func NewHistory(
	dir, tmpdir string,
	aggregationStep uint64,
	filenameBase string,
	indexKeysTable string,
	indexTable string,
	historyValsTable string,
	settingsTable string,
	compressVals bool,
	integrityFileExtensions []string,
) (*History, error) {
	h := History{
		files:                   btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		roFiles:                 *atomic2.NewPointer(&[]ctxItem{}),
		historyValsTable:        historyValsTable,
		settingsTable:           settingsTable,
		compressVals:            compressVals,
		compressWorkers:         1,
		integrityFileExtensions: integrityFileExtensions,
	}

	var err error
	h.InvertedIndex, err = NewInvertedIndex(dir, tmpdir, aggregationStep, filenameBase, indexKeysTable, indexTable, true, append(slices.Clone(h.integrityFileExtensions), "v"))
	if err != nil {
		return nil, fmt.Errorf("NewHistory: %s, %w", filenameBase, err)
	}

	return &h, nil
}

// OpenList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (h *History) OpenList(fNames []string) error {
	if err := h.InvertedIndex.OpenList(fNames); err != nil {
		return err
	}
	return h.openList(fNames)

}
func (h *History) openList(fNames []string) error {
	h.closeWhatNotInList(fNames)
	_ = h.scanStateFiles(fNames)
	if err := h.openFiles(); err != nil {
		return fmt.Errorf("History.OpenList: %s, %w", h.filenameBase, err)
	}
	return nil
}

func (h *History) OpenFolder() error {
	files, err := h.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return h.OpenList(files)
}

// scanStateFiles
// returns `uselessFiles` where file "is useless" means: it's subset of frozen file. such files can be safely deleted. subset of non-frozen file may be useful
func (h *History) scanStateFiles(fNames []string) (uselessFiles []*filesItem) {
	re := regexp.MustCompile("^" + h.filenameBase + ".([0-9]+)-([0-9]+).v$")
	var err error
Loop:
	for _, name := range fNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 3 {
			if len(subs) != 0 {
				log.Warn("File ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			log.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*h.aggregationStep, endStep*h.aggregationStep
		frozen := endStep-startStep == StepsInBiggestFile

		for _, ext := range h.integrityFileExtensions {
			requiredFile := fmt.Sprintf("%s.%d-%d.%s", h.filenameBase, startStep, endStep, ext)
			if !dir.FileExist(filepath.Join(h.dir, requiredFile)) {
				log.Debug(fmt.Sprintf("[snapshots] skip %s because %s doesn't exists", name, requiredFile))
				continue Loop
			}
		}

		var newFile = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum, frozen: frozen}
		if _, has := h.files.Get(newFile); has {
			continue
		}

		addNewFile := true
		var subSets []*filesItem
		h.files.Walk(func(items []*filesItem) bool {
			for _, item := range items {
				if item.isSubsetOf(newFile) {
					subSets = append(subSets, item)
					continue
				}

				if newFile.isSubsetOf(item) {
					if item.frozen {
						addNewFile = false
						uselessFiles = append(uselessFiles, newFile)
					}
					continue
				}
			}
			return true
		})
		//for _, subSet := range subSets {
		//	h.files.Delete(subSet)
		//}
		if addNewFile {
			h.files.Set(newFile)
		}
	}
	return uselessFiles
}

func (h *History) openFiles() error {
	var totalKeys uint64
	var err error
	invalidFileItems := make([]*filesItem, 0)
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				continue
			}
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, fromStep, toStep))
			if !dir.FileExist(datPath) {
				invalidFileItems = append(invalidFileItems, item)
				continue
			}
			if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				log.Debug("Hisrory.openFiles: %w, %s", err, datPath)
				return false
			}

			if item.index != nil {
				continue
			}
			idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep))
			if dir.FileExist(idxPath) {
				if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
					log.Debug(fmt.Errorf("Hisrory.openFiles: %w, %s", err, idxPath).Error())
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
		h.files.Delete(item)
	}

	h.reCalcRoFiles()
	return nil
}

func (h *History) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	h.files.Walk(func(items []*filesItem) bool {
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
			if err := item.decompressor.Close(); err != nil {
				log.Trace("close", "err", err, "file", item.index.FileName())
			}
			item.decompressor = nil
		}
		if item.index != nil {
			if err := item.index.Close(); err != nil {
				log.Trace("close", "err", err, "file", item.index.FileName())
			}
			item.index = nil
		}
		h.files.Delete(item)
	}
}

func (h *History) Close() {
	h.InvertedIndex.Close()
	h.closeWhatNotInList([]string{})
	h.reCalcRoFiles()
}

func (h *History) Files() (res []string) {
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				res = append(res, item.decompressor.FileName())
			}
		}
		return true
	})
	res = append(res, h.InvertedIndex.Files()...)
	return res
}

func (h *History) missedIdxFiles() (l []*filesItem) {
	h.files.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			if !dir.FileExist(filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep))) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (h *History) BuildOptionalMissedIndices(ctx context.Context) (err error) {
	return h.localityIndex.BuildMissedIndices(ctx, h.InvertedIndex)
}

func (h *History) buildVi(ctx context.Context, item *filesItem) (err error) {
	search := &filesItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum}
	iiItem, ok := h.InvertedIndex.files.Get(search)
	if !ok {
		return nil
	}

	fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
	fName := fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(h.dir, fName)
	log.Info("[snapshots] build idx", "file", fName)
	count, err := iterateForVi(item, iiItem, h.compressVals, func(v []byte) error { return nil })
	if err != nil {
		return err
	}
	return buildVi(ctx, item, iiItem, idxPath, h.tmpdir, count, false /* values */, h.compressVals)
}

func (h *History) BuildMissedIndices(ctx context.Context, g *errgroup.Group) {
	h.InvertedIndex.BuildMissedIndices(ctx, g)
	missedFiles := h.missedIdxFiles()
	for _, item := range missedFiles {
		item := item
		g.Go(func() error { return h.buildVi(ctx, item) })
	}
}

func iterateForVi(historyItem, iiItem *filesItem, compressVals bool, f func(v []byte) error) (count int, err error) {
	var cp CursorHeap
	heap.Init(&cp)
	g := iiItem.decompressor.MakeGetter()
	g.Reset(0)
	if g.HasNext() {
		g2 := historyItem.decompressor.MakeGetter()
		key, _ := g.NextUncompressed()
		val, _ := g.NextUncompressed()
		heap.Push(&cp, &CursorItem{
			t:        FILE_CURSOR,
			dg:       g,
			dg2:      g2,
			key:      key,
			val:      val,
			endTxNum: iiItem.endTxNum,
			reverse:  false,
		})
	}

	// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
	// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
	// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
	// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
	// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
	var valBuf []byte
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		// Advance all the items that have this key (including the top)
		//var mergeOnce bool
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			keysCount := eliasfano32.Count(ci1.val)
			for i := uint64(0); i < keysCount; i++ {
				if compressVals {
					valBuf, _ = ci1.dg2.Next(valBuf[:0])
				} else {
					valBuf, _ = ci1.dg2.NextUncompressed()
				}
				if err = f(valBuf); err != nil {
					return count, err
				}
			}
			count += int(keysCount)
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.NextUncompressed()
				ci1.val, _ = ci1.dg.NextUncompressed()
				heap.Fix(&cp, 0)
			} else {
				heap.Remove(&cp, 0)
			}
		}
	}
	return count, nil
}

func buildVi(ctx context.Context, historyItem, iiItem *filesItem, historyIdxPath, tmpdir string, count int, values, compressVals bool) error {
	_, fName := filepath.Split(historyIdxPath)
	log.Debug("[snapshots] build idx", "file", fName)
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:    count,
		Enums:       false,
		BucketSize:  2000,
		LeafSize:    8,
		TmpDir:      tmpdir,
		IndexFile:   historyIdxPath,
		EtlBufLimit: etl.BufferOptimalSize / 2,
	})
	if err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	rs.LogLvl(log.LvlTrace)
	defer rs.Close()
	var historyKey []byte
	var txKey [8]byte
	var valOffset uint64

	defer iiItem.decompressor.EnableMadvNormal().DisableReadAhead()
	defer historyItem.decompressor.EnableMadvNormal().DisableReadAhead()

	g := iiItem.decompressor.MakeGetter()
	g2 := historyItem.decompressor.MakeGetter()
	var keyBuf, valBuf []byte
	for {
		g.Reset(0)
		g2.Reset(0)
		valOffset = 0
		for g.HasNext() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			keyBuf, _ = g.NextUncompressed()
			valBuf, _ = g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(valBuf)
			efIt := ef.Iterator()
			for efIt.HasNext() {
				txNum, _ := efIt.Next()
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), keyBuf...)
				if err = rs.AddKey(historyKey, valOffset); err != nil {
					return err
				}
				if compressVals {
					valOffset = g2.Skip()
				} else {
					valOffset = g2.SkipUncompressed()
				}
			}
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("build %s idx: %w", historyIdxPath, err)
			}
		} else {
			break
		}
	}
	return nil
}

func (h *History) AddPrevValue(key1, key2, original []byte) (err error) {
	return h.wal.addPrevValue(key1, key2, original)
}

func (h *History) DiscardHistory() {
	h.InvertedIndex.StartWrites()
	h.wal = h.newWriter(h.tmpdir, false, true)
}
func (h *History) StartWrites() {
	h.InvertedIndex.StartWrites()
	h.wal = h.newWriter(h.tmpdir, true, false)
}
func (h *History) FinishWrites() {
	h.InvertedIndex.FinishWrites()
	h.wal.close()
	h.wal = nil
}

func (h *History) Rotate() historyFlusher {
	if h.wal != nil {
		h.wal.historyValsFlushing, h.wal.historyVals = h.wal.historyVals, h.wal.historyValsFlushing
		h.wal.autoIncrementFlush = h.wal.autoIncrement
	}
	return historyFlusher{h.wal, h.InvertedIndex.Rotate()}
}

type historyFlusher struct {
	h *historyWAL
	i *invertedIndexWAL
}

func (f historyFlusher) Flush(ctx context.Context, tx kv.RwTx) error {
	if err := f.i.Flush(ctx, tx); err != nil {
		return err
	}
	if err := f.h.flush(ctx, tx); err != nil {
		return err
	}
	return nil
}

type historyWAL struct {
	h                   *History
	historyVals         *etl.Collector
	historyValsFlushing *etl.Collector
	tmpdir              string
	autoIncrementBuf    []byte
	historyKey          []byte
	autoIncrement       uint64
	autoIncrementFlush  uint64
	buffered            bool
	discard             bool
}

func (h *historyWAL) close() {
	if h == nil { // allow dobule-close
		return
	}
	if h.historyVals != nil {
		h.historyVals.Close()
	}
}

func (h *History) newWriter(tmpdir string, buffered, discard bool) *historyWAL {
	w := &historyWAL{h: h,
		tmpdir:   tmpdir,
		buffered: buffered,
		discard:  discard,

		autoIncrementBuf: make([]byte, 8),
		historyKey:       make([]byte, 0, 128),
	}
	if buffered {
		w.historyVals = etl.NewCollector(h.historyValsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRam))
		w.historyValsFlushing = etl.NewCollector(h.historyValsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRam))
		w.historyVals.LogLvl(log.LvlTrace)
		w.historyValsFlushing.LogLvl(log.LvlTrace)
	}

	val, err := h.tx.GetOne(h.settingsTable, historyValCountKey)
	if err != nil {
		panic(err)
		//return err
	}
	var valNum uint64
	if len(val) > 0 {
		valNum = binary.BigEndian.Uint64(val)
	}
	w.autoIncrement = valNum
	return w
}

func (h *historyWAL) flush(ctx context.Context, tx kv.RwTx) error {
	if h.discard {
		return nil
	}
	binary.BigEndian.PutUint64(h.autoIncrementBuf, h.autoIncrementFlush)
	if err := tx.Put(h.h.settingsTable, historyValCountKey, h.autoIncrementBuf); err != nil {
		return err
	}
	if err := h.historyValsFlushing.Load(tx, h.h.historyValsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	return nil
}

func (h *historyWAL) addPrevValue(key1, key2, original []byte) error {
	if h.discard {
		return nil
	}

	/*
		lk := len(key1) + len(key2)
		historyKey := make([]byte, lk+8)
		copy(historyKey, key1)
		if len(key2) > 0 {
			copy(historyKey[len(key1):], key2)
		}
		if len(original) > 0 {
			val, err := h.h.tx.GetOne(h.h.settingsTable, historyValCountKey)
			if err != nil {
				return err
			}
			var valNum uint64
			if len(val) > 0 {
				valNum = binary.BigEndian.Uint64(val)
			}
			valNum++
			binary.BigEndian.PutUint64(historyKey[lk:], valNum)
			if err = h.h.tx.Put(h.h.settingsTable, historyValCountKey, historyKey[lk:]); err != nil {
				return err
			}
			if err = h.h.tx.Put(h.h.historyValsTable, historyKey[lk:], original); err != nil {
				return err
			}
		}
	*/

	lk := len(key1) + len(key2)
	historyKey := h.historyKey[:lk+8]
	copy(historyKey, key1)
	if len(key2) > 0 {
		copy(historyKey[len(key1):], key2)
	}
	if len(original) > 0 {
		h.autoIncrement++
		binary.BigEndian.PutUint64(historyKey[lk:], h.autoIncrement)
		//if err := h.h.tx.Put(h.h.settingsTable, historyValCountKey, historyKey[lk:]); err != nil {
		//	return err
		//}

		if h.buffered {
			if err := h.historyVals.Collect(historyKey[lk:], original); err != nil {
				return err
			}
		} else {
			if err := h.h.tx.Put(h.h.historyValsTable, historyKey[lk:], original); err != nil {
				return err
			}
		}
	} else {
		binary.BigEndian.PutUint64(historyKey[lk:], 0)
	}

	if err := h.h.InvertedIndex.add(historyKey, historyKey[:lk]); err != nil {
		return err
	}
	return nil
}

type HistoryCollation struct {
	historyComp  *compress.Compressor
	indexBitmaps map[string]*roaring64.Bitmap
	historyPath  string
	historyCount int
}

func (c HistoryCollation) Close() {
	if c.historyComp != nil {
		c.historyComp.Close()
	}
	for _, b := range c.indexBitmaps {
		bitmapdb.ReturnToPool64(b)
	}
}

func (h *History) collate(step, txFrom, txTo uint64, roTx kv.Tx, logEvery *time.Ticker) (HistoryCollation, error) {
	var historyComp *compress.Compressor
	var err error
	closeComp := true
	defer func() {
		if closeComp {
			if historyComp != nil {
				historyComp.Close()
			}
		}
	}()
	historyPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, step, step+1))
	if historyComp, err = compress.NewCompressor(context.Background(), "collate history", historyPath, h.tmpdir, compress.MinPatternScore, h.compressWorkers, log.LvlTrace); err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history compressor: %w", h.filenameBase, err)
	}
	keysCursor, err := roTx.CursorDupSort(h.indexKeysTable)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer keysCursor.Close()
	indexBitmaps := map[string]*roaring64.Bitmap{}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var val []byte
	var k, v []byte
	for k, v, err = keysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = keysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		var bitmap *roaring64.Bitmap
		var ok bool
		if bitmap, ok = indexBitmaps[string(v[:len(v)-8])]; !ok {
			bitmap = bitmapdb.NewBitmap64()
			indexBitmaps[string(v[:len(v)-8])] = bitmap
		}
		bitmap.Add(txNum)
		select {
		case <-logEvery.C:
			log.Info("[snapshots] collate history", "name", h.filenameBase, "range", fmt.Sprintf("%.2f-%.2f", float64(txNum)/float64(h.aggregationStep), float64(txTo)/float64(h.aggregationStep)))
			bitmap.RunOptimize()
		default:
		}
	}
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("iterate over %s history cursor: %w", h.filenameBase, err)
	}
	keys := make([]string, 0, len(indexBitmaps))
	for key := range indexBitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	historyCount := 0
	for _, key := range keys {
		bitmap := indexBitmaps[key]
		it := bitmap.Iterator()
		for it.HasNext() {
			txNum := it.Next()
			binary.BigEndian.PutUint64(txKey[:], txNum)
			v, err := keysCursor.SeekBothRange(txKey[:], []byte(key))
			if err != nil {
				return HistoryCollation{}, err
			}
			if !bytes.HasPrefix(v, []byte(key)) {
				continue
			}
			valNum := binary.BigEndian.Uint64(v[len(v)-8:])
			if valNum == 0 {
				val = nil
			} else {
				if val, err = roTx.GetOne(h.historyValsTable, v[len(v)-8:]); err != nil {
					return HistoryCollation{}, fmt.Errorf("get %s history val [%x]=>%d: %w", h.filenameBase, k, valNum, err)
				}
			}
			if err = historyComp.AddUncompressedWord(val); err != nil {
				return HistoryCollation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", h.filenameBase, k, val, err)
			}
			historyCount++
		}
	}
	closeComp = false
	return HistoryCollation{
		historyPath:  historyPath,
		historyComp:  historyComp,
		historyCount: historyCount,
		indexBitmaps: indexBitmaps,
	}, nil
}

type HistoryFiles struct {
	historyDecomp   *compress.Decompressor
	historyIdx      *recsplit.Index
	efHistoryDecomp *compress.Decompressor
	efHistoryIdx    *recsplit.Index
}

func (sf HistoryFiles) Close() {
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
func (h *History) reCalcRoFiles() {
	roFiles := make([]ctxItem, 0, h.files.Len())
	var prevStart uint64
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.canDelete.Load() {
				continue
			}
			//if item.startTxNum > h.endTxNumMinimax() {
			//	continue
			//}
			// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
			// see super-set file, just drop sub-set files from list
			if item.startTxNum < prevStart {
				for len(roFiles) > 0 {
					if roFiles[len(roFiles)-1].startTxNum < item.startTxNum {
						break
					}
					roFiles[len(roFiles)-1].src = nil
					roFiles = roFiles[:len(roFiles)-1]
				}
			}

			roFiles = append(roFiles, ctxItem{
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				//getter:     item.decompressor.MakeGetter(),
				//reader:     recsplit.NewIndexReader(item.index),

				i:   len(roFiles),
				src: item,
			})
		}
		return true
	})
	if roFiles == nil {
		roFiles = []ctxItem{}
	}
	h.roFiles.Store(&roFiles)
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (h *History) buildFiles(ctx context.Context, step uint64, collation HistoryCollation) (HistoryFiles, error) {
	historyComp := collation.historyComp
	var historyDecomp, efHistoryDecomp *compress.Decompressor
	var historyIdx, efHistoryIdx *recsplit.Index
	var efHistoryComp *compress.Compressor
	var rs *recsplit.RecSplit
	closeComp := true
	defer func() {
		if closeComp {
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
			if rs != nil {
				rs.Close()
			}
		}
	}()
	historyIdxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, step, step+1))
	if err := historyComp.Compress(); err != nil {
		return HistoryFiles{}, fmt.Errorf("compress %s history: %w", h.filenameBase, err)
	}
	historyComp.Close()
	historyComp = nil
	var err error
	if historyDecomp, err = compress.NewDecompressor(collation.historyPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s history decompressor: %w", h.filenameBase, err)
	}
	// Build history ef
	efHistoryPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.ef", h.filenameBase, step, step+1))
	efHistoryComp, err = compress.NewCompressor(ctx, "ef history", efHistoryPath, h.tmpdir, compress.MinPatternScore, h.compressWorkers, log.LvlTrace)
	if err != nil {
		return HistoryFiles{}, fmt.Errorf("create %s ef history compressor: %w", h.filenameBase, err)
	}
	var buf []byte
	keys := make([]string, 0, len(collation.indexBitmaps))
	for key := range collation.indexBitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err = efHistoryComp.AddUncompressedWord([]byte(key)); err != nil {
			return HistoryFiles{}, fmt.Errorf("add %s ef history key [%x]: %w", h.InvertedIndex.filenameBase, key, err)
		}
		bitmap := collation.indexBitmaps[key]
		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			txNum := it.Next()
			ef.AddOffset(txNum)
		}
		ef.Build()
		buf = ef.AppendBytes(buf[:0])
		if err = efHistoryComp.AddUncompressedWord(buf); err != nil {
			return HistoryFiles{}, fmt.Errorf("add %s ef history val: %w", h.filenameBase, err)
		}
	}
	if err = efHistoryComp.Compress(); err != nil {
		return HistoryFiles{}, fmt.Errorf("compress %s ef history: %w", h.filenameBase, err)
	}
	efHistoryComp.Close()
	efHistoryComp = nil
	if efHistoryDecomp, err = compress.NewDecompressor(efHistoryPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s ef history decompressor: %w", h.filenameBase, err)
	}
	efHistoryIdxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.efi", h.filenameBase, step, step+1))
	if efHistoryIdx, err = buildIndexThenOpen(ctx, efHistoryDecomp, efHistoryIdxPath, h.tmpdir, len(keys), false /* values */); err != nil {
		return HistoryFiles{}, fmt.Errorf("build %s ef history idx: %w", h.filenameBase, err)
	}
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   collation.historyCount,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.tmpdir,
		IndexFile:  historyIdxPath,
	}); err != nil {
		return HistoryFiles{}, fmt.Errorf("create recsplit: %w", err)
	}
	rs.LogLvl(log.LvlTrace)
	var historyKey []byte
	var txKey [8]byte
	var valOffset uint64
	g := historyDecomp.MakeGetter()
	for {
		g.Reset(0)
		valOffset = 0
		for _, key := range keys {
			bitmap := collation.indexBitmaps[key]
			it := bitmap.Iterator()
			for it.HasNext() {
				txNum := it.Next()
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), key...)
				if err = rs.AddKey(historyKey, valOffset); err != nil {
					return HistoryFiles{}, fmt.Errorf("add %s history idx [%x]: %w", h.filenameBase, historyKey, err)
				}
				valOffset = g.Skip()
			}
		}
		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return HistoryFiles{}, fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	rs.Close()
	rs = nil
	if historyIdx, err = recsplit.OpenIndex(historyIdxPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open idx: %w", err)
	}
	closeComp = false
	return HistoryFiles{
		historyDecomp:   historyDecomp,
		historyIdx:      historyIdx,
		efHistoryDecomp: efHistoryDecomp,
		efHistoryIdx:    efHistoryIdx,
	}, nil
}

func (h *History) integrateFiles(sf HistoryFiles, txNumFrom, txNumTo uint64) {
	h.InvertedIndex.integrateFiles(InvertedFiles{
		decomp: sf.efHistoryDecomp,
		index:  sf.efHistoryIdx,
	}, txNumFrom, txNumTo)
	h.files.Set(&filesItem{
		frozen:       (txNumTo-txNumFrom)/h.aggregationStep == StepsInBiggestFile,
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.historyDecomp,
		index:        sf.historyIdx,
	})
	h.reCalcRoFiles()
}

func (h *History) warmup(ctx context.Context, txFrom, limit uint64, tx kv.Tx) error {
	historyKeysCursor, err := tx.CursorDupSort(h.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	idxC, err := tx.CursorDupSort(h.indexTable)
	if err != nil {
		return err
	}
	defer idxC.Close()
	valsC, err := tx.Cursor(h.historyValsTable)
	if err != nil {
		return err
	}
	defer valsC.Close()
	k, v, err := historyKeysCursor.Seek(txKey[:])
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	txFrom = binary.BigEndian.Uint64(k)
	txTo := txFrom + h.aggregationStep
	if limit != math.MaxUint64 && limit != 0 {
		txTo = txFrom + limit
	}
	for ; err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		_, _, _ = valsC.Seek(v[len(v)-8:])
		_, _ = idxC.SeekBothRange(v[:len(v)-8], k)
	}
	if err != nil {
		return fmt.Errorf("iterate over %s history keys: %w", h.filenameBase, err)
	}

	return nil
}

func (h *History) prune(ctx context.Context, txFrom, txTo, limit uint64, logEvery *time.Ticker) error {
	historyKeysCursor, err := h.tx.RwCursorDupSort(h.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	k, v, err := historyKeysCursor.Seek(txKey[:])
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	txFrom = binary.BigEndian.Uint64(k)
	if limit != math.MaxUint64 && limit != 0 {
		txTo = cmp.Min(txTo, txFrom+limit)
	}
	if txFrom >= txTo {
		return nil
	}

	valsC, err := h.tx.RwCursor(h.historyValsTable)
	if err != nil {
		return err
	}
	defer valsC.Close()
	idxC, err := h.tx.RwCursorDupSort(h.indexTable)
	if err != nil {
		return err
	}
	defer idxC.Close()

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	for ; err == nil && k != nil; k, v, err = historyKeysCursor.NextNoDup() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		for ; err == nil && k != nil; k, v, err = historyKeysCursor.NextDup() {
			if err = valsC.Delete(v[len(v)-8:]); err != nil {
				return err
			}

			if err = idxC.DeleteExact(v[:len(v)-8], k); err != nil {
				return err
			}
			//for vv, err := idxC.SeekBothRange(v[:len(v)-8], k); vv != nil; _, vv, err = idxC.NextDup() {
			//	if err != nil {
			//		return err
			//	}
			//	if binary.BigEndian.Uint64(vv) >= txTo {
			//		break
			//	}
			//	if err = idxC.DeleteCurrent(); err != nil {
			//		return err
			//	}
			//}
		}

		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if err = historyKeysCursor.DeleteCurrentDuplicates(); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-logEvery.C:
			log.Info("[snapshots] prune history", "name", h.filenameBase, "range", fmt.Sprintf("%.2f-%.2f", float64(txNum)/float64(h.aggregationStep), float64(txTo)/float64(h.aggregationStep)))
		default:
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s history keys: %w", h.filenameBase, err)
	}
	return nil
}

func (h *History) pruneF(txFrom, txTo uint64, f func(txNum uint64, k, v []byte) error) error {
	historyKeysCursor, err := h.tx.RwCursorDupSort(h.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var k, v []byte
	idxC, err := h.tx.RwCursorDupSort(h.indexTable)
	if err != nil {
		return err
	}
	defer idxC.Close()
	valsC, err := h.tx.RwCursor(h.historyValsTable)
	if err != nil {
		return err
	}
	defer valsC.Close()
	for k, v, err = historyKeysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		key, txnNumBytes := v[:len(v)-8], v[len(v)-8:]
		{
			kk, vv, err := valsC.SeekExact(txnNumBytes)
			if err != nil {
				return err
			}
			if err := f(txNum, key, vv); err != nil {
				return err
			}
			if kk != nil {
				if err = valsC.DeleteCurrent(); err != nil {
					return err
				}
			}
		}
		if err = idxC.DeleteExact(key, k); err != nil {
			return err
		}
		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if err = historyKeysCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s history keys: %w", h.filenameBase, err)
	}
	return nil
}

type HistoryContext struct {
	h  *History
	ic *InvertedIndexContext

	files   []ctxItem // have no garbage (canDelete=true, overlaps, etc...)
	getters []*compress.Getter
	readers []*recsplit.IndexReader

	trace bool
}

func (h *History) MakeContext() *HistoryContext {
	var hc = HistoryContext{
		h:     h,
		ic:    h.InvertedIndex.MakeContext(),
		files: *h.roFiles.Load(),

		trace: false,
	}
	for _, item := range hc.files {
		if !item.src.frozen {
			item.src.refcount.Inc()
		}
	}

	return &hc
}

func (hc *HistoryContext) statelessGetter(i int) *compress.Getter {
	if hc.getters == nil {
		hc.getters = make([]*compress.Getter, len(hc.files))
	}
	r := hc.getters[i]
	if r == nil {
		r = hc.files[i].src.decompressor.MakeGetter()
		hc.getters[i] = r
	}
	return r
}
func (hc *HistoryContext) statelessIdxReader(i int) *recsplit.IndexReader {
	if hc.readers == nil {
		hc.readers = make([]*recsplit.IndexReader, len(hc.files))
	}
	r := hc.readers[i]
	if r == nil {
		r = hc.files[i].src.index.GetReaderFromPool()
		hc.readers[i] = r
	}
	return r
}

func (hc *HistoryContext) Close() {
	hc.ic.Close()
	for _, item := range hc.files {
		if item.src.frozen {
			continue
		}
		refCnt := item.src.refcount.Dec()
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && item.src.canDelete.Load() {
			item.src.closeFilesAndRemove()
		}
	}
	for _, r := range hc.readers {
		r.Close()
	}

}

func (hc *HistoryContext) getFile(from, to uint64) (it ctxItem, ok bool) {
	for _, item := range hc.files {
		if item.startTxNum == from && item.endTxNum == to {
			return item, true
		}
	}
	return it, false
}

func (hc *HistoryContext) GetNoState(key []byte, txNum uint64) ([]byte, bool, error) {
	exactStep1, exactStep2, lastIndexedTxNum, foundExactShard1, foundExactShard2 := hc.h.localityIndex.lookupIdxFiles(hc.ic.loc, key, txNum)

	//fmt.Printf("GetNoState [%x] %d\n", key, txNum)
	var foundTxNum uint64
	var foundEndTxNum uint64
	var foundStartTxNum uint64
	var found bool
	var findInFile = func(item ctxItem) bool {
		reader := hc.ic.statelessIdxReader(item.i)
		if reader.Empty() {
			return true
		}
		offset := reader.Lookup(key)
		g := hc.ic.statelessGetter(item.i)
		g.Reset(offset)
		k, _ := g.NextUncompressed()

		if !bytes.Equal(k, key) {
			//if bytes.Equal(key, hex.MustDecodeString("009ba32869045058a3f05d6f3dd2abb967e338f6")) {
			//	fmt.Printf("not in this shard: %x, %d, %d-%d\n", k, txNum, item.startTxNum/hc.h.aggregationStep, item.endTxNum/hc.h.aggregationStep)
			//}
			return true
		}
		eliasVal, _ := g.NextUncompressed()
		ef, _ := eliasfano32.ReadEliasFano(eliasVal)
		n, ok := ef.Search(txNum)
		if hc.trace {
			n2, _ := ef.Search(n + 1)
			n3, _ := ef.Search(n - 1)
			fmt.Printf("hist: files: %s %d<-%d->%d->%d, %x\n", hc.h.filenameBase, n3, txNum, n, n2, key)
		}
		if ok {
			foundTxNum = n
			foundEndTxNum = item.endTxNum
			foundStartTxNum = item.startTxNum
			found = true
			return false
		}
		return true
	}

	// -- LocaliyIndex opimization --
	// check up to 2 exact files
	if foundExactShard1 {
		from, to := exactStep1*hc.h.aggregationStep, (exactStep1+StepsInBiggestFile)*hc.h.aggregationStep
		item, ok := hc.ic.getFile(from, to)
		if ok {
			findInFile(item)
		}
		//for _, item := range hc.invIndexFiles {
		//	if item.startTxNum == from && item.endTxNum == to {
		//		findInFile(item)
		//	}
		//}
		//exactShard1, ok := hc.invIndexFiles.Get(ctxItem{startTxNum: exactStep1 * hc.h.aggregationStep, endTxNum: (exactStep1 + StepsInBiggestFile) * hc.h.aggregationStep})
		//if ok {
		//	findInFile(exactShard1)
		//}
	}
	if !found && foundExactShard2 {
		from, to := exactStep2*hc.h.aggregationStep, (exactStep2+StepsInBiggestFile)*hc.h.aggregationStep
		item, ok := hc.ic.getFile(from, to)
		if ok {
			findInFile(item)
		}
		//exactShard2, ok := hc.invIndexFiles.Get(ctxItem{startTxNum: exactStep2 * hc.h.aggregationStep, endTxNum: (exactStep2 + StepsInBiggestFile) * hc.h.aggregationStep})
		//if ok {
		//	findInFile(exactShard2)
		//}
	}
	// otherwise search in recent non-fully-merged files (they are out of LocalityIndex scope)
	// searchFrom - variable already set for this
	// if there is no LocaliyIndex available
	// -- LocaliyIndex opimization End --

	if !found {
		for _, item := range hc.ic.files {
			if item.endTxNum <= lastIndexedTxNum {
				continue
			}
			if !findInFile(item) {
				break
			}
		}
		//hc.invIndexFiles.AscendGreaterOrEqual(ctxItem{startTxNum: lastIndexedTxNum, endTxNum: lastIndexedTxNum}, findInFile)
	}

	if found {
		historyItem, ok := hc.getFile(foundStartTxNum, foundEndTxNum)
		if !ok {
			return nil, false, fmt.Errorf("hist file not found: key=%x, %s.%d-%d", key, hc.h.filenameBase, foundStartTxNum/hc.h.aggregationStep, foundEndTxNum/hc.h.aggregationStep)
		}
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], foundTxNum)
		reader := hc.statelessIdxReader(historyItem.i)
		offset := reader.Lookup2(txKey[:], key)
		//fmt.Printf("offset = %d, txKey=[%x], key=[%x]\n", offset, txKey[:], key)
		g := hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		if hc.h.compressVals {
			v, _ := g.Next(nil)
			return v, true, nil
		}
		v, _ := g.NextUncompressed()
		return v, true, nil
	}
	return nil, false, nil
}

func (hs *HistoryStep) GetNoState(key []byte, txNum uint64) ([]byte, bool, uint64) {
	//fmt.Printf("GetNoState [%x] %d\n", key, txNum)
	if hs.indexFile.reader.Empty() {
		return nil, false, txNum
	}
	offset := hs.indexFile.reader.Lookup(key)
	g := hs.indexFile.getter
	g.Reset(offset)
	k, _ := g.NextUncompressed()
	if !bytes.Equal(k, key) {
		return nil, false, txNum
	}
	//fmt.Printf("Found key=%x\n", k)
	eliasVal, _ := g.NextUncompressed()
	ef, _ := eliasfano32.ReadEliasFano(eliasVal)
	n, ok := ef.Search(txNum)
	if !ok {
		return nil, false, ef.Max()
	}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], n)
	offset = hs.historyFile.reader.Lookup2(txKey[:], key)
	//fmt.Printf("offset = %d, txKey=[%x], key=[%x]\n", offset, txKey[:], key)
	g = hs.historyFile.getter
	g.Reset(offset)
	if hs.compressVals {
		v, _ := g.Next(nil)
		return v, true, txNum
	}
	v, _ := g.NextUncompressed()
	return v, true, txNum
}

func (hs *HistoryStep) MaxTxNum(key []byte) (bool, uint64) {
	if hs.indexFile.reader.Empty() {
		return false, 0
	}
	offset := hs.indexFile.reader.Lookup(key)
	g := hs.indexFile.getter
	g.Reset(offset)
	k, _ := g.NextUncompressed()
	if !bytes.Equal(k, key) {
		return false, 0
	}
	//fmt.Printf("Found key=%x\n", k)
	eliasVal, _ := g.NextUncompressed()
	return true, eliasfano32.Max(eliasVal)
}

// GetNoStateWithRecent searches history for a value of specified key before txNum
// second return value is true if the value is found in the history (even if it is nil)
func (hc *HistoryContext) GetNoStateWithRecent(key []byte, txNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	v, ok, err := hc.GetNoState(key, txNum)
	if err != nil {
		return nil, ok, err
	}
	if ok {
		return v, true, nil
	}

	// Value not found in history files, look in the recent history
	if roTx == nil {
		return nil, false, fmt.Errorf("roTx is nil")
	}
	v, ok, err = hc.getNoStateFromDB(key, txNum, roTx)
	if err != nil {
		return nil, ok, err
	}
	if ok {
		return v, true, nil
	}
	return nil, false, err
}

func (hc *HistoryContext) getNoStateFromDB(key []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	indexCursor, err := tx.CursorDupSort(hc.h.indexTable)
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
		if hc.trace {
			_, vv, _ := indexCursor.NextDup()
			indexCursor.Prev()
			_, prevV, _ := indexCursor.Prev()
			fmt.Printf("hist: db: %s, %d<-%d->%d->%d, %x\n", hc.h.filenameBase, u64or0(prevV), txNum, u64or0(foundTxNumVal), u64or0(vv), key)
		}

		var historyKeysCursor kv.CursorDupSort
		if historyKeysCursor, err = tx.CursorDupSort(hc.h.indexKeysTable); err != nil {
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
		if v, err = tx.GetOne(hc.h.historyValsTable, vn[len(vn)-8:]); err != nil {
			return nil, false, err
		}
		return v, true, nil
	}
	return nil, false, nil
}

func (hc *HistoryContext) WalkAsOf(startTxNum uint64, from, to []byte, roTx kv.Tx, amount int) *StateAsOfIter {
	hi := StateAsOfIter{
		hasNextInDb:  true,
		roTx:         roTx,
		indexTable:   hc.h.indexTable,
		idxKeysTable: hc.h.indexKeysTable,
		valsTable:    hc.h.historyValsTable,
		from:         from, to: to, limit: amount,
	}
	for _, item := range hc.ic.files {
		if item.endTxNum <= startTxNum {
			continue
		}
		// TODO: seek(from)
		g := item.src.decompressor.MakeGetter()
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.NextUncompressed()
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
			hi.hasNextInFiles = true
		}
		hi.total += uint64(item.getter.Size())
	}
	hi.hc = hc
	hi.compressVals = hc.h.compressVals
	hi.startTxNum = startTxNum
	binary.BigEndian.PutUint64(hi.startTxKey[:], startTxNum)
	hi.advanceInDb()
	hi.advanceInFiles()
	hi.advance()
	return &hi
}

type StateAsOfIter struct {
	roTx          kv.Tx
	txNum2kCursor kv.CursorDupSort
	idxCursor     kv.CursorDupSort
	hc            *HistoryContext
	valsTable     string
	idxKeysTable  string
	indexTable    string

	from, to []byte
	limit    int

	nextFileKey []byte
	nextDbKey   []byte
	nextDbVal   []byte
	nextFileVal []byte
	nextVal     []byte
	nextKey     []byte

	h              ReconHeap
	total          uint64
	startTxNum     uint64
	advFileCnt     int
	advDbCnt       int
	startTxKey     [8]byte
	txnKey         [8]byte
	hasNextInFiles bool
	hasNextInDb    bool
	compressVals   bool

	k, v, kBackup, vBackup []byte
}

func (hi *StateAsOfIter) Stat() (int, int) { return hi.advDbCnt, hi.advFileCnt }

func (hi *StateAsOfIter) Close() {
	if hi.idxCursor != nil {
		hi.idxCursor.Close()
	}
	if hi.txNum2kCursor != nil {
		hi.txNum2kCursor.Close()
	}
}

func (hi *StateAsOfIter) advanceInFiles() {
	hi.advFileCnt++
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		var idxVal []byte
		if hi.compressVals {
			idxVal, _ = top.g.Next(nil)
		} else {
			idxVal, _ = top.g.NextUncompressed()
		}
		if top.g.HasNext() {
			if hi.compressVals {
				top.key, _ = top.g.Next(nil)
			} else {
				top.key, _ = top.g.NextUncompressed()
			}
			if hi.to == nil || bytes.Compare(top.key, hi.to) < 0 {
				heap.Push(&hi.h, top)
			}
		}

		if hi.from != nil && bytes.Compare(key, hi.from) < 0 { //TODO: replace by Seek()
			continue
		}

		if bytes.Equal(key, hi.nextFileKey) {
			continue
		}
		ef, _ := eliasfano32.ReadEliasFano(idxVal)
		n, ok := ef.Search(hi.startTxNum)
		if !ok {
			continue
		}

		hi.nextFileKey = key
		binary.BigEndian.PutUint64(hi.txnKey[:], n)
		historyItem, ok := hi.hc.getFile(top.startTxNum, top.endTxNum)
		if !ok {
			panic(fmt.Errorf("no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextFileKey))
		}
		reader := hi.hc.statelessIdxReader(historyItem.i)
		offset := reader.Lookup2(hi.txnKey[:], hi.nextFileKey)
		g := hi.hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		if hi.compressVals {
			hi.nextFileVal, _ = g.Next(nil)
		} else {
			hi.nextFileVal, _ = g.NextUncompressed()
		}
		hi.nextFileKey = key
		return
	}
	hi.hasNextInFiles = false
}

func (hi *StateAsOfIter) advanceInDb() {
	hi.advDbCnt++
	var k []byte
	var err error
	if hi.idxCursor == nil {
		if hi.idxCursor, err = hi.roTx.CursorDupSort(hi.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		if hi.txNum2kCursor, err = hi.roTx.CursorDupSort(hi.idxKeysTable); err != nil {
			panic(err)
		}
		if k, _, err = hi.idxCursor.Seek(hi.from); err != nil {
			// TODO pass error properly around
			panic(err)
		}
	} else {
		if k, _, err = hi.idxCursor.NextNoDup(); err != nil {
			panic(err)
		}
	}
	for ; k != nil; k, _, err = hi.idxCursor.NextNoDup() {
		if err != nil {
			panic(err)
		}
		if hi.to != nil && bytes.Compare(k, hi.to) >= 0 {
			break
		}

		foundTxNumVal, err := hi.idxCursor.SeekBothRange(k, hi.startTxKey[:])
		if err != nil {
			panic(err)
		}
		if foundTxNumVal == nil {
			continue
		}
		//txNum := binary.BigEndian.Uint64(foundTxNumVal)
		//if txNum >= hi.endTxNum {
		//	continue
		//}
		hi.nextDbKey = append(hi.nextDbKey[:0], k...)
		vn, err := hi.txNum2kCursor.SeekBothRange(foundTxNumVal, k)
		if err != nil {
			panic(err)
		}
		valNum := binary.BigEndian.Uint64(vn[len(vn)-8:])
		if valNum == 0 {
			// This is special valNum == 0, which is empty value
			hi.nextDbVal = hi.nextDbVal[:0]
			return
		}
		v, err := hi.roTx.GetOne(hi.valsTable, vn[len(vn)-8:])
		if err != nil {
			panic(err)
		}
		hi.nextDbVal = append(hi.nextDbVal[:0], v...)
		return
	}
	hi.idxCursor.Close()
	hi.idxCursor = nil
	hi.hasNextInDb = false
}

func (hi *StateAsOfIter) advance() {
	if hi.hasNextInFiles {
		if hi.hasNextInDb {
			c := bytes.Compare(hi.nextFileKey, hi.nextDbKey)
			if c < 0 {
				hi.nextKey = append(hi.nextKey[:0], hi.nextFileKey...)
				hi.nextVal = append(hi.nextVal[:0], hi.nextFileVal...)
				hi.advanceInFiles()
			} else if c > 0 {
				hi.nextKey = append(hi.nextKey[:0], hi.nextDbKey...)
				hi.nextVal = append(hi.nextVal[:0], hi.nextDbVal...)
				hi.advanceInDb()
			} else {
				hi.nextKey = append(hi.nextKey[:0], hi.nextFileKey...)
				hi.nextVal = append(hi.nextVal[:0], hi.nextFileVal...)
				hi.advanceInDb()
				hi.advanceInFiles()
			}
		} else {
			hi.nextKey = append(hi.nextKey[:0], hi.nextFileKey...)
			hi.nextVal = append(hi.nextVal[:0], hi.nextFileVal...)
			hi.advanceInFiles()
		}
	} else if hi.hasNextInDb {
		hi.nextKey = append(hi.nextKey[:0], hi.nextDbKey...)
		hi.nextVal = append(hi.nextVal[:0], hi.nextDbVal...)
		hi.advanceInDb()
	} else {
		hi.nextKey = nil
		hi.nextVal = nil
	}
}

func (hi *StateAsOfIter) HasNext() bool {
	return hi.limit != 0 && (hi.hasNextInFiles || hi.hasNextInDb || hi.nextKey != nil)
}

func (hi *StateAsOfIter) Next() ([]byte, []byte, error) {
	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy iter.Dual Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	hi.advance()
	return hi.kBackup, hi.vBackup, nil
}

func (hc *HistoryContext) IterateChanged(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) *HistoryChangesIter {
	if asc == order.Desc {
		panic("not supported yet")
	}
	if limit >= 0 {
		panic("not supported yet")
	}
	if fromTxNum < 0 {
		panic("not supported yet")
	}
	if toTxNum < 0 {
		panic("not supported yet")
	}
	startTxNum, endTxNum := uint64(fromTxNum), uint64(toTxNum)

	hi := HistoryChangesIter{
		hasNextInDb:  true,
		roTx:         roTx,
		indexTable:   hc.h.indexTable,
		idxKeysTable: hc.h.indexKeysTable,
		valsTable:    hc.h.historyValsTable,
	}

	for _, item := range hc.ic.files {
		if item.endTxNum >= endTxNum {
			hi.hasNextInDb = false
		}
		if item.endTxNum <= startTxNum {
			continue
		}
		if item.startTxNum >= endTxNum {
			break
		}
		g := item.src.decompressor.MakeGetter()
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.NextUncompressed()
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
			hi.hasNextInFiles = true
		}
		hi.total += uint64(g.Size())
	}
	hi.hc = hc
	hi.compressVals = hc.h.compressVals
	hi.startTxNum = startTxNum
	hi.endTxNum = endTxNum
	binary.BigEndian.PutUint64(hi.startTxKey[:], startTxNum)
	hi.advanceInDb()
	hi.advanceInFiles()
	hi.advance()
	return &hi
}

type HistoryChangesIter struct {
	roTx           kv.Tx
	txNum2kCursor  kv.CursorDupSort
	idxCursor      kv.CursorDupSort
	hc             *HistoryContext
	valsTable      string
	idxKeysTable   string
	indexTable     string
	nextFileKey    []byte
	nextDbKey      []byte
	nextDbVal      []byte
	nextFileVal    []byte
	nextVal        []byte
	nextKey        []byte
	h              ReconHeap
	total          uint64
	endTxNum       uint64
	startTxNum     uint64
	advFileCnt     int
	advDbCnt       int
	startTxKey     [8]byte
	txnKey         [8]byte
	hasNextInFiles bool
	hasNextInDb    bool
	compressVals   bool

	k, v []byte
}

func (hi *HistoryChangesIter) Stat() (int, int) { return hi.advDbCnt, hi.advFileCnt }

func (hi *HistoryChangesIter) Close() {
	if hi.idxCursor != nil {
		hi.idxCursor.Close()
	}
	if hi.txNum2kCursor != nil {
		hi.txNum2kCursor.Close()
	}
}

func (hi *HistoryChangesIter) advanceInFiles() {
	hi.advFileCnt++
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		var idxVal []byte
		if hi.compressVals {
			idxVal, _ = top.g.Next(nil)
		} else {
			idxVal, _ = top.g.NextUncompressed()
		}
		if top.g.HasNext() {
			if hi.compressVals {
				top.key, _ = top.g.Next(nil)
			} else {
				top.key, _ = top.g.NextUncompressed()
			}
			heap.Push(&hi.h, top)
		}

		if bytes.Equal(key, hi.nextFileKey) {
			continue
		}
		ef, _ := eliasfano32.ReadEliasFano(idxVal)
		n, ok := ef.Search(hi.startTxNum)
		if !ok {
			continue
		}
		if n >= hi.endTxNum {
			continue
		}

		hi.nextFileKey = key
		binary.BigEndian.PutUint64(hi.txnKey[:], n)
		historyItem, ok := hi.hc.getFile(top.startTxNum, top.endTxNum)
		if !ok {
			panic(fmt.Errorf("no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextFileKey))
		}
		reader := hi.hc.statelessIdxReader(historyItem.i)
		offset := reader.Lookup2(hi.txnKey[:], hi.nextFileKey)
		g := hi.hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		if hi.compressVals {
			hi.nextFileVal, _ = g.Next(nil)
		} else {
			hi.nextFileVal, _ = g.NextUncompressed()
		}
		hi.nextFileKey = key
		return
	}
	hi.hasNextInFiles = false
}

func (hi *HistoryChangesIter) advanceInDb() {
	hi.advDbCnt++
	var k []byte
	var err error
	if hi.idxCursor == nil {
		if hi.idxCursor, err = hi.roTx.CursorDupSort(hi.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		if hi.txNum2kCursor, err = hi.roTx.CursorDupSort(hi.idxKeysTable); err != nil {
			panic(err)
		}

		if k, _, err = hi.idxCursor.First(); err != nil {
			// TODO pass error properly around
			panic(err)
		}
	} else {
		if k, _, err = hi.idxCursor.NextNoDup(); err != nil {
			panic(err)
		}
	}
	for ; k != nil; k, _, err = hi.idxCursor.NextNoDup() {
		if err != nil {
			panic(err)
		}
		foundTxNumVal, err := hi.idxCursor.SeekBothRange(k, hi.startTxKey[:])
		if err != nil {
			panic(err)
		}
		if foundTxNumVal == nil {
			continue
		}
		txNum := binary.BigEndian.Uint64(foundTxNumVal)
		if txNum >= hi.endTxNum {
			continue
		}
		hi.nextDbKey = append(hi.nextDbKey[:0], k...)
		vn, err := hi.txNum2kCursor.SeekBothRange(foundTxNumVal, k)
		if err != nil {
			panic(err)
		}
		valNum := binary.BigEndian.Uint64(vn[len(vn)-8:])
		if valNum == 0 {
			// This is special valNum == 0, which is empty value
			hi.nextDbVal = hi.nextDbVal[:0]
			return
		}
		v, err := hi.roTx.GetOne(hi.valsTable, vn[len(vn)-8:])
		if err != nil {
			panic(err)
		}
		hi.nextDbVal = append(hi.nextDbVal[:0], v...)
		return
	}
	hi.idxCursor.Close()
	hi.idxCursor = nil
	hi.hasNextInDb = false
}

func (hi *HistoryChangesIter) advance() {
	if hi.hasNextInFiles {
		if hi.hasNextInDb {
			c := bytes.Compare(hi.nextFileKey, hi.nextDbKey)
			if c < 0 {
				hi.nextKey = append(hi.nextKey[:0], hi.nextFileKey...)
				hi.nextVal = append(hi.nextVal[:0], hi.nextFileVal...)
				hi.advanceInFiles()
			} else if c > 0 {
				hi.nextKey = append(hi.nextKey[:0], hi.nextDbKey...)
				hi.nextVal = append(hi.nextVal[:0], hi.nextDbVal...)
				hi.advanceInDb()
			} else {
				hi.nextKey = append(hi.nextKey[:0], hi.nextFileKey...)
				hi.nextVal = append(hi.nextVal[:0], hi.nextFileVal...)
				hi.advanceInDb()
				hi.advanceInFiles()
			}
		} else {
			hi.nextKey = append(hi.nextKey[:0], hi.nextFileKey...)
			hi.nextVal = append(hi.nextVal[:0], hi.nextFileVal...)
			hi.advanceInFiles()
		}
	} else if hi.hasNextInDb {
		hi.nextKey = append(hi.nextKey[:0], hi.nextDbKey...)
		hi.nextVal = append(hi.nextVal[:0], hi.nextDbVal...)
		hi.advanceInDb()
	} else {
		hi.nextKey = nil
		hi.nextVal = nil
	}
}

func (hi *HistoryChangesIter) HasNext() bool {
	return hi.hasNextInFiles || hi.hasNextInDb || hi.nextKey != nil
}

func (hi *HistoryChangesIter) Next() ([]byte, []byte, error) {
	hi.k = append(hi.k[:0], hi.nextKey...)
	hi.v = append(hi.v[:0], hi.nextVal...)
	hi.advance()
	return hi.k, hi.v, nil
}

func (hc *HistoryContext) IterateRecentlyChanged(startTxNum, endTxNum uint64, roTx kv.Tx, f func([]byte, []byte) error) error {
	col := etl.NewCollector("", hc.h.tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer col.Close()
	col.LogLvl(log.LvlTrace)

	it := hc.IterateRecentlyChangedUnordered(startTxNum, endTxNum, roTx)
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		if err := col.Collect(k, v); err != nil {
			return err
		}
	}
	return col.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return f(k, v)
	}, etl.TransformArgs{})
}

func (hc *HistoryContext) IterateRecentlyChangedUnordered(startTxNum, endTxNum uint64, roTx kv.Tx) *HistoryIterator2 {
	hi := HistoryIterator2{
		hasNext:      true,
		roTx:         roTx,
		idxKeysTable: hc.h.indexKeysTable,
		valsTable:    hc.h.historyValsTable,
		hc:           hc,
		startTxNum:   startTxNum,
		endTxNum:     endTxNum,
	}
	binary.BigEndian.PutUint64(hi.startTxKey[:], startTxNum)
	hi.advanceInDb()
	return &hi
}

type HistoryIterator2 struct {
	roTx          kv.Tx
	txNum2kCursor kv.CursorDupSort
	hc            *HistoryContext
	idxKeysTable  string
	valsTable     string
	nextKey       []byte
	nextVal       []byte
	nextErr       error
	endTxNum      uint64
	startTxNum    uint64
	advDbCnt      int
	startTxKey    [8]byte
	hasNext       bool
}

func (hi *HistoryIterator2) Stat() int { return hi.advDbCnt }

func (hi *HistoryIterator2) Close() {
	if hi.txNum2kCursor != nil {
		hi.txNum2kCursor.Close()
	}
}

func (hi *HistoryIterator2) advanceInDb() {
	hi.advDbCnt++
	var k, v []byte
	var err error
	if hi.txNum2kCursor == nil {
		if hi.txNum2kCursor, err = hi.roTx.CursorDupSort(hi.idxKeysTable); err != nil {
			hi.nextErr, hi.hasNext = err, true
			return
		}
		if k, v, err = hi.txNum2kCursor.Seek(hi.startTxKey[:]); err != nil {
			hi.nextErr, hi.hasNext = err, true
			return
		}
	} else {
		if k, v, err = hi.txNum2kCursor.NextDup(); err != nil {
			hi.nextErr, hi.hasNext = err, true
			return
		}
		if k == nil {
			k, v, err = hi.txNum2kCursor.NextNoDup()
			if err != nil {
				hi.nextErr, hi.hasNext = err, true
				return
			}
			if k != nil && binary.BigEndian.Uint64(k) >= hi.endTxNum {
				k = nil // end
			}
		}
	}
	if k != nil {
		hi.nextKey = v[:len(v)-8]
		hi.hasNext = true

		valNum := v[len(v)-8:]

		if binary.BigEndian.Uint64(valNum) == 0 {
			// This is special valNum == 0, which is empty value
			hi.nextVal = []byte{}
			return
		}
		val, err := hi.roTx.GetOne(hi.valsTable, valNum)
		if err != nil {
			hi.nextErr, hi.hasNext = err, true
			return
		}
		hi.nextVal = val
		return
	}
	hi.txNum2kCursor.Close()
	hi.txNum2kCursor = nil
	hi.hasNext = false
}

func (hi *HistoryIterator2) HasNext() bool {
	return hi.hasNext
}

func (hi *HistoryIterator2) Next() ([]byte, []byte, error) {
	k, v, err := hi.nextKey, hi.nextVal, hi.nextErr
	if err != nil {
		return nil, nil, err
	}
	hi.advanceInDb()
	return k, v, nil
}

func (h *History) DisableReadAhead() {
	h.InvertedIndex.DisableReadAhead()
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.DisableReadAhead()
			if item.index != nil {
				item.index.DisableReadAhead()
			}
		}
		return true
	})
}

func (h *History) EnableReadAhead() *History {
	h.InvertedIndex.EnableReadAhead()
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableReadAhead()
			if item.index != nil {
				item.index.EnableReadAhead()
			}
		}
		return true
	})
	return h
}
func (h *History) EnableMadvWillNeed() *History {
	h.InvertedIndex.EnableMadvWillNeed()
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableWillNeed()
			if item.index != nil {
				item.index.EnableWillNeed()
			}
		}
		return true
	})
	return h
}
func (h *History) EnableMadvNormalReadAhead() *History {
	h.InvertedIndex.EnableMadvNormalReadAhead()
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableMadvNormal()
			if item.index != nil {
				item.index.EnableMadvNormal()
			}
		}
		return true
	})
	return h
}

// HistoryStep used for incremental state reconsitution, it isolates only one snapshot interval
type HistoryStep struct {
	compressVals bool
	indexItem    *filesItem
	indexFile    ctxItem
	historyItem  *filesItem
	historyFile  ctxItem
}

// MakeSteps [0, toTxNum)
func (h *History) MakeSteps(toTxNum uint64) []*HistoryStep {
	var steps []*HistoryStep
	h.InvertedIndex.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || !item.frozen || item.startTxNum >= toTxNum {
				continue
			}

			step := &HistoryStep{
				compressVals: h.compressVals,
				indexItem:    item,
				indexFile: ctxItem{
					startTxNum: item.startTxNum,
					endTxNum:   item.endTxNum,
					getter:     item.decompressor.MakeGetter(),
					reader:     recsplit.NewIndexReader(item.index),
				},
			}
			steps = append(steps, step)
		}
		return true
	})
	i := 0
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || !item.frozen || item.startTxNum >= toTxNum {
				continue
			}
			steps[i].historyItem = item
			steps[i].historyFile = ctxItem{
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				getter:     item.decompressor.MakeGetter(),
				reader:     recsplit.NewIndexReader(item.index),
			}
			i++
		}
		return true
	})
	return steps
}

func (hs *HistoryStep) Clone() *HistoryStep {
	return &HistoryStep{
		compressVals: hs.compressVals,
		indexItem:    hs.indexItem,
		indexFile: ctxItem{
			startTxNum: hs.indexFile.startTxNum,
			endTxNum:   hs.indexFile.endTxNum,
			getter:     hs.indexItem.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(hs.indexItem.index),
		},
		historyItem: hs.historyItem,
		historyFile: ctxItem{
			startTxNum: hs.historyFile.startTxNum,
			endTxNum:   hs.historyFile.endTxNum,
			getter:     hs.historyItem.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(hs.historyItem.index),
		},
	}
}

func u64or0(in []byte) (v uint64) {
	if len(in) > 0 {
		v = binary.BigEndian.Uint64(in)
	}
	return v
}

func (h *History) CleanupDir() {
	files, _ := h.fileNamesOnDisk()
	uselessFiles := h.scanStateFiles(files)
	for _, f := range uselessFiles {
		fName := fmt.Sprintf("%s.%d-%d.v", h.filenameBase, f.startTxNum/h.aggregationStep, f.endTxNum/h.aggregationStep)
		err := os.Remove(filepath.Join(h.dir, fName))
		log.Debug("[clean] remove", "file", fName, "err", err)
		fIdxName := fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, f.startTxNum/h.aggregationStep, f.endTxNum/h.aggregationStep)
		err = os.Remove(filepath.Join(h.dir, fIdxName))
		log.Debug("[clean] remove", "file", fName, "err", err)
	}
	h.InvertedIndex.CleanupDir()
}
