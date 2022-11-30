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
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/semaphore"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

type History struct {
	*InvertedIndex
	files            *btree.BTreeG[*filesItem]
	historyValsTable string // key1+key2+txnNum -> oldValue , stores values BEFORE change
	settingsTable    string
	workers          int
	compressVals     bool

	wal     *historyWAL
	walLock sync.RWMutex
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
) (*History, error) {
	h := History{
		files:            btree.NewG[*filesItem](32, filesItemLess),
		historyValsTable: historyValsTable,
		settingsTable:    settingsTable,
		compressVals:     compressVals,
		workers:          1,
	}
	var err error
	h.InvertedIndex, err = NewInvertedIndex(dir, tmpdir, aggregationStep, filenameBase, indexKeysTable, indexTable)
	if err != nil {
		return nil, fmt.Errorf("NewHistory: %s, %w", filenameBase, err)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	h.scanStateFiles(files)
	if err = h.openFiles(); err != nil {
		return nil, fmt.Errorf("NewHistory.openFiles: %s, %w", filenameBase, err)
	}
	return &h, nil
}

func (h *History) scanStateFiles(files []fs.DirEntry) {
	re := regexp.MustCompile("^" + h.filenameBase + ".([0-9]+)-([0-9]+).v$")
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
		var item = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		{
			var subSet, superSet *filesItem
			h.files.DescendLessOrEqual(item, func(it *filesItem) bool {
				if it.isSubsetOf(item) {
					subSet = it
				} else if item.isSubsetOf(it) {
					superSet = it
				}
				return true
			})
			if subSet != nil {
				h.files.Delete(subSet)
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.v", h.filenameBase, subSet.startTxNum/h.aggregationStep, subSet.endTxNum/h.aggregationStep),
					fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, subSet.startTxNum/h.aggregationStep, subSet.endTxNum/h.aggregationStep),
				)
			}
			if superSet != nil {
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.v", h.filenameBase, startStep, endStep),
					fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, startStep, endStep),
				)
				continue
			}
		}
		{
			var subSet, superSet *filesItem
			h.files.AscendGreaterOrEqual(item, func(it *filesItem) bool {
				if it.isSubsetOf(item) {
					subSet = it
				} else if item.isSubsetOf(it) {
					superSet = it
				}
				return false
			})
			if subSet != nil {
				h.files.Delete(subSet)
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.v", h.filenameBase, subSet.startTxNum/h.aggregationStep, subSet.endTxNum/h.aggregationStep),
					fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, subSet.startTxNum/h.aggregationStep, subSet.endTxNum/h.aggregationStep),
				)
			}
			if superSet != nil {
				uselessFiles = append(uselessFiles,
					fmt.Sprintf("%s.%d-%d.v", h.filenameBase, startStep, endStep),
					fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, startStep, endStep),
				)
				continue
			}
		}
		h.files.ReplaceOrInsert(item)
	}
	if len(uselessFiles) > 0 {
		log.Info("[snapshots] history can delete", "files", strings.Join(uselessFiles, ","))
	}
}

func (h *History) openFiles() error {
	var totalKeys uint64
	var err error

	invalidFileItems := make([]*filesItem, 0)
	h.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
		datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, fromStep, toStep))
		if !dir.FileExist(datPath) {
			invalidFileItems = append(invalidFileItems, item)
			return true
		}
		if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			log.Debug("Hisrory.openFiles: %w, %s", err, datPath)
			return false
		}
		if item.index == nil {
			idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep))
			if dir.FileExist(idxPath) {
				if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
					log.Debug("Hisrory.openFiles: %w, %s", err, idxPath)
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
	return nil
}

func (h *History) closeFiles() {
	h.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (h *History) Close() {
	h.InvertedIndex.Close()
	h.closeFiles()
}

func (h *History) Files() (res []string) {
	h.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			_, fName := filepath.Split(item.decompressor.FilePath())
			res = append(res, filepath.Join("history", fName))
		}
		return true
	})
	res = append(res, h.InvertedIndex.Files()...)
	return res
}

func (h *History) missedIdxFiles() (l []*filesItem) {
	h.files.Ascend(func(item *filesItem) bool { // don't run slow logic while iterating on btree
		fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
		if !dir.FileExist(filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep))) {
			l = append(l, item)
		}
		return true
	})
	return l
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (h *History) BuildMissedIndices(ctx context.Context, sem *semaphore.Weighted) (err error) {
	if err := h.InvertedIndex.BuildMissedIndices(ctx, sem); err != nil {
		return err
	}

	missedFiles := h.missedIdxFiles()
	errs := make(chan error, len(missedFiles))
	wg := sync.WaitGroup{}

	for _, item := range missedFiles {
		if err := sem.Acquire(ctx, 1); err != nil {
			errs <- err
			break
		}
		wg.Add(1)
		go func(item *filesItem) {
			defer sem.Release(1)
			defer wg.Done()

			search := &filesItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum}
			iiItem, ok := h.InvertedIndex.files.Get(search)
			if !ok {
				return
			}

			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			fName := fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep)
			idxPath := filepath.Join(h.dir, fName)
			log.Info("[snapshots] build idx", "file", fName)
			count, err := iterateForVi(item, iiItem, h.compressVals, func(v []byte) error { return nil })
			if err != nil {
				errs <- err
			}
			errs <- buildVi(item, iiItem, idxPath, h.tmpdir, count, false /* values */, h.compressVals)
		}(item)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	var lastError error
	for err := range errs {
		if err != nil {
			lastError = err
		}
	}
	if lastError != nil {
		return lastError
	}

	return h.openFiles()
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
			ef, _ := eliasfano32.ReadEliasFano(ci1.val)
			for i := uint64(0); i < ef.Count(); i++ {
				if compressVals {
					valBuf, _ = ci1.dg2.Next(valBuf[:0])
				} else {
					valBuf, _ = ci1.dg2.NextUncompressed()
				}
				if err = f(valBuf); err != nil {
					return count, err
				}
			}
			count += int(ef.Count())
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

func buildVi(historyItem, iiItem *filesItem, historyIdxPath, tmpdir string, count int, values, compressVals bool) error {
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
			keyBuf, _ = g.NextUncompressed()
			valBuf, _ = g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(valBuf)
			efIt := ef.Iterator()
			for efIt.HasNext() {
				txNum := efIt.Next()
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
	h.walLock.RLock() // read-lock for reading fielw `w` and writing into it, write-lock for setting new `w`
	err = h.wal.addPrevValue(key1, key2, original)
	h.walLock.RUnlock()
	return err
}

func (h *History) DiscardHistory(tmpdir string) {
	h.InvertedIndex.StartWrites(tmpdir)
	h.walLock.Lock()
	defer h.walLock.Unlock()
	h.wal = h.newWriter(tmpdir, false, true)
}
func (h *History) StartWrites(tmpdir string) {
	h.InvertedIndex.StartWrites(tmpdir)
	h.walLock.Lock()
	defer h.walLock.Unlock()
	h.wal = h.newWriter(tmpdir, true, false)
}
func (h *History) FinishWrites() {
	h.InvertedIndex.FinishWrites()
	h.walLock.Lock()
	defer h.walLock.Unlock()
	h.wal.close()
	h.wal = nil
}

func (h *History) Rotate() historyFlusher {
	h.walLock.Lock()
	defer h.walLock.Unlock()
	w := h.wal
	h.wal = h.newWriter(h.wal.tmpdir, h.wal.buffered, h.wal.discard)
	h.wal.autoIncrement = w.autoIncrement
	return historyFlusher{w, h.InvertedIndex.Rotate()}
}

type historyFlusher struct {
	h *historyWAL
	i *invertedIndexWAL
}

func (f historyFlusher) Flush(tx kv.RwTx) error {
	if err := f.i.Flush(tx); err != nil {
		return err
	}
	if err := f.h.flush(tx); err != nil {
		return err
	}
	return nil
}

type historyWAL struct {
	h                *History
	historyVals      *etl.Collector
	tmpdir           string
	autoIncrementBuf []byte
	historyKey       []byte
	autoIncrement    uint64
	buffered         bool
	discard          bool
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
		w.historyVals.LogLvl(log.LvlTrace)
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

func (h *historyWAL) flush(tx kv.RwTx) error {
	if h.discard {
		return nil
	}
	binary.BigEndian.PutUint64(h.autoIncrementBuf, h.autoIncrement)
	if err := tx.Put(h.h.settingsTable, historyValCountKey, h.autoIncrementBuf); err != nil {
		return err
	}
	if err := h.historyVals.Load(tx, h.h.historyValsTable, loadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	h.close()
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
	if historyComp, err = compress.NewCompressor(context.Background(), "collate history", historyPath, h.tmpdir, compress.MinPatternScore, h.workers, log.LvlTrace); err != nil {
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
	efHistoryComp, err = compress.NewCompressor(ctx, "ef history", efHistoryPath, h.tmpdir, compress.MinPatternScore, h.workers, log.LvlTrace)
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
	if efHistoryIdx, err = buildIndex(ctx, efHistoryDecomp, efHistoryIdxPath, h.tmpdir, len(keys), false /* values */); err != nil {
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
	h.files.ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.historyDecomp,
		index:        sf.historyIdx,
	})
}

func (h *History) warmup(txFrom, limit uint64, tx kv.Tx) error {
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
	select {
	case <-ctx.Done():
		return nil
	default:
	}

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
	for ; err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		if err = valsC.Delete(v[len(v)-8:]); err != nil {
			return err
		}
		if err = idxC.DeleteExact(v[:len(v)-8], k); err != nil {
			return err
		}
		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if err = historyKeysCursor.DeleteCurrent(); err != nil {
			return err
		}
		select {
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
	h                        *History
	indexFiles, historyFiles *btree.BTreeG[ctxItem]

	tx kv.Tx
}

func (h *History) MakeContext() *HistoryContext {
	var hc = HistoryContext{h: h}
	hc.indexFiles = btree.NewG[ctxItem](32, ctxItemLess)
	h.InvertedIndex.files.Ascend(func(item *filesItem) bool {
		if item.index == nil {
			return false
		}
		hc.indexFiles.ReplaceOrInsert(ctxItem{
			startTxNum: item.startTxNum,
			endTxNum:   item.endTxNum,
			getter:     item.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(item.index),
		})
		return true
	})
	hc.historyFiles = btree.NewG[ctxItem](32, ctxItemLess)
	h.files.Ascend(func(item *filesItem) bool {
		if item.index == nil {
			return false
		}
		hc.historyFiles.ReplaceOrInsert(ctxItem{
			startTxNum: item.startTxNum,
			endTxNum:   item.endTxNum,
			getter:     item.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(item.index),
		})
		return true
	})
	return &hc
}
func (hc *HistoryContext) SetTx(tx kv.Tx) { hc.tx = tx }

func (hc *HistoryContext) GetNoState(key []byte, txNum uint64) ([]byte, bool, error) {
	//fmt.Printf("GetNoState [%x] %d\n", key, txNum)
	var foundTxNum uint64
	var foundEndTxNum uint64
	var foundStartTxNum uint64
	var found bool
	hc.indexFiles.AscendGreaterOrEqual(ctxItem{startTxNum: txNum, endTxNum: txNum}, func(item ctxItem) bool {
		//fmt.Printf("ef item %d-%d, key %x\n", item.startTxNum, item.endTxNum, key)
		if item.reader.Empty() {
			return true
		}
		offset := item.reader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		k, _ := g.NextUncompressed()
		if !bytes.Equal(k, key) {
			return true
		}
		//fmt.Printf("Found key=%x\n", k)
		eliasVal, _ := g.NextUncompressed()
		ef, _ := eliasfano32.ReadEliasFano(eliasVal)
		n, ok := ef.Search(txNum)
		if ok {
			foundTxNum = n
			foundEndTxNum = item.endTxNum
			foundStartTxNum = item.startTxNum
			found = true
			//fmt.Printf("Found n=%d\n", n)
			return false
		}
		return true
	})
	if found {
		var historyItem ctxItem
		var ok bool
		var search ctxItem
		search.startTxNum = foundStartTxNum
		search.endTxNum = foundEndTxNum
		if historyItem, ok = hc.historyFiles.Get(search); !ok {
			return nil, false, fmt.Errorf("no %s file found for [%x]", hc.h.filenameBase, key)
		}
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], foundTxNum)
		offset := historyItem.reader.Lookup2(txKey[:], key)
		//fmt.Printf("offset = %d, txKey=[%x], key=[%x]\n", offset, txKey[:], key)
		g := historyItem.getter
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

func (hc *HistoryContext) IterateChanged(startTxNum, endTxNum uint64, roTx kv.Tx) *HistoryIterator1 {
	hi := HistoryIterator1{
		hasNextInDb:  true,
		roTx:         roTx,
		indexTable:   hc.h.indexTable,
		idxKeysTable: hc.h.indexKeysTable,
		valsTable:    hc.h.historyValsTable,
	}

	hc.indexFiles.Ascend(func(item ctxItem) bool {
		if item.endTxNum >= endTxNum {
			hi.hasNextInDb = false
		}
		if item.endTxNum <= startTxNum {
			return true
		}
		if item.startTxNum >= endTxNum {
			return false
		}
		g := item.getter
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.NextUncompressed()
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
			hi.hasNextInFiles = true
		}
		hi.total += uint64(item.getter.Size())
		return true
	})
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

type HistoryIterator1 struct {
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
}

func (hi *HistoryIterator1) Stat() (int, int) { return hi.advDbCnt, hi.advFileCnt }

func (hi *HistoryIterator1) Close() {
	if hi.idxCursor != nil {
		hi.idxCursor.Close()
	}
	if hi.txNum2kCursor != nil {
		hi.txNum2kCursor.Close()
	}
}

func (hi *HistoryIterator1) advanceInFiles() {
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
		search := ctxItem{startTxNum: top.startTxNum, endTxNum: top.endTxNum}
		historyItem, ok := hi.hc.historyFiles.Get(search)
		if !ok {
			panic(fmt.Errorf("no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextFileKey))
		}
		offset := historyItem.reader.Lookup2(hi.txnKey[:], hi.nextFileKey)
		g := historyItem.getter
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

func (hi *HistoryIterator1) advanceInDb() {
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

func (hi *HistoryIterator1) advance() {
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

func (hi *HistoryIterator1) HasNext() bool {
	return hi.hasNextInFiles || hi.hasNextInDb || hi.nextKey != nil
}

func (hi *HistoryIterator1) Next(keyBuf, valBuf []byte) ([]byte, []byte) {
	k := append(keyBuf, hi.nextKey...)
	v := append(valBuf, hi.nextVal...)
	hi.advance()
	return k, v
}

func (h *History) DisableReadAhead() {
	h.InvertedIndex.DisableReadAhead()
	h.files.Ascend(func(item *filesItem) bool {
		item.decompressor.DisableReadAhead()
		if item.index != nil {
			item.index.DisableReadAhead()
		}
		return true
	})
}

func (h *History) EnableReadAhead() *History {
	h.InvertedIndex.EnableReadAhead()
	h.files.Ascend(func(item *filesItem) bool {
		item.decompressor.EnableReadAhead()
		if item.index != nil {
			item.index.EnableReadAhead()
		}
		return true
	})
	return h
}
func (h *History) EnableMadvWillNeed() *History {
	h.InvertedIndex.EnableMadvWillNeed()
	h.files.Ascend(func(item *filesItem) bool {
		item.decompressor.EnableWillNeed()
		if item.index != nil {
			item.index.EnableWillNeed()
		}
		return true
	})
	return h
}
func (h *History) EnableMadvNormalReadAhead() *History {
	h.InvertedIndex.EnableMadvNormalReadAhead()
	h.files.Ascend(func(item *filesItem) bool {
		item.decompressor.EnableMadvNormal()
		if item.index != nil {
			item.index.EnableMadvNormal()
		}
		return true
	})
	return h
}
