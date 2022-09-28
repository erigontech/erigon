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

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

type History struct {
	*InvertedIndex
	historyValsTable string // key1+key2+txnNum -> oldValue , stores values BEFORE change
	settingsTable    string
	files            *btree.BTreeG[*filesItem]
	compressVals     bool
	workers          int
}

func NewHistory(
	dir string,
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
	h.InvertedIndex, err = NewInvertedIndex(dir, aggregationStep, filenameBase, indexKeysTable, indexTable)
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
	re := regexp.MustCompile("^" + h.filenameBase + ".([0-9]+)-([0-9]+).(v|vi)$")
	var err error
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}

		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				log.Warn("File ignored by inverted index scan, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startTxNum, endTxNum uint64
		if startTxNum, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endTxNum, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startTxNum > endTxNum {
			log.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}
		var item = &filesItem{startTxNum: startTxNum * h.aggregationStep, endTxNum: endTxNum * h.aggregationStep}
		var foundI *filesItem
		h.files.AscendGreaterOrEqual(&filesItem{startTxNum: endTxNum * h.aggregationStep, endTxNum: endTxNum * h.aggregationStep}, func(it *filesItem) bool {
			if it.endTxNum == endTxNum {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startTxNum > startTxNum {
			//log.Info("Load state file", "name", name, "startTxNum", startTxNum*ii.aggregationStep, "endTxNum", endTxNum*ii.aggregationStep)
			h.files.ReplaceOrInsert(item)
		}
	}
}

func (h *History) openFiles() error {
	var totalKeys uint64
	var err error

	invalidFileItems := make([]*filesItem, 0)
	h.files.Ascend(func(item *filesItem) bool {
		datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep))
		if fi, err := os.Stat(datPath); err != nil || fi.IsDir() {
			invalidFileItems = append(invalidFileItems, item)
			return true
		}
		if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			return false
		}
		idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep))

		if fi, err := os.Stat(idxPath); err != nil || fi.IsDir() {
			invalidFileItems = append(invalidFileItems, item)
			return true
		}

		//if !dir.Exist(idxPath) {
		//	if _, err = buildIndex(item.decompressor, idxPath, h.dir, item.decompressor.Count()/2, false /* values */); err != nil {
		//		return false
		//	}
		//}

		if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
			return false
		}
		totalKeys += item.index.KeyCount()
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

func (h *History) BuildMissedIndices() (err error) {
	if err := h.InvertedIndex.BuildMissedIndices(); err != nil {
		return err
	}
	//TODO: build .vi
	return nil
}

func (h *History) AddPrevValue(key1, key2, original []byte) error {
	lk := len(key1) + len(key2)
	historyKey := make([]byte, lk+8)
	copy(historyKey, key1)
	if len(key2) > 0 {
		copy(historyKey[len(key1):], key2)
	}
	if len(original) > 0 {
		val, err := h.tx.GetOne(h.settingsTable, historyValCountKey)
		if err != nil {
			return err
		}
		var valNum uint64
		if len(val) > 0 {
			valNum = binary.BigEndian.Uint64(val)
		}
		valNum++
		binary.BigEndian.PutUint64(historyKey[lk:], valNum)
		if err = h.tx.Put(h.settingsTable, historyValCountKey, historyKey[lk:]); err != nil {
			return err
		}
		if err = h.tx.Put(h.historyValsTable, historyKey[lk:], original); err != nil {
			return err
		}
	}
	if err := h.InvertedIndex.add(historyKey, historyKey[:lk]); err != nil {
		return err
	}
	return nil
}

type HistoryCollation struct {
	historyPath  string
	historyComp  *compress.Compressor
	historyCount int
	indexBitmaps map[string]*roaring64.Bitmap
}

func (c HistoryCollation) Close() {
	if c.historyComp != nil {
		c.historyComp.Close()
	}
}

func (h *History) collate(step, txFrom, txTo uint64, roTx kv.Tx) (HistoryCollation, error) {
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
	if historyComp, err = compress.NewCompressor(context.Background(), "collate history", historyPath, h.dir, compress.MinPatternScore, h.workers, log.LvlDebug); err != nil {
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
			bitmap = roaring64.New()
			indexBitmaps[string(v[:len(v)-8])] = bitmap
		}
		bitmap.Add(txNum)
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
			if bytes.HasPrefix(v, []byte(key)) {
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
func (h *History) buildFiles(step uint64, collation HistoryCollation) (HistoryFiles, error) {
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
	efHistoryComp, err = compress.NewCompressor(context.Background(), "ef history", efHistoryPath, h.dir, compress.MinPatternScore, h.workers, log.LvlDebug)
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
	if efHistoryIdx, err = buildIndex(efHistoryDecomp, efHistoryIdxPath, h.dir, len(keys), false /* values */); err != nil {
		return HistoryFiles{}, fmt.Errorf("build %s ef history idx: %w", h.filenameBase, err)
	}
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   collation.historyCount,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.dir,
		IndexFile:  historyIdxPath,
	}); err != nil {
		return HistoryFiles{}, fmt.Errorf("create recsplit: %w", err)
	}
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

// [txFrom; txTo)
func (h *History) prune(step uint64, txFrom, txTo uint64) error {
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
		if err = valsC.Delete(v[len(v)-8:]); err != nil {
			return err
		}
		if err = idxC.DeleteExact(v[:len(v)-8], k); err != nil {
			return err
		}
		// This DeleteCurrent needs to the the last in the loop iteration, because it invalidates k and v
		if err = historyKeysCursor.DeleteCurrent(); err != nil {
			return err
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
		// This DeleteCurrent needs to the the last in the loop iteration, because it invalidates k and v
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
	//hc.indexFiles.Ascend(func(item *ctxItem) bool {
	hc.indexFiles.AscendGreaterOrEqual(ctxItem{startTxNum: txNum, endTxNum: txNum}, func(item ctxItem) bool {
		//fmt.Printf("ef item %d-%d, key %x\n", item.startTxNum, item.endTxNum, key)
		if item.reader.Empty() {
			return true
		}
		offset := item.reader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		if k, _ := g.NextUncompressed(); bytes.Equal(k, key) {
			//fmt.Printf("Found key=%x\n", k)
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			if n, ok := ef.Search(txNum); ok {
				foundTxNum = n
				foundEndTxNum = item.endTxNum
				foundStartTxNum = item.startTxNum
				found = true
				//fmt.Printf("Found n=%d\n", n)
				return false
			}
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
	return hc.getNoStateFromDB(key, txNum, roTx)
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
	hc           *HistoryContext
	compressVals bool
	total        uint64

	hasNextInFiles                      bool
	hasNextInDb                         bool
	startTxKey, txnKey                  [8]byte
	startTxNum, endTxNum                uint64
	roTx                                kv.Tx
	idxCursor, txNum2kCursor            kv.CursorDupSort
	indexTable, idxKeysTable, valsTable string
	h                                   ReconHeap

	nextKey, nextVal, nextFileKey, nextFileVal, nextDbKey, nextDbVal []byte
	advFileCnt, advDbCnt                                             int
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
