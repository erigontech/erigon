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
	"os"
	"path/filepath"
	"strings"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func (d *Domain) endTxNumMinimax() uint64 {
	minimax := d.History.endTxNumMinimax()
	if max, ok := d.files.Max(); ok {
		endTxNum := max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

func (ii *InvertedIndex) endTxNumMinimax() uint64 {
	var minimax uint64
	if max, ok := ii.files.Max(); ok {
		endTxNum := max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

func (h *History) endTxNumMinimax() uint64 {
	minimax := h.InvertedIndex.endTxNumMinimax()
	if max, ok := h.files.Max(); ok {
		endTxNum := max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

type DomainRanges struct {
	valuesStartTxNum  uint64
	valuesEndTxNum    uint64
	historyStartTxNum uint64
	historyEndTxNum   uint64
	indexStartTxNum   uint64
	indexEndTxNum     uint64
	values            bool
	history           bool
	index             bool
}

func (r DomainRanges) any() bool {
	return r.values || r.history || r.index
}

// findMergeRange assumes that all fTypes in d.files have items at least as far as maxEndTxNum
// That is why only Values type is inspected
func (d *Domain) findMergeRange(maxEndTxNum, maxSpan uint64) DomainRanges {
	hr := d.History.findMergeRange(maxEndTxNum, maxSpan)
	r := DomainRanges{
		historyStartTxNum: hr.historyStartTxNum,
		historyEndTxNum:   hr.historyEndTxNum,
		history:           hr.history,
		indexStartTxNum:   hr.indexStartTxNum,
		indexEndTxNum:     hr.indexEndTxNum,
		index:             hr.index,
	}
	d.files.Ascend(func(item *filesItem) bool {
		if item.endTxNum > maxEndTxNum {
			return false
		}
		endStep := item.endTxNum / d.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * d.aggregationStep
		start := item.endTxNum - span
		if start < item.startTxNum {
			if !r.values || start < r.valuesStartTxNum {
				r.values = true
				r.valuesStartTxNum = start
				r.valuesEndTxNum = item.endTxNum
			}
		}
		return true
	})
	return r
}

func (ii *InvertedIndex) findMergeRange(maxEndTxNum, maxSpan uint64) (bool, uint64, uint64) {
	var minFound bool
	var startTxNum, endTxNum uint64
	ii.files.Ascend(func(item *filesItem) bool {
		if item.endTxNum > maxEndTxNum {
			return false
		}
		endStep := item.endTxNum / ii.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * ii.aggregationStep
		if span > maxSpan {
			span = maxSpan
		}
		start := item.endTxNum - span
		if start < item.startTxNum {
			if !minFound || start < startTxNum {
				minFound = true
				startTxNum = start
				endTxNum = item.endTxNum
			}
		}
		return true
	})
	return minFound, startTxNum, endTxNum
}

type HistoryRanges struct {
	historyStartTxNum uint64
	historyEndTxNum   uint64
	indexStartTxNum   uint64
	indexEndTxNum     uint64
	history           bool
	index             bool
}

func (r HistoryRanges) String(aggStep uint64) string {
	var str string
	if r.history {
		str += fmt.Sprintf("hist: %d-%d, ", r.historyStartTxNum/aggStep, r.historyEndTxNum/aggStep)
	}
	if r.index {
		str += fmt.Sprintf("idx: %d-%d", r.indexStartTxNum/aggStep, r.indexEndTxNum/aggStep)
	}
	return str
}
func (r HistoryRanges) any() bool {
	return r.history || r.index
}

func (h *History) findMergeRange(maxEndTxNum, maxSpan uint64) HistoryRanges {
	var r HistoryRanges
	r.index, r.indexStartTxNum, r.indexEndTxNum = h.InvertedIndex.findMergeRange(maxEndTxNum, maxSpan)
	h.files.Ascend(func(item *filesItem) bool {
		if item.endTxNum > maxEndTxNum {
			return false
		}
		endStep := item.endTxNum / h.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * h.aggregationStep
		if span > maxSpan {
			span = maxSpan
		}
		start := item.endTxNum - span
		if start < item.startTxNum {
			if !r.history || start < r.historyStartTxNum {
				r.history = true
				r.historyStartTxNum = start
				r.historyEndTxNum = item.endTxNum
			}
		}
		return true
	})
	return r
}

// staticFilesInRange returns list of static files with txNum in specified range [startTxNum; endTxNum)
// files are in the descending order of endTxNum
func (d *Domain) staticFilesInRange(r DomainRanges) (valuesFiles, indexFiles, historyFiles []*filesItem, startJ int) {
	if r.index || r.history {
		indexFiles, historyFiles, startJ = d.History.staticFilesInRange(HistoryRanges{
			historyStartTxNum: r.historyStartTxNum,
			historyEndTxNum:   r.historyEndTxNum,
			history:           r.history,
			indexStartTxNum:   r.indexStartTxNum,
			indexEndTxNum:     r.indexEndTxNum,
			index:             r.index,
		})
	}
	if r.values {
		d.files.Ascend(func(item *filesItem) bool {
			if item.startTxNum < r.valuesStartTxNum {
				startJ++
				return true
			}
			if item.endTxNum > r.valuesEndTxNum {
				return false
			}
			valuesFiles = append(valuesFiles, item)
			return true
		})
		for _, f := range valuesFiles {
			if f == nil {
				panic("must not happen")
			}
		}
	}
	return
}

func (ii *InvertedIndex) staticFilesInRange(startTxNum, endTxNum uint64) ([]*filesItem, int) {
	var files []*filesItem
	var startJ int
	ii.files.Ascend(func(item *filesItem) bool {
		if item.startTxNum < startTxNum {
			startJ++
			return true
		}
		if item.endTxNum > endTxNum {
			return false
		}
		files = append(files, item)
		return true
	})
	for _, f := range files {
		if f == nil {
			panic("must not happen")
		}
	}

	return files, startJ
}

func (h *History) staticFilesInRange(r HistoryRanges) (indexFiles, historyFiles []*filesItem, startJ int) {
	if r.index {
		indexFiles, startJ = h.InvertedIndex.staticFilesInRange(r.indexStartTxNum, r.indexEndTxNum)
	}
	if r.history {
		startJ = 0
		h.files.Ascend(func(item *filesItem) bool {
			if item.startTxNum < r.historyStartTxNum {
				startJ++
				return true
			}
			if item.endTxNum > r.historyEndTxNum {
				return false
			}
			historyFiles = append(historyFiles, item)
			return true
		})

		for _, f := range historyFiles {
			if f == nil {
				panic("must not happen")
			}
		}
		if r.index && len(indexFiles) != len(historyFiles) {
			var sIdx, sHist []string
			for _, f := range indexFiles {
				_, fName := filepath.Split(f.index.FilePath())
				sIdx = append(sIdx, fmt.Sprintf("%+v", fName))
			}
			for _, f := range historyFiles {
				_, fName := filepath.Split(f.decompressor.FilePath())
				sHist = append(sHist, fmt.Sprintf("%+v", fName))
			}
			log.Warn("something wrong with files for merge", "idx", strings.Join(sIdx, ","), "hist", strings.Join(sHist, ","))
		}
	}
	return
}

func mergeEfs(preval, val, buf []byte) ([]byte, error) {
	preef, _ := eliasfano32.ReadEliasFano(preval)
	ef, _ := eliasfano32.ReadEliasFano(val)
	preIt := preef.Iterator()
	efIt := ef.Iterator()
	newEf := eliasfano32.NewEliasFano(preef.Count()+ef.Count(), ef.Max())
	for preIt.HasNext() {
		newEf.AddOffset(preIt.Next())
	}
	for efIt.HasNext() {
		newEf.AddOffset(efIt.Next())
	}
	newEf.Build()
	return newEf.AppendBytes(buf), nil
}

func (d *Domain) mergeFiles(ctx context.Context, valuesFiles, indexFiles, historyFiles []*filesItem, r DomainRanges, workers int) (valuesIn, indexIn, historyIn *filesItem, err error) {
	if !r.any() {
		return
	}
	var comp *compress.Compressor
	var decomp *compress.Decompressor
	var closeItem bool = true
	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if decomp != nil {
				decomp.Close()
			}
			if indexIn != nil {
				if indexIn.decompressor != nil {
					indexIn.decompressor.Close()
				}
				if indexIn.index != nil {
					indexIn.index.Close()
				}
			}
			if historyIn != nil {
				if historyIn.decompressor != nil {
					historyIn.decompressor.Close()
				}
				if historyIn.index != nil {
					historyIn.index.Close()
				}
			}
			if valuesIn != nil {
				if valuesIn.decompressor != nil {
					valuesIn.decompressor.Close()
				}
				if valuesIn.index != nil {
					valuesIn.index.Close()
				}
			}
		}
	}()
	if indexIn, historyIn, err = d.History.mergeFiles(ctx, indexFiles, historyFiles,
		HistoryRanges{
			historyStartTxNum: r.historyStartTxNum,
			historyEndTxNum:   r.historyEndTxNum,
			history:           r.history,
			indexStartTxNum:   r.indexStartTxNum,
			indexEndTxNum:     r.indexEndTxNum,
			index:             r.index}, workers); err != nil {
		return nil, nil, nil, err
	}
	if r.values {
		log.Info(fmt.Sprintf("[snapshots] merge: %s.%d-%d.kv", d.filenameBase, r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep))
		for _, f := range valuesFiles {
			defer f.decompressor.EnableMadvNormal().DisableReadAhead()
		}

		datPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep))
		if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, d.tmpdir, compress.MinPatternScore, workers, log.LvlTrace); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s history compressor: %w", d.filenameBase, err)
		}
		var cp CursorHeap
		heap.Init(&cp)
		for _, item := range valuesFiles {
			g := item.decompressor.MakeGetter()
			g.Reset(0)
			if g.HasNext() {
				key, _ := g.NextUncompressed()
				var val []byte
				if d.compressVals {
					val, _ = g.Next(nil)
				} else {
					val, _ = g.NextUncompressed()
				}
				heap.Push(&cp, &CursorItem{
					t:        FILE_CURSOR,
					dg:       g,
					key:      key,
					val:      val,
					endTxNum: item.endTxNum,
					reverse:  true,
				})
			}
		}
		count := 0
		// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
		// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
		// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
		// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
		// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
		var keyBuf, valBuf []byte
		for cp.Len() > 0 {
			lastKey := common.Copy(cp[0].key)
			lastVal := common.Copy(cp[0].val)
			// Advance all the items that have this key (including the top)
			for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
				ci1 := cp[0]
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.NextUncompressed()
					if d.compressVals {
						ci1.val, _ = ci1.dg.Next(ci1.val[:0])
					} else {
						ci1.val, _ = ci1.dg.NextUncompressed()
					}
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			}
			var skip bool
			if d.prefixLen > 0 {
				skip = r.valuesStartTxNum == 0 && len(lastVal) == 0 && len(lastKey) != d.prefixLen
			} else {
				// For the rest of types, empty value means deletion
				skip = r.valuesStartTxNum == 0 && len(lastVal) == 0
			}
			if !skip {
				if keyBuf != nil && (d.prefixLen == 0 || len(keyBuf) != d.prefixLen || bytes.HasPrefix(lastKey, keyBuf)) {
					if err = comp.AddUncompressedWord(keyBuf); err != nil {
						return nil, nil, nil, err
					}
					count++ // Only counting keys, not values

					//if d.valueMergeFn != nil {
					//	valBuf, err = d.valueMergeFn(valBuf, nil)
					//	if err != nil {
					//		return nil, nil, nil, err
					//	}
					//}
					if d.compressVals {
						if err = comp.AddWord(valBuf); err != nil {
							return nil, nil, nil, err
						}
					} else {
						if err = comp.AddUncompressedWord(valBuf); err != nil {
							return nil, nil, nil, err
						}
					}
				}
				keyBuf = append(keyBuf[:0], lastKey...)
				valBuf = append(valBuf[:0], lastVal...)
			}
		}
		if keyBuf != nil {
			if err = comp.AddUncompressedWord(keyBuf); err != nil {
				return nil, nil, nil, err
			}
			count++ // Only counting keys, not values
			if d.compressVals {
				if err = comp.AddWord(valBuf); err != nil {
					return nil, nil, nil, err
				}
			} else {
				if err = comp.AddUncompressedWord(valBuf); err != nil {
					return nil, nil, nil, err
				}
			}
		}
		if err = comp.Compress(); err != nil {
			return nil, nil, nil, err
		}
		comp.Close()
		comp = nil
		idxPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep))
		valuesIn = &filesItem{startTxNum: r.valuesStartTxNum, endTxNum: r.valuesEndTxNum}
		if valuesIn.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}
		if valuesIn.index, err = buildIndex(ctx, valuesIn.decompressor, idxPath, d.tmpdir, count, false /* values */); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}
	}
	closeItem = false
	d.stats.MergesCount++
	d.mergesCount++
	return
}

//func (d *Domain) SetValueMergeStrategy(merge func([]byte, []byte) ([]byte, error)) {
//	d.valueMergeFn = merge
//}

func (ii *InvertedIndex) mergeFiles(ctx context.Context, files []*filesItem, startTxNum, endTxNum uint64, workers int) (*filesItem, error) {
	for _, h := range files {
		defer h.decompressor.EnableMadvNormal().DisableReadAhead()
	}
	log.Info(fmt.Sprintf("[snapshots] merge: %s.%d-%d.ef", ii.filenameBase, startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep))

	var outItem *filesItem
	var comp *compress.Compressor
	var decomp *compress.Decompressor
	var err error
	var closeItem bool = true
	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if decomp != nil {
				decomp.Close()
			}
			if outItem != nil {
				if outItem.decompressor != nil {
					outItem.decompressor.Close()
				}
				if outItem.index != nil {
					outItem.index.Close()
				}
				outItem = nil
			}
		}
	}()
	datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep))
	if comp, err = compress.NewCompressor(ctx, "Snapshots merge", datPath, ii.tmpdir, compress.MinPatternScore, workers, log.LvlTrace); err != nil {
		return nil, fmt.Errorf("merge %s inverted index compressor: %w", ii.filenameBase, err)
	}
	var cp CursorHeap
	heap.Init(&cp)
	for _, item := range files {
		g := item.decompressor.MakeGetter()
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
			//fmt.Printf("heap push %s [%d] %x\n", item.decompressor.FilePath(), item.endTxNum, key)
			heap.Push(&cp, &CursorItem{
				t:        FILE_CURSOR,
				dg:       g,
				key:      key,
				val:      val,
				endTxNum: item.endTxNum,
				reverse:  true,
			})
		}
	}
	keyCount := 0

	// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
	// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
	// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
	// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
	// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
	var keyBuf, valBuf []byte
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		var mergedOnce bool

		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			if mergedOnce {
				if lastVal, err = mergeEfs(ci1.val, lastVal, nil); err != nil {
					return nil, fmt.Errorf("merge %s inverted index: %w", ii.filenameBase, err)
				}
			} else {
				mergedOnce = true
			}
			//fmt.Printf("multi-way %s [%d] %x\n", ii.indexKeysTable, ci1.endTxNum, ci1.key)
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.NextUncompressed()
				ci1.val, _ = ci1.dg.NextUncompressed()
				//fmt.Printf("heap next push %s [%d] %x\n", ii.indexKeysTable, ci1.endTxNum, ci1.key)
				heap.Fix(&cp, 0)
			} else {
				heap.Pop(&cp)
			}
		}
		if keyBuf != nil {
			if err = comp.AddUncompressedWord(keyBuf); err != nil {
				return nil, err
			}
			keyCount++ // Only counting keys, not values
			if err = comp.AddUncompressedWord(valBuf); err != nil {
				return nil, err
			}
		}
		keyBuf = append(keyBuf[:0], lastKey...)
		valBuf = append(valBuf[:0], lastVal...)
	}
	if keyBuf != nil {
		if err = comp.AddUncompressedWord(keyBuf); err != nil {
			return nil, err
		}
		keyCount++ // Only counting keys, not values
		if err = comp.AddUncompressedWord(valBuf); err != nil {
			return nil, err
		}
	}
	if err = comp.Compress(); err != nil {
		return nil, err
	}
	comp.Close()
	comp = nil
	idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep))
	outItem = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
	if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
		return nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	if outItem.index, err = buildIndex(ctx, outItem.decompressor, idxPath, ii.tmpdir, keyCount, false /* values */); err != nil {
		return nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	closeItem = false
	return outItem, nil
}

func (h *History) mergeFiles(ctx context.Context, indexFiles, historyFiles []*filesItem, r HistoryRanges, workers int) (indexIn, historyIn *filesItem, err error) {
	if !r.any() {
		return nil, nil, nil
	}
	var closeIndex = true
	defer func() {
		if closeIndex {
			if indexIn != nil {
				indexIn.decompressor.Close()
				indexIn.index.Close()
			}
		}
	}()
	if indexIn, err = h.InvertedIndex.mergeFiles(ctx, indexFiles, r.indexStartTxNum, r.indexEndTxNum, workers); err != nil {
		return nil, nil, err
	}
	if r.history {
		log.Info(fmt.Sprintf("[snapshots] merge: %s.%d-%d.v", h.filenameBase, r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep))
		for _, f := range indexFiles {
			defer f.decompressor.EnableMadvNormal().DisableReadAhead()
		}
		for _, f := range historyFiles {
			defer f.decompressor.EnableMadvNormal().DisableReadAhead()
		}

		var comp *compress.Compressor
		var decomp *compress.Decompressor
		var rs *recsplit.RecSplit
		var index *recsplit.Index
		var closeItem bool = true
		defer func() {
			if closeItem {
				if comp != nil {
					comp.Close()
				}
				if decomp != nil {
					decomp.Close()
				}
				if rs != nil {
					rs.Close()
				}
				if index != nil {
					index.Close()
				}
				if historyIn != nil {
					if historyIn.decompressor != nil {
						historyIn.decompressor.Close()
					}
					if historyIn.index != nil {
						historyIn.index.Close()
					}
				}
			}
		}()
		datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep))
		idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep))
		if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, h.tmpdir, compress.MinPatternScore, workers, log.LvlTrace); err != nil {
			return nil, nil, fmt.Errorf("merge %s history compressor: %w", h.filenameBase, err)
		}
		var cp CursorHeap
		heap.Init(&cp)
		for i, item := range indexFiles {
			g := item.decompressor.MakeGetter()
			g.Reset(0)
			if g.HasNext() {
				g2 := historyFiles[i].decompressor.MakeGetter()
				key, _ := g.NextUncompressed()
				val, _ := g.NextUncompressed()
				heap.Push(&cp, &CursorItem{
					t:        FILE_CURSOR,
					dg:       g,
					dg2:      g2,
					key:      key,
					val:      val,
					endTxNum: item.endTxNum,
					reverse:  false,
				})
			}
		}
		// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
		// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
		// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
		// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
		// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
		var valBuf []byte
		var keyCount int
		for cp.Len() > 0 {
			lastKey := common.Copy(cp[0].key)
			// Advance all the items that have this key (including the top)
			//var mergeOnce bool
			for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
				ci1 := cp[0]

				//if h.valueMergeFn != nil && mergeOnce {
				//	valBuf, err = h.valueMergeFn(ci1.val, valBuf)
				//	if err != nil {
				//		return nil, nil, err
				//	}
				//	ci1.val = valBuf
				//}
				//if !mergeOnce {
				//	mergeOnce = true
				//}
				ef, _ := eliasfano32.ReadEliasFano(ci1.val)
				for i := uint64(0); i < ef.Count(); i++ {
					if h.compressVals {
						valBuf, _ = ci1.dg2.Next(valBuf[:0])
						if err = comp.AddWord(valBuf); err != nil {
							return nil, nil, err
						}
					} else {
						valBuf, _ = ci1.dg2.NextUncompressed()
						if err = comp.AddUncompressedWord(valBuf); err != nil {
							return nil, nil, err
						}
					}
				}
				keyCount += int(ef.Count())
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.NextUncompressed()
					ci1.val, _ = ci1.dg.NextUncompressed()
					heap.Fix(&cp, 0)
				} else {
					heap.Remove(&cp, 0)
				}
			}
		}
		if err = comp.Compress(); err != nil {
			return nil, nil, err
		}
		comp.Close()
		comp = nil
		if decomp, err = compress.NewDecompressor(datPath); err != nil {
			return nil, nil, err
		}
		if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   keyCount,
			Enums:      false,
			BucketSize: 2000,
			LeafSize:   8,
			TmpDir:     h.tmpdir,
			IndexFile:  idxPath,
		}); err != nil {
			return nil, nil, fmt.Errorf("create recsplit: %w", err)
		}
		rs.LogLvl(log.LvlTrace)
		var historyKey []byte
		var txKey [8]byte
		var valOffset uint64
		g := indexIn.decompressor.MakeGetter()
		g2 := decomp.MakeGetter()
		var keyBuf []byte
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
						return nil, nil, err
					}
					if h.compressVals {
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
					return nil, nil, fmt.Errorf("build %s idx: %w", h.filenameBase, err)
				}
			} else {
				break
			}
		}
		rs.Close()
		rs = nil
		if index, err = recsplit.OpenIndex(idxPath); err != nil {
			return nil, nil, fmt.Errorf("open %s idx: %w", h.filenameBase, err)
		}
		historyIn = &filesItem{startTxNum: r.historyStartTxNum, endTxNum: r.historyEndTxNum, decompressor: decomp, index: index}
		closeItem = false
	}
	closeIndex = false
	return
}

func (d *Domain) integrateMergedFiles(valuesOuts, indexOuts, historyOuts []*filesItem, valuesIn, indexIn, historyIn *filesItem) {
	if valuesIn == nil {
		return
	}
	d.History.integrateMergedFiles(indexOuts, historyOuts, indexIn, historyIn)
	d.files.ReplaceOrInsert(valuesIn)
	for _, out := range valuesOuts {
		if out == nil {
			panic("must not happen")
		}
		d.files.Delete(out)
		out.decompressor.Close()
		out.index.Close()
	}
}

func (ii *InvertedIndex) integrateMergedFiles(outs []*filesItem, in *filesItem) {
	if in == nil {
		return
	}
	ii.files.ReplaceOrInsert(in)
	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + ii.filenameBase)
		}
		ii.files.Delete(out)
		out.decompressor.Close()
		out.index.Close()
	}
}

func (h *History) integrateMergedFiles(indexOuts, historyOuts []*filesItem, indexIn, historyIn *filesItem) {
	if historyIn == nil {
		return
	}
	h.InvertedIndex.integrateMergedFiles(indexOuts, indexIn)
	h.files.ReplaceOrInsert(historyIn)
	for _, out := range historyOuts {
		if out == nil {
			panic("must not happen: " + h.filenameBase)
		}
		h.files.Delete(out)
		out.decompressor.Close()
		out.index.Close()
	}
}

func (d *Domain) deleteFiles(valuesOuts, indexOuts, historyOuts []*filesItem) error {
	if err := d.History.deleteFiles(indexOuts, historyOuts); err != nil {
		return err
	}
	for _, out := range valuesOuts {
		datPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, out.startTxNum/d.aggregationStep, out.endTxNum/d.aggregationStep))
		if err := os.Remove(datPath); err != nil {
			return err
		}
		idxPath := filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, out.startTxNum/d.aggregationStep, out.endTxNum/d.aggregationStep))
		_ = os.Remove(idxPath) // may not exist
	}
	return nil
}

func (ii *InvertedIndex) deleteFiles(outs []*filesItem) error {
	for _, out := range outs {
		datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, out.startTxNum/ii.aggregationStep, out.endTxNum/ii.aggregationStep))
		if err := os.Remove(datPath); err != nil {
			return err
		}
		idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, out.startTxNum/ii.aggregationStep, out.endTxNum/ii.aggregationStep))
		_ = os.Remove(idxPath) // may not exist
	}
	return nil
}

func (h *History) deleteFiles(indexOuts, historyOuts []*filesItem) error {
	if err := h.InvertedIndex.deleteFiles(indexOuts); err != nil {
		return err
	}
	for _, out := range historyOuts {
		datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, out.startTxNum/h.aggregationStep, out.endTxNum/h.aggregationStep))
		if err := os.Remove(datPath); err != nil {
			return err
		}
		idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, out.startTxNum/h.aggregationStep, out.endTxNum/h.aggregationStep))
		_ = os.Remove(idxPath) // may not exist
	}
	return nil
}
