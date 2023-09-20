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

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
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
func (ii *InvertedIndex) endIndexedTxNumMinimax(needFrozen bool) uint64 {
	var max uint64
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || (needFrozen && !item.frozen) {
				continue
			}
			max = cmp.Max(max, item.endTxNum)
		}
		return true
	})
	return max
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
func (h *History) endIndexedTxNumMinimax(needFrozen bool) uint64 {
	var max uint64
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || (needFrozen && !item.frozen) {
				continue
			}
			max = cmp.Max(max, item.endTxNum)
		}
		return true
	})
	return cmp.Min(max, h.InvertedIndex.endIndexedTxNumMinimax(needFrozen))
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

func (r DomainRanges) String() string {
	var b strings.Builder
	if r.values {
		b.WriteString(fmt.Sprintf("Values: [%d, %d)", r.valuesStartTxNum, r.valuesEndTxNum))
	}
	if r.history {
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("History: [%d, %d)", r.historyStartTxNum, r.historyEndTxNum))
	}
	if r.index {
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("Index: [%d, %d)", r.indexStartTxNum, r.indexEndTxNum))
	}
	return b.String()
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
	d.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.endTxNum > maxEndTxNum {
				return false
			}
			endStep := item.endTxNum / d.aggregationStep
			spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
			span := cmp.Min(spanStep*d.aggregationStep, maxSpan)
			start := item.endTxNum - span
			if start < item.startTxNum {
				if !r.values || start < r.valuesStartTxNum {
					r.values = true
					r.valuesStartTxNum = start
					r.valuesEndTxNum = item.endTxNum
				}
			}
		}
		return true
	})
	return r
}

// 0-1,1-2,2-3,3-4: allow merge 0-1
// 0-2,2-3,3-4: allow merge 0-4
// 0-2,2-4: allow merge 0-4
//
// 0-1,1-2,2-3: allow merge 0-2
//
// 0-2,2-3: nothing to merge
func (ii *InvertedIndex) findMergeRange(maxEndTxNum, maxSpan uint64) (bool, uint64, uint64) {
	var minFound bool
	var startTxNum, endTxNum uint64
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.endTxNum > maxEndTxNum {
				continue
			}
			endStep := item.endTxNum / ii.aggregationStep
			spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
			span := cmp.Min(spanStep*ii.aggregationStep, maxSpan)
			start := item.endTxNum - span
			foundSuperSet := startTxNum == item.startTxNum && item.endTxNum >= endTxNum
			if foundSuperSet {
				minFound = false
				startTxNum = start
				endTxNum = item.endTxNum
			} else if start < item.startTxNum {
				if !minFound || start < startTxNum {
					minFound = true
					startTxNum = start
					endTxNum = item.endTxNum
				}
			}
		}
		return true
	})
	return minFound, startTxNum, endTxNum
}

func (ii *InvertedIndex) mergeRangesUpTo(ctx context.Context, maxTxNum, maxSpan uint64, workers int, ictx *InvertedIndexContext, ps *background.ProgressSet) (err error) {
	closeAll := true
	for updated, startTx, endTx := ii.findMergeRange(maxSpan, maxTxNum); updated; updated, startTx, endTx = ii.findMergeRange(maxTxNum, maxSpan) {
		staticFiles, _ := ictx.staticFilesInRange(startTx, endTx)
		defer func() {
			if closeAll {
				for _, i := range staticFiles {
					i.decompressor.Close()
					i.index.Close()
				}
			}
		}()

		mergedIndex, err := ii.mergeFiles(ctx, staticFiles, startTx, endTx, workers, ps)
		if err != nil {
			return err
		}
		defer func() {
			if closeAll {
				mergedIndex.decompressor.Close()
				mergedIndex.index.Close()
			}
		}()

		ii.integrateMergedFiles(staticFiles, mergedIndex)
		if mergedIndex.frozen {
			ii.cleanAfterFreeze(mergedIndex.endTxNum)
		}
	}
	closeAll = false
	return nil
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
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.endTxNum > maxEndTxNum {
				continue
			}
			endStep := item.endTxNum / h.aggregationStep
			spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
			span := cmp.Min(spanStep*h.aggregationStep, maxSpan)
			start := item.endTxNum - span
			foundSuperSet := r.indexStartTxNum == item.startTxNum && item.endTxNum >= r.historyEndTxNum
			if foundSuperSet {
				r.history = false
				r.historyStartTxNum = start
				r.historyEndTxNum = item.endTxNum
			} else if start < item.startTxNum {
				if !r.history || start < r.historyStartTxNum {
					r.history = true
					r.historyStartTxNum = start
					r.historyEndTxNum = item.endTxNum
				}
			}
		}
		return true
	})

	if r.history && r.index {
		// history is behind idx: then merge only history
		historyIsAgead := r.historyEndTxNum > r.indexEndTxNum
		if historyIsAgead {
			r.history, r.historyStartTxNum, r.historyEndTxNum = false, 0, 0
			return r
		}

		historyIsBehind := r.historyEndTxNum < r.indexEndTxNum
		if historyIsBehind {
			r.index, r.indexStartTxNum, r.indexEndTxNum = false, 0, 0
			return r
		}
	}
	return r
}

// staticFilesInRange returns list of static files with txNum in specified range [startTxNum; endTxNum)
// files are in the descending order of endTxNum
func (dc *DomainContext) staticFilesInRange(r DomainRanges) (valuesFiles, indexFiles, historyFiles []*filesItem, startJ int) {
	if r.index || r.history {
		var err error
		indexFiles, historyFiles, startJ, err = dc.hc.staticFilesInRange(HistoryRanges{
			historyStartTxNum: r.historyStartTxNum,
			historyEndTxNum:   r.historyEndTxNum,
			history:           r.history,
			indexStartTxNum:   r.indexStartTxNum,
			indexEndTxNum:     r.indexEndTxNum,
			index:             r.index,
		})
		if err != nil {
			panic(err)
		}
	}
	if r.values {
		for _, item := range dc.files {
			if item.startTxNum < r.valuesStartTxNum {
				startJ++
				continue
			}
			if item.endTxNum > r.valuesEndTxNum {
				break
			}
			valuesFiles = append(valuesFiles, item.src)
		}
		for _, f := range valuesFiles {
			if f == nil {
				panic("must not happen")
			}
		}
	}
	return
}

// nolint
func (d *Domain) staticFilesInRange(r DomainRanges, dc *DomainContext) (valuesFiles, indexFiles, historyFiles []*filesItem, startJ int) {
	panic("deprecated: use DomainContext.staticFilesInRange")
}
func (ic *InvertedIndexContext) staticFilesInRange(startTxNum, endTxNum uint64) ([]*filesItem, int) {
	files := make([]*filesItem, 0, len(ic.files))
	var startJ int

	for _, item := range ic.files {
		if item.startTxNum < startTxNum {
			startJ++
			continue
		}
		if item.endTxNum > endTxNum {
			break
		}
		files = append(files, item.src)
	}
	for _, f := range files {
		if f == nil {
			panic("must not happen")
		}
	}

	return files, startJ
}

// nolint
func (ii *InvertedIndex) staticFilesInRange(startTxNum, endTxNum uint64, ic *InvertedIndexContext) ([]*filesItem, int) {
	panic("deprecated: use InvertedIndexContext.staticFilesInRange")
}

func (hc *HistoryContext) staticFilesInRange(r HistoryRanges) (indexFiles, historyFiles []*filesItem, startJ int, err error) {
	if !r.history && r.index {
		indexFiles, startJ = hc.ic.staticFilesInRange(r.indexStartTxNum, r.indexEndTxNum)
		return indexFiles, historyFiles, startJ, nil
	}

	if r.history {
		// Get history files from HistoryContext (no "garbage/overalps"), but index files not from InvertedIndexContext
		// because index files may already be merged (before `kill -9`) and it means not visible in InvertedIndexContext
		startJ = 0
		for _, item := range hc.files {
			if item.startTxNum < r.historyStartTxNum {
				startJ++
				continue
			}
			if item.endTxNum > r.historyEndTxNum {
				break
			}

			historyFiles = append(historyFiles, item.src)
			idxFile, ok := hc.h.InvertedIndex.files.Get(item.src)
			if ok {
				indexFiles = append(indexFiles, idxFile)
			} else {
				walkErr := fmt.Errorf("History.staticFilesInRange: required file not found: %s.%d-%d.efi", hc.h.filenameBase, item.startTxNum/hc.h.aggregationStep, item.endTxNum/hc.h.aggregationStep)
				return nil, nil, 0, walkErr
			}
		}

		for _, f := range historyFiles {
			if f == nil {
				panic("must not happen")
			}
		}
		if r.index && len(indexFiles) != len(historyFiles) {
			var sIdx, sHist []string
			for _, f := range indexFiles {
				if f.index != nil {
					_, fName := filepath.Split(f.index.FilePath())
					sIdx = append(sIdx, fmt.Sprintf("%+v", fName))
				}
			}
			for _, f := range historyFiles {
				if f.decompressor != nil {
					_, fName := filepath.Split(f.decompressor.FilePath())
					sHist = append(sHist, fmt.Sprintf("%+v", fName))
				}
			}
			log.Warn("[snapshots] something wrong with files for merge", "idx", strings.Join(sIdx, ","), "hist", strings.Join(sHist, ","))
		}
	}
	return
}

// nolint
func (h *History) staticFilesInRange(r HistoryRanges, hc *HistoryContext) (indexFiles, historyFiles []*filesItem, startJ int, err error) {
	panic("deprecated: use HistoryContext.staticFilesInRange")
}

func mergeEfs(preval, val, buf []byte) ([]byte, error) {
	preef, _ := eliasfano32.ReadEliasFano(preval)
	ef, _ := eliasfano32.ReadEliasFano(val)
	preIt := preef.Iterator()
	efIt := ef.Iterator()
	newEf := eliasfano32.NewEliasFano(preef.Count()+ef.Count(), ef.Max())
	for preIt.HasNext() {
		v, err := preIt.Next()
		if err != nil {
			return nil, err
		}
		newEf.AddOffset(v)
	}
	for efIt.HasNext() {
		v, err := efIt.Next()
		if err != nil {
			return nil, err
		}
		newEf.AddOffset(v)
	}
	newEf.Build()
	return newEf.AppendBytes(buf), nil
}

func (d *Domain) mergeFiles(ctx context.Context, valuesFiles, indexFiles, historyFiles []*filesItem, r DomainRanges, workers int, ps *background.ProgressSet) (valuesIn, indexIn, historyIn *filesItem, err error) {
	if !r.any() {
		return
	}
	var comp *compress.Compressor
	closeItem := true

	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if indexIn != nil {
				if indexIn.decompressor != nil {
					indexIn.decompressor.Close()
				}
				if indexIn.index != nil {
					indexIn.index.Close()
				}
				if indexIn.bindex != nil {
					indexIn.bindex.Close()
				}
			}
			if historyIn != nil {
				if historyIn.decompressor != nil {
					historyIn.decompressor.Close()
				}
				if historyIn.index != nil {
					historyIn.index.Close()
				}
				if historyIn.bindex != nil {
					historyIn.bindex.Close()
				}
			}
			if valuesIn != nil {
				if valuesIn.decompressor != nil {
					valuesIn.decompressor.Close()
				}
				if valuesIn.index != nil {
					valuesIn.index.Close()
				}
				if valuesIn.bindex != nil {
					valuesIn.bindex.Close()
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
			index:             r.index}, workers, ps); err != nil {
		return nil, nil, nil, err
	}
	if r.values {
		for _, f := range valuesFiles {
			defer f.decompressor.EnableMadvNormal().DisableReadAhead()
		}
		datFileName := fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
		datPath := filepath.Join(d.dir, datFileName)
		if comp, err = compress.NewCompressor(ctx, "merge", datPath, d.tmpdir, compress.MinPatternScore, workers, log.LvlTrace, d.logger); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s history compressor: %w", d.filenameBase, err)
		}
		if d.noFsync {
			comp.DisableFsync()
		}
		p := ps.AddNew("merege "+datFileName, 1)
		defer ps.Delete(p)

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

			// empty value means deletion
			deleted := r.valuesStartTxNum == 0 && len(lastVal) == 0
			if !deleted {
				if keyBuf != nil {
					if err = comp.AddUncompressedWord(keyBuf); err != nil {
						return nil, nil, nil, err
					}
					keyCount++ // Only counting keys, not values
					switch d.compressVals {
					case true:
						if err = comp.AddWord(valBuf); err != nil {
							return nil, nil, nil, err
						}
					default:
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
			keyCount++ // Only counting keys, not values
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
		ps.Delete(p)
		valuesIn = newFilesItem(r.valuesStartTxNum, r.valuesEndTxNum, d.aggregationStep)
		if valuesIn.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}

		idxFileName := fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
		idxPath := filepath.Join(d.dir, idxFileName)
		p = ps.AddNew("merge "+idxFileName, uint64(keyCount*2))
		defer ps.Delete(p)
		ps.Delete(p)

		//		if valuesIn.index, err = buildIndex(valuesIn.decompressor, idxPath, d.dir, keyCount, false /* values */); err != nil {
		if valuesIn.index, err = buildIndexThenOpen(ctx, valuesIn.decompressor, idxPath, d.tmpdir, keyCount, false /* values */, p, d.logger, d.noFsync); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}

		btFileName := strings.TrimSuffix(idxFileName, "kvi") + "bt"
		p = ps.AddNew(btFileName, uint64(keyCount*2))
		defer ps.Delete(p)
		btPath := filepath.Join(d.dir, btFileName)
		err = BuildBtreeIndexWithDecompressor(btPath, valuesIn.decompressor, p, d.tmpdir, d.logger)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s btindex [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}

		bt, err := OpenBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesIn.decompressor)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s btindex2 [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}
		valuesIn.bindex = bt
	}
	closeItem = false
	d.stats.MergesCount++
	return
}

func (ii *InvertedIndex) mergeFiles(ctx context.Context, files []*filesItem, startTxNum, endTxNum uint64, workers int, ps *background.ProgressSet) (*filesItem, error) {
	for _, h := range files {
		defer h.decompressor.EnableMadvNormal().DisableReadAhead()
	}

	var outItem *filesItem
	var comp *compress.Compressor
	var decomp *compress.Decompressor
	var err error
	var closeItem = true
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
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	datFileName := fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep)
	datPath := filepath.Join(ii.dir, datFileName)
	if comp, err = compress.NewCompressor(ctx, "Snapshots merge", datPath, ii.tmpdir, compress.MinPatternScore, workers, log.LvlTrace, ii.logger); err != nil {
		return nil, fmt.Errorf("merge %s inverted index compressor: %w", ii.filenameBase, err)
	}
	if ii.noFsync {
		comp.DisableFsync()
	}
	p := ps.AddNew("merge "+datFileName, 1)
	defer ps.Delete(p)

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
	outItem = newFilesItem(startTxNum, endTxNum, ii.aggregationStep)
	if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
		return nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	ps.Delete(p)

	idxFileName := fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep)
	idxPath := filepath.Join(ii.dir, idxFileName)
	p = ps.AddNew("merge "+idxFileName, uint64(outItem.decompressor.Count()*2))
	defer ps.Delete(p)
	if outItem.index, err = buildIndexThenOpen(ctx, outItem.decompressor, idxPath, ii.tmpdir, keyCount, false /* values */, p, ii.logger, ii.noFsync); err != nil {
		return nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	closeItem = false
	return outItem, nil
}

func (h *History) mergeFiles(ctx context.Context, indexFiles, historyFiles []*filesItem, r HistoryRanges, workers int, ps *background.ProgressSet) (indexIn, historyIn *filesItem, err error) {
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
	if indexIn, err = h.InvertedIndex.mergeFiles(ctx, indexFiles, r.indexStartTxNum, r.indexEndTxNum, workers, ps); err != nil {
		return nil, nil, err
	}
	if r.history {
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
		var closeItem = true
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
		datFileName := fmt.Sprintf("%s.%d-%d.v", h.filenameBase, r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep)
		idxFileName := fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep)
		datPath := filepath.Join(h.dir, datFileName)
		idxPath := filepath.Join(h.dir, idxFileName)
		if comp, err = compress.NewCompressor(ctx, "merge", datPath, h.tmpdir, compress.MinPatternScore, workers, log.LvlTrace, h.logger); err != nil {
			return nil, nil, fmt.Errorf("merge %s history compressor: %w", h.filenameBase, err)
		}
		if h.noFsync {
			comp.DisableFsync()
		}
		p := ps.AddNew("merge "+datFileName, 1)
		defer ps.Delete(p)
		var cp CursorHeap
		heap.Init(&cp)
		for _, item := range indexFiles {
			g := item.decompressor.MakeGetter()
			g.Reset(0)
			if g.HasNext() {
				var g2 *compress.Getter
				for _, hi := range historyFiles { // full-scan, because it's ok to have different amount files. by unclean-shutdown.
					if hi.startTxNum == item.startTxNum && hi.endTxNum == item.endTxNum {
						g2 = hi.decompressor.MakeGetter()
						break
					}
				}
				if g2 == nil {
					panic(fmt.Sprintf("for file: %s, not found corresponding file to merge", g.FileName()))
				}
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
			for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
				ci1 := cp[0]
				count := eliasfano32.Count(ci1.val)
				for i := uint64(0); i < count; i++ {
					if !ci1.dg2.HasNext() {
						panic(fmt.Errorf("assert: no value??? %s, i=%d, count=%d, lastKey=%x, ci1.key=%x", ci1.dg2.FileName(), i, count, lastKey, ci1.key))
					}

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
				keyCount += int(count)
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
		ps.Delete(p)

		p = ps.AddNew("merge "+idxFileName, uint64(2*keyCount))
		defer ps.Delete(p)
		if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   keyCount,
			Enums:      false,
			BucketSize: 2000,
			LeafSize:   8,
			TmpDir:     h.tmpdir,
			IndexFile:  idxPath,
		}, h.logger); err != nil {
			return nil, nil, fmt.Errorf("create recsplit: %w", err)
		}
		rs.LogLvl(log.LvlTrace)
		if h.noFsync {
			rs.DisableFsync()
		}
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
					txNum, _ := efIt.Next()
					binary.BigEndian.PutUint64(txKey[:], txNum)
					historyKey = append(append(historyKey[:0], txKey[:]...), keyBuf...)
					if err = rs.AddKey(historyKey, valOffset); err != nil {
						return nil, nil, err
					}
					if h.compressVals {
						valOffset, _ = g2.Skip()
					} else {
						valOffset, _ = g2.SkipUncompressed()
					}
				}
				p.Processed.Add(1)
			}
			if err = rs.Build(ctx); err != nil {
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
		historyIn = newFilesItem(r.historyStartTxNum, r.historyEndTxNum, h.aggregationStep)
		historyIn.decompressor = decomp
		historyIn.index = index

		closeItem = false
	}

	closeIndex = false
	return
}

func (d *Domain) integrateMergedFiles(valuesOuts, indexOuts, historyOuts []*filesItem, valuesIn, indexIn, historyIn *filesItem) {
	d.History.integrateMergedFiles(indexOuts, historyOuts, indexIn, historyIn)
	if valuesIn != nil {
		d.files.Set(valuesIn)

		// `kill -9` may leave some garbage
		// but it still may be useful for merges, until we finish merge frozen file
		if historyIn != nil && historyIn.frozen {
			d.files.Walk(func(items []*filesItem) bool {
				for _, item := range items {
					if item.frozen || item.endTxNum > valuesIn.endTxNum {
						continue
					}
					valuesOuts = append(valuesOuts, item)
				}
				return true
			})
		}
	}
	for _, out := range valuesOuts {
		if out == nil {
			panic("must not happen")
		}
		d.files.Delete(out)
		out.canDelete.Store(true)
	}
	d.reCalcRoFiles()
}

func (ii *InvertedIndex) integrateMergedFiles(outs []*filesItem, in *filesItem) {
	if in != nil {
		ii.files.Set(in)

		// `kill -9` may leave some garbage
		// but it still may be useful for merges, until we finish merge frozen file
		if in.frozen {
			ii.files.Walk(func(items []*filesItem) bool {
				for _, item := range items {
					if item.frozen || item.endTxNum > in.endTxNum {
						continue
					}
					outs = append(outs, item)
				}
				return true
			})
		}
	}
	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + ii.filenameBase)
		}
		ii.files.Delete(out)
		out.canDelete.Store(true)
	}
	ii.reCalcRoFiles()
}

func (h *History) integrateMergedFiles(indexOuts, historyOuts []*filesItem, indexIn, historyIn *filesItem) {
	h.InvertedIndex.integrateMergedFiles(indexOuts, indexIn)
	//TODO: handle collision
	if historyIn != nil {
		h.files.Set(historyIn)

		// `kill -9` may leave some garbage
		// but it still may be useful for merges, until we finish merge frozen file
		if historyIn.frozen {
			h.files.Walk(func(items []*filesItem) bool {
				for _, item := range items {
					if item.frozen || item.endTxNum > historyIn.endTxNum {
						continue
					}
					historyOuts = append(historyOuts, item)
				}
				return true
			})
		}
	}
	for _, out := range historyOuts {
		if out == nil {
			panic("must not happen: " + h.filenameBase)
		}
		h.files.Delete(out)
		out.canDelete.Store(true)
	}
	h.reCalcRoFiles()
}

// nolint
func (dc *DomainContext) frozenTo() uint64 {
	if len(dc.files) == 0 {
		return 0
	}
	for i := len(dc.files) - 1; i >= 0; i-- {
		if dc.files[i].src.frozen {
			return cmp.Min(dc.files[i].endTxNum, dc.hc.frozenTo())
		}
	}
	return 0
}

func (hc *HistoryContext) frozenTo() uint64 {
	if len(hc.files) == 0 {
		return 0
	}
	for i := len(hc.files) - 1; i >= 0; i-- {
		if hc.files[i].src.frozen {
			return cmp.Min(hc.files[i].endTxNum, hc.ic.frozenTo())
		}
	}
	return 0
}
func (ic *InvertedIndexContext) frozenTo() uint64 {
	if len(ic.files) == 0 {
		return 0
	}
	for i := len(ic.files) - 1; i >= 0; i-- {
		if ic.files[i].src.frozen {
			return ic.files[i].endTxNum
		}
	}
	return 0
}

func (d *Domain) cleanAfterFreeze(frozenTo uint64) {
	if frozenTo == 0 {
		return
	}

	var outs []*filesItem
	// `kill -9` may leave some garbage
	// but it may be useful for merges, until merge `frozen` file
	d.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen || item.endTxNum > frozenTo {
				continue
			}
			outs = append(outs, item)
		}
		return true
	})

	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + d.filenameBase)
		}
		d.files.Delete(out)
		if out.refcount.Load() == 0 {
			// if it has no readers (invisible even for us) - it's safe to remove file right here
			out.closeFilesAndRemove()
		}
		out.canDelete.Store(true)
	}
	d.History.cleanAfterFreeze(frozenTo)
}

// cleanAfterFreeze - mark all small files before `f` as `canDelete=true`
func (h *History) cleanAfterFreeze(frozenTo uint64) {
	if frozenTo == 0 {
		return
	}
	//if h.filenameBase == "accounts" {
	//	log.Warn("[history] History.cleanAfterFreeze", "frozenTo", frozenTo/h.aggregationStep, "stack", dbg.Stack())
	//}
	var outs []*filesItem
	// `kill -9` may leave some garbage
	// but it may be useful for merges, until merge `frozen` file
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen || item.endTxNum > frozenTo {
				continue
			}
			outs = append(outs, item)
		}
		return true
	})

	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + h.filenameBase)
		}
		out.canDelete.Store(true)

		//if out.refcount.Load() == 0 {
		//	if h.filenameBase == "accounts" {
		//		log.Warn("[history] History.cleanAfterFreeze: immediately delete", "name", out.decompressor.FileName())
		//	}
		//} else {
		//	if h.filenameBase == "accounts" {
		//		log.Warn("[history] History.cleanAfterFreeze: mark as 'canDelete=true'", "name", out.decompressor.FileName())
		//	}
		//}

		// if it has no readers (invisible even for us) - it's safe to remove file right here
		if out.refcount.Load() == 0 {
			out.closeFilesAndRemove()
		}
		h.files.Delete(out)
	}
	h.InvertedIndex.cleanAfterFreeze(frozenTo)
}

// cleanAfterFreeze - mark all small files before `f` as `canDelete=true`
func (ii *InvertedIndex) cleanAfterFreeze(frozenTo uint64) {
	if frozenTo == 0 {
		return
	}
	var outs []*filesItem
	// `kill -9` may leave some garbage
	// but it may be useful for merges, until merge `frozen` file
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen || item.endTxNum > frozenTo {
				continue
			}
			outs = append(outs, item)
		}
		return true
	})

	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + ii.filenameBase)
		}
		out.canDelete.Store(true)
		if out.refcount.Load() == 0 {
			// if it has no readers (invisible even for us) - it's safe to remove file right here
			out.closeFilesAndRemove()
		}
		ii.files.Delete(out)
	}
}

// nolint
func (d *Domain) deleteGarbageFiles() {
	for _, item := range d.garbageFiles {
		// paranoic-mode: don't delete frozen files
		steps := item.endTxNum/d.aggregationStep - item.startTxNum/d.aggregationStep
		if steps%StepsInBiggestFile == 0 {
			continue
		}
		f1 := fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep)
		os.Remove(filepath.Join(d.dir, f1))
		log.Debug("[snapshots] delete garbage", f1)
		f2 := fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep)
		os.Remove(filepath.Join(d.dir, f2))
		log.Debug("[snapshots] delete garbage", f2)
	}
	d.garbageFiles = nil
	d.History.deleteGarbageFiles()
}
func (h *History) deleteGarbageFiles() {
	for _, item := range h.garbageFiles {
		// paranoic-mode: don't delete frozen files
		if item.endTxNum/h.aggregationStep-item.startTxNum/h.aggregationStep == StepsInBiggestFile {
			continue
		}
		f1 := fmt.Sprintf("%s.%d-%d.v", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep)
		os.Remove(filepath.Join(h.dir, f1))
		log.Debug("[snapshots] delete garbage", f1)
		f2 := fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep)
		os.Remove(filepath.Join(h.dir, f2))
		log.Debug("[snapshots] delete garbage", f2)
	}
	h.garbageFiles = nil
	h.InvertedIndex.deleteGarbageFiles()
}
func (ii *InvertedIndex) deleteGarbageFiles() {
	for _, item := range ii.garbageFiles {
		// paranoic-mode: don't delete frozen files
		if item.endTxNum/ii.aggregationStep-item.startTxNum/ii.aggregationStep == StepsInBiggestFile {
			continue
		}
		f1 := fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
		os.Remove(filepath.Join(ii.dir, f1))
		log.Debug("[snapshots] delete garbage", f1)
		f2 := fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
		os.Remove(filepath.Join(ii.dir, f2))
		log.Debug("[snapshots] delete garbage", f2)
	}
	ii.garbageFiles = nil
}
