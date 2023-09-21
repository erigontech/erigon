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
	"path/filepath"
	"strings"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
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

	aggStep uint64
}

func (r DomainRanges) String() string {
	var b strings.Builder
	if r.values {
		b.WriteString(fmt.Sprintf("val:%d-%d", r.valuesStartTxNum/r.aggStep, r.valuesEndTxNum/r.aggStep))
	}
	if r.history {
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("hist:%d-%d", r.historyStartTxNum/r.aggStep, r.historyEndTxNum/r.aggStep))
	}
	if r.index {
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("idx:%d-%d", r.indexStartTxNum/r.aggStep, r.indexEndTxNum/r.aggStep))
	}
	return b.String()
}

func (r DomainRanges) any() bool {
	return r.values || r.history || r.index
}

// findMergeRange
// assumes that all fTypes in d.files have items at least as far as maxEndTxNum
// That is why only Values type is inspected
//
// As any other methods of DomainContext - it can't see any files overlaps or garbage
func (dc *DomainContext) findMergeRange(maxEndTxNum, maxSpan uint64) DomainRanges {
	hr := dc.hc.findMergeRange(maxEndTxNum, maxSpan)
	r := DomainRanges{
		historyStartTxNum: hr.historyStartTxNum,
		historyEndTxNum:   hr.historyEndTxNum,
		history:           hr.history,
		indexStartTxNum:   hr.indexStartTxNum,
		indexEndTxNum:     hr.indexEndTxNum,
		index:             hr.index,
		aggStep:           dc.d.aggregationStep,
	}
	for _, item := range dc.files {
		if item.endTxNum > maxEndTxNum {
			break
		}
		endStep := item.endTxNum / dc.d.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * dc.d.aggregationStep
		start := item.endTxNum - span
		if start < item.startTxNum {
			if !r.values || start < r.valuesStartTxNum {
				r.values = true
				r.valuesStartTxNum = start
				r.valuesEndTxNum = item.endTxNum
			}
		}
	}
	return r
}

func (hc *HistoryContext) findMergeRange(maxEndTxNum, maxSpan uint64) HistoryRanges {
	var r HistoryRanges
	r.index, r.indexStartTxNum, r.indexEndTxNum = hc.ic.findMergeRange(maxEndTxNum, maxSpan)
	for _, item := range hc.files {
		if item.endTxNum > maxEndTxNum {
			continue
		}
		endStep := item.endTxNum / hc.h.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := cmp.Min(spanStep*hc.h.aggregationStep, maxSpan)
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

// 0-1,1-2,2-3,3-4: allow merge 0-1
// 0-2,2-3,3-4: allow merge 0-4
// 0-2,2-4: allow merge 0-4
//
// 0-1,1-2,2-3: allow merge 0-2
//
// 0-2,2-3: nothing to merge
func (ic *InvertedIndexContext) findMergeRange(maxEndTxNum, maxSpan uint64) (bool, uint64, uint64) {
	var minFound bool
	var startTxNum, endTxNum uint64
	for _, item := range ic.files {
		if item.endTxNum > maxEndTxNum {
			continue
		}
		endStep := item.endTxNum / ic.ii.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := cmp.Min(spanStep*ic.ii.aggregationStep, maxSpan)
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

func (dc *DomainContext) BuildOptionalMissedIndices(ctx context.Context, ps *background.ProgressSet) (err error) {
	if err := dc.hc.ic.BuildOptionalMissedIndices(ctx, ps); err != nil {
		return err
	}
	return nil
}

func (ic *InvertedIndexContext) BuildOptionalMissedIndices(ctx context.Context, ps *background.ProgressSet) (err error) {
	if ic.ii.withLocalityIndex && ic.ii.coldLocalityIdx != nil {
		from, to := uint64(0), ic.maxColdStep()
		if to == 0 || ic.ii.coldLocalityIdx.exists(from, to) {
			return nil
		}
		defer func() {
			if ic.ii.filenameBase == AggTraceFileLife {
				ic.ii.logger.Warn(fmt.Sprintf("[agg] BuildColdLocality done: %s.%d-%d", ic.ii.filenameBase, from, to))
			}
		}()
		if err = ic.ii.coldLocalityIdx.BuildMissedIndices(ctx, from, to, true, ps,
			func() *LocalityIterator { return ic.iterateKeysLocality(ctx, from, to, nil) },
		); err != nil {
			return err
		}
	}
	return nil
}

func (dc *DomainContext) maxColdStep() uint64 {
	return dc.maxTxNumInFiles(true) / dc.d.aggregationStep
}
func (ic *InvertedIndexContext) maxColdStep() uint64 {
	return ic.maxTxNumInFiles(true) / ic.ii.aggregationStep
}
func (ic *InvertedIndexContext) minWarmStep() uint64 {
	return ic.maxTxNumInFiles(true) / ic.ii.aggregationStep
}
func (ic *InvertedIndexContext) maxWarmStep() uint64 {
	return ic.maxTxNumInFiles(false) / ic.ii.aggregationStep
}

func (dc *DomainContext) maxTxNumInFiles(cold bool) uint64 {
	if len(dc.files) == 0 {
		return 0
	}
	var max uint64
	if cold {
		for i := len(dc.files) - 1; i >= 0; i-- {
			if !dc.files[i].src.frozen {
				continue
			}
			max = dc.files[i].endTxNum
			break
		}
	} else {
		max = dc.files[len(dc.files)-1].endTxNum
	}
	return cmp.Min(max, dc.hc.maxTxNumInFiles(cold))
}

func (hc *HistoryContext) maxTxNumInFiles(cold bool) uint64 {
	if len(hc.files) == 0 {
		return 0
	}
	var max uint64
	if cold {
		for i := len(hc.files) - 1; i >= 0; i-- {
			if !hc.files[i].src.frozen {
				continue
			}
			max = hc.files[i].endTxNum
			break
		}
	} else {
		max = hc.files[len(hc.files)-1].endTxNum
	}
	return cmp.Min(max, hc.ic.maxTxNumInFiles(cold))
}
func (ic *InvertedIndexContext) maxTxNumInFiles(cold bool) uint64 {
	if len(ic.files) == 0 {
		return 0
	}
	if !cold {
		return ic.files[len(ic.files)-1].endTxNum
	}
	for i := len(ic.files) - 1; i >= 0; i-- {
		if !ic.files[i].src.frozen {
			continue
		}
		return ic.files[i].endTxNum
	}
	return 0
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

	closeItem := true
	var comp ArchiveWriter
	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if indexIn != nil {
				indexIn.closeFilesAndRemove()
			}
			if historyIn != nil {
				historyIn.closeFilesAndRemove()
			}
			if valuesIn != nil {
				valuesIn.closeFilesAndRemove()
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

	if !r.values {
		closeItem = false
		return
	}

	for _, f := range valuesFiles {
		defer f.decompressor.EnableReadAhead().DisableReadAhead()
	}

	datPath := d.kvFilePath(r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
	compr, err := compress.NewCompressor(ctx, "merge", datPath, d.dirs.Tmp, compress.MinPatternScore, workers, log.LvlTrace, d.logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s domain compressor: %w", d.filenameBase, err)
	}

	comp = NewArchiveWriter(compr, d.compression)
	if d.noFsync {
		comp.DisableFsync()
	}
	_, datFileName := filepath.Split(datPath)
	p := ps.AddNew("merge "+datFileName, 1)
	defer ps.Delete(p)

	var cp CursorHeap
	heap.Init(&cp)
	for _, item := range valuesFiles {
		g := NewArchiveGetter(item.decompressor.MakeGetter(), d.compression)
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
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
			ci1 := heap.Pop(&cp).(*CursorItem)
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.Next(nil)
				ci1.val, _ = ci1.dg.Next(nil)
				heap.Push(&cp, ci1)
			}
		}

		// empty value means deletion
		deleted := r.valuesStartTxNum == 0 && len(lastVal) == 0
		if !deleted {
			if keyBuf != nil {
				if err = comp.AddWord(keyBuf); err != nil {
					return nil, nil, nil, err
				}
				if err = comp.AddWord(valBuf); err != nil {
					return nil, nil, nil, err
				}
			}
			keyBuf = append(keyBuf[:0], lastKey...)
			valBuf = append(valBuf[:0], lastVal...)
		}
	}
	if keyBuf != nil {
		if err = comp.AddWord(keyBuf); err != nil {
			return nil, nil, nil, err
		}
		if err = comp.AddWord(valBuf); err != nil {
			return nil, nil, nil, err
		}
	}
	if err = comp.Compress(); err != nil {
		return nil, nil, nil, err
	}
	comp.Close()
	comp = nil
	ps.Delete(p)

	valuesIn = newFilesItem(r.valuesStartTxNum, r.valuesEndTxNum, d.aggregationStep)
	valuesIn.frozen = false
	if valuesIn.decompressor, err = compress.NewDecompressor(datPath); err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
	}

	//		if valuesIn.index, err = buildIndex(valuesIn.decompressor, idxPath, d.dir,  false /* values */); err != nil {
	if !UseBpsTree {
		idxPath := d.kvAccessorFilePath(r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
		if valuesIn.index, err = buildIndexThenOpen(ctx, valuesIn.decompressor, d.compression, idxPath, d.dirs.Tmp, false, d.salt, ps, d.logger, d.noFsync); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}
	}

	btPath := d.kvBtFilePath(r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
	valuesIn.bindex, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesIn.decompressor, d.compression, *d.salt, ps, d.dirs.Tmp, d.logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s btindex [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
	}

	{
		eiPath := d.kvExistenceIdxFilePath(r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
		if dir.FileExist(eiPath) {
			valuesIn.bloom, err = OpenBloom(eiPath)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("merge %s bloom [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
			}
		}
	}

	closeItem = false
	d.stats.MergesCount++
	return
}

func (d *DomainCommitted) mergeFiles(ctx context.Context, oldFiles SelectedStaticFiles, mergedFiles MergedFiles, r DomainRanges, workers int, ps *background.ProgressSet) (valuesIn, indexIn, historyIn *filesItem, err error) {
	if !r.any() {
		return
	}

	domainFiles := oldFiles.commitment
	indexFiles := oldFiles.commitmentIdx
	historyFiles := oldFiles.commitmentHist

	var comp ArchiveWriter
	var closeItem = true
	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if indexIn != nil {
				indexIn.closeFilesAndRemove()
			}
			if historyIn != nil {
				historyIn.closeFilesAndRemove()
			}
			if valuesIn != nil {
				valuesIn.closeFilesAndRemove()
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

	if !r.values {
		closeItem = false
		return
	}

	datPath := d.kvFilePath(r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
	_, datFileName := filepath.Split(datPath)
	p := ps.AddNew(datFileName, 1)
	defer ps.Delete(p)

	cmp, err := compress.NewCompressor(ctx, "merge", datPath, d.dirs.Tmp, compress.MinPatternScore, workers, log.LvlTrace, d.logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s compressor: %w", d.filenameBase, err)
	}
	comp = NewArchiveWriter(cmp, d.compression)

	for _, f := range domainFiles {
		defer f.decompressor.EnableReadAhead().DisableReadAhead()
	}

	var cp CursorHeap
	heap.Init(&cp)
	for _, item := range domainFiles {
		g := NewArchiveGetter(item.decompressor.MakeGetter(), d.compression)
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
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
			ci1 := heap.Pop(&cp).(*CursorItem)
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.Next(nil)
				ci1.val, _ = ci1.dg.Next(nil)
				heap.Push(&cp, ci1)
			}
		}
		// For the rest of types, empty value means deletion
		skip := r.valuesStartTxNum == 0 && len(lastVal) == 0
		if !skip {
			if keyBuf != nil {
				if err = comp.AddWord(keyBuf); err != nil {
					return nil, nil, nil, err
				}
				if err = comp.AddWord(valBuf); err != nil {
					return nil, nil, nil, err
				}
			}
			keyBuf = append(keyBuf[:0], lastKey...)
			valBuf = append(valBuf[:0], lastVal...)
		}
	}
	if keyBuf != nil {
		if err = comp.AddWord(keyBuf); err != nil {
			return nil, nil, nil, err
		}
		//fmt.Printf("last heap key %x\n", keyBuf)
		valBuf, err = d.commitmentValTransform(&oldFiles, &mergedFiles, valBuf)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("merge: 2valTransform [%x] %w", valBuf, err)
		}
		if err = comp.AddWord(valBuf); err != nil {
			return nil, nil, nil, err
		}
	}
	if err = comp.Compress(); err != nil {
		return nil, nil, nil, err
	}
	comp.Close()
	comp = nil

	valuesIn = newFilesItem(r.valuesStartTxNum, r.valuesEndTxNum, d.aggregationStep)
	valuesIn.frozen = false
	if valuesIn.decompressor, err = compress.NewDecompressor(datPath); err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
	}
	ps.Delete(p)

	if !UseBpsTree {
		idxPath := d.kvAccessorFilePath(r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
		if valuesIn.index, err = buildIndexThenOpen(ctx, valuesIn.decompressor, d.compression, idxPath, d.dirs.Tmp, false, d.salt, ps, d.logger, d.noFsync); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}
	}

	btPath := d.kvBtFilePath(r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep)
	valuesIn.bindex, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesIn.decompressor, d.compression, *d.salt, ps, d.dirs.Tmp, d.logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create btindex %s [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
	}

	closeItem = false
	return
}

func (ii *InvertedIndex) mergeFiles(ctx context.Context, files []*filesItem, startTxNum, endTxNum uint64, workers int, ps *background.ProgressSet) (*filesItem, error) {
	for _, h := range files {
		defer h.decompressor.EnableReadAhead().DisableReadAhead()
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
				outItem.closeFilesAndRemove()
			}
		}
	}()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	datPath := ii.efFilePath(startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep)
	if comp, err = compress.NewCompressor(ctx, "Snapshots merge", datPath, ii.dirs.Tmp, compress.MinPatternScore, workers, log.LvlTrace, ii.logger); err != nil {
		return nil, fmt.Errorf("merge %s inverted index compressor: %w", ii.filenameBase, err)
	}
	if ii.noFsync {
		comp.DisableFsync()
	}
	write := NewArchiveWriter(comp, ii.compression)
	_, datFileName := filepath.Split(datPath)
	p := ps.AddNew(datFileName, 1)
	defer ps.Delete(p)

	var cp CursorHeap
	heap.Init(&cp)

	for _, item := range files {
		g := NewArchiveGetter(item.decompressor.MakeGetter(), ii.compression)
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
			ci1 := heap.Pop(&cp).(*CursorItem)
			if mergedOnce {
				if lastVal, err = mergeEfs(ci1.val, lastVal, nil); err != nil {
					return nil, fmt.Errorf("merge %s inverted index: %w", ii.filenameBase, err)
				}
			} else {
				mergedOnce = true
			}
			//fmt.Printf("multi-way %s [%d] %x\n", ii.indexKeysTable, ci1.endTxNum, ci1.key)
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.Next(nil)
				ci1.val, _ = ci1.dg.Next(nil)
				//fmt.Printf("heap next push %s [%d] %x\n", ii.indexKeysTable, ci1.endTxNum, ci1.key)
				heap.Push(&cp, ci1)
			}
		}
		if keyBuf != nil {
			if err = write.AddWord(keyBuf); err != nil {
				return nil, err
			}
			if err = write.AddWord(valBuf); err != nil {
				return nil, err
			}
		}
		keyBuf = append(keyBuf[:0], lastKey...)
		valBuf = append(valBuf[:0], lastVal...)
	}
	if keyBuf != nil {
		if err = write.AddWord(keyBuf); err != nil {
			return nil, err
		}
		if err = write.AddWord(valBuf); err != nil {
			return nil, err
		}
	}
	if err = write.Compress(); err != nil {
		return nil, err
	}
	comp.Close()
	comp = nil

	outItem = newFilesItem(startTxNum, endTxNum, ii.aggregationStep)
	if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
		return nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	ps.Delete(p)

	{
		idxPath := ii.efAccessorFilePath(startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep)
		if outItem.index, err = buildIndexThenOpen(ctx, outItem.decompressor, ii.compression, idxPath, ii.dirs.Tmp, false, ii.salt, ps, ii.logger, ii.noFsync); err != nil {
			return nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
		}
	}
	if ii.withExistenceIndex {
		idxPath := ii.efExistenceIdxFilePath(startTxNum/ii.aggregationStep, endTxNum/ii.aggregationStep)
		if outItem.bloom, err = buildIndexFilterThenOpen(ctx, outItem.decompressor, ii.compression, idxPath, ii.dirs.Tmp, ii.salt, ps, ii.logger, ii.noFsync); err != nil {
			return nil, err
		}
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
				indexIn.closeFilesAndRemove()
			}
		}
	}()
	if indexIn, err = h.InvertedIndex.mergeFiles(ctx, indexFiles, r.indexStartTxNum, r.indexEndTxNum, workers, ps); err != nil {
		return nil, nil, err
	}
	if r.history {
		for _, f := range indexFiles {
			defer f.decompressor.EnableReadAhead().DisableReadAhead()
		}
		for _, f := range historyFiles {
			defer f.decompressor.EnableReadAhead().DisableReadAhead()
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
					historyIn.closeFilesAndRemove()
				}
			}
		}()
		datPath := h.vFilePath(r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep)
		idxPath := h.vAccessorFilePath(r.historyStartTxNum/h.aggregationStep, r.historyEndTxNum/h.aggregationStep)
		if comp, err = compress.NewCompressor(ctx, "merge", datPath, h.dirs.Tmp, compress.MinPatternScore, workers, log.LvlTrace, h.logger); err != nil {
			return nil, nil, fmt.Errorf("merge %s history compressor: %w", h.filenameBase, err)
		}
		compr := NewArchiveWriter(comp, h.compression)
		if h.noFsync {
			compr.DisableFsync()
		}
		_, datFileName := filepath.Split(datPath)
		p := ps.AddNew(datFileName, 1)
		defer ps.Delete(p)

		var cp CursorHeap
		heap.Init(&cp)
		for _, item := range indexFiles {
			g := NewArchiveGetter(item.decompressor.MakeGetter(), h.compression)
			g.Reset(0)
			if g.HasNext() {
				var g2 ArchiveGetter
				for _, hi := range historyFiles { // full-scan, because it's ok to have different amount files. by unclean-shutdown.
					if hi.startTxNum == item.startTxNum && hi.endTxNum == item.endTxNum {
						g2 = NewArchiveGetter(hi.decompressor.MakeGetter(), h.compression)
						break
					}
				}
				if g2 == nil {
					panic(fmt.Sprintf("for file: %s, not found corresponding file to merge", g.FileName()))
				}
				key, _ := g.Next(nil)
				val, _ := g.Next(nil)
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
				ci1 := heap.Pop(&cp).(*CursorItem)
				count := eliasfano32.Count(ci1.val)
				for i := uint64(0); i < count; i++ {
					if !ci1.dg2.HasNext() {
						panic(fmt.Errorf("assert: no value??? %s, i=%d, count=%d, lastKey=%x, ci1.key=%x", ci1.dg2.FileName(), i, count, lastKey, ci1.key))
					}

					valBuf, _ = ci1.dg2.Next(valBuf[:0])
					if err = compr.AddWord(valBuf); err != nil {
						return nil, nil, err
					}
				}
				keyCount += int(count)
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.Next(nil)
					ci1.val, _ = ci1.dg.Next(nil)
					heap.Push(&cp, ci1)
				}
			}
		}
		if err = compr.Compress(); err != nil {
			return nil, nil, err
		}
		compr.Close()
		comp = nil
		if decomp, err = compress.NewDecompressor(datPath); err != nil {
			return nil, nil, err
		}
		ps.Delete(p)

		_, idxFileName := filepath.Split(idxPath)
		p = ps.AddNew(idxFileName, uint64(decomp.Count()/2))
		defer ps.Delete(p)
		if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:    keyCount,
			Enums:       false,
			BucketSize:  2000,
			LeafSize:    8,
			TmpDir:      h.dirs.Tmp,
			IndexFile:   idxPath,
			EtlBufLimit: etl.BufferOptimalSize / 2,
			Salt:        h.salt,
		}, h.logger); err != nil {
			return nil, nil, fmt.Errorf("create recsplit: %w", err)
		}
		rs.LogLvl(log.LvlTrace)

		if h.noFsync {
			rs.DisableFsync()
		}

		var (
			txKey      [8]byte
			historyKey []byte
			keyBuf     []byte
			valOffset  uint64
		)

		g := NewArchiveGetter(indexIn.decompressor.MakeGetter(), h.InvertedIndex.compression)
		g2 := NewArchiveGetter(decomp.MakeGetter(), h.compression)

		for {
			g.Reset(0)
			g2.Reset(0)
			valOffset = 0
			for g.HasNext() {
				keyBuf, _ = g.Next(nil)
				valBuf, _ = g.Next(nil)
				ef, _ := eliasfano32.ReadEliasFano(valBuf)
				efIt := ef.Iterator()
				for efIt.HasNext() {
					txNum, _ := efIt.Next()
					binary.BigEndian.PutUint64(txKey[:], txNum)
					historyKey = append(append(historyKey[:0], txKey[:]...), keyBuf...)
					if err = rs.AddKey(historyKey, valOffset); err != nil {
						return nil, nil, err
					}
					valOffset, _ = g2.Skip()
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
		d.files.Walk(func(items []*filesItem) bool {
			for _, item := range items {
				if item.frozen {
					continue
				}
				if item.startTxNum < valuesIn.startTxNum {
					continue
				}
				if item.endTxNum > valuesIn.endTxNum {
					continue
				}
				if item.startTxNum == valuesIn.startTxNum && item.endTxNum == valuesIn.endTxNum {
					continue
				}
				valuesOuts = append(valuesOuts, item)
			}
			return true
		})
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

		if ii.filenameBase == AggTraceFileLife {
			ii.logger.Warn(fmt.Sprintf("[agg] mark can delete: %s, triggered by merge of: %s", out.decompressor.FileName(), in.decompressor.FileName()))
		}
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

// nolint
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

// nolint
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

func (d *Domain) cleanAfterFreeze(mergedDomain, mergedHist, mergedIdx *filesItem) {
	if mergedHist != nil && mergedHist.frozen {
		d.History.cleanAfterFreeze(mergedHist.endTxNum)
	}
	if mergedDomain == nil {
		return
	}
	var outs []*filesItem
	mergedFrom, mergedTo := mergedDomain.startTxNum, mergedDomain.endTxNum
	// `kill -9` may leave some garbage
	// but it may be useful for merges, until merge `frozen` file
	d.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.startTxNum > mergedFrom && item.endTxNum < mergedTo {
				outs = append(outs, item)
			}
			//TODO: domain doesn't have .frozen flag. Somehow need delete all earlier sub-sets, but keep largest one.
		}
		return true
	})

	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + d.filenameBase)
		}
		d.files.Delete(out)
		out.canDelete.Store(true)
		if out.refcount.Load() == 0 {
			if d.filenameBase == AggTraceFileLife && out.decompressor != nil {
				d.logger.Info(fmt.Sprintf("[agg] cleanAfterFreeze remove: %s\n", out.decompressor.FileName()))
			}
			// if it has no readers (invisible even for us) - it's safe to remove file right here
			out.closeFilesAndRemove()
		} else {
			if d.filenameBase == AggTraceFileLife && out.decompressor != nil {
				d.logger.Warn(fmt.Sprintf("[agg] cleanAfterFreeze mark as delete: %s, refcnt=%d", out.decompressor.FileName(), out.refcount.Load()))
			}
		}
	}
}

// cleanAfterFreeze - sometime inverted_index may be already merged, but history not yet. and power-off happening.
// in this case we need keep small files, but when history already merged to `frozen` state - then we can cleanup
// all earlier small files, by mark tem as `canDelete=true`
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
