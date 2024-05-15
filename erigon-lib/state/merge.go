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
	"path"
	"path/filepath"
	"strings"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common/background"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/erigon-lib/seg"
)

func (d *Domain) dirtyFilesEndTxNumMinimax() uint64 {
	minimax := d.History.endTxNumMinimax()
	if _max, ok := d.dirtyFiles.Max(); ok {
		endTxNum := _max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

func (ii *InvertedIndex) endTxNumMinimax() uint64 {
	var minimax uint64
	if _max, ok := ii.dirtyFiles.Max(); ok {
		endTxNum := _max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}
func (ii *InvertedIndex) endIndexedTxNumMinimax(needFrozen bool) uint64 {
	var _max uint64
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || (needFrozen && !item.frozen) {
				continue
			}
			_max = cmp.Max(_max, item.endTxNum)
		}
		return true
	})
	return _max
}

func (h *History) endTxNumMinimax() uint64 {
	if h.dontProduceHistoryFiles {
		return math.MaxUint64
	}
	minimax := h.InvertedIndex.endTxNumMinimax()
	if _max, ok := h.dirtyFiles.Max(); ok {
		endTxNum := _max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}
func (h *History) endIndexedTxNumMinimax(needFrozen bool) uint64 {
	var _max uint64
	if h.dontProduceHistoryFiles && h.dirtyFiles.Len() == 0 {
		_max = math.MaxUint64
	}
	h.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || (needFrozen && !item.frozen) {
				continue
			}
			_max = cmp.Max(_max, item.endTxNum)
		}
		return true
	})
	return cmp.Min(_max, h.InvertedIndex.endIndexedTxNumMinimax(needFrozen))
}

type DomainRanges struct {
	name              kv.Domain
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
// As any other methods of DomainRoTx - it can't see any files overlaps or garbage
func (dt *DomainRoTx) findMergeRange(maxEndTxNum, maxSpan uint64) DomainRanges {
	hr := dt.ht.findMergeRange(maxEndTxNum, maxSpan)
	domainName, err := kv.String2Domain(dt.d.filenameBase)
	if err != nil {
		panic(err)
	}
	r := DomainRanges{
		name:              domainName,
		historyStartTxNum: hr.historyStartTxNum,
		historyEndTxNum:   hr.historyEndTxNum,
		history:           hr.history,
		indexStartTxNum:   hr.indexStartTxNum,
		indexEndTxNum:     hr.indexEndTxNum,
		index:             hr.index,
		aggStep:           dt.d.aggregationStep,
	}
	for _, item := range dt.files {
		if item.endTxNum > maxEndTxNum {
			break
		}
		endStep := item.endTxNum / dt.d.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * dt.d.aggregationStep
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

func (ht *HistoryRoTx) findMergeRange(maxEndTxNum, maxSpan uint64) HistoryRanges {
	var r HistoryRanges
	r.index, r.indexStartTxNum, r.indexEndTxNum = ht.iit.findMergeRange(maxEndTxNum, maxSpan)
	for _, item := range ht.files {
		if item.endTxNum > maxEndTxNum {
			continue
		}
		endStep := item.endTxNum / ht.h.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := cmp.Min(spanStep*ht.h.aggregationStep, maxSpan)
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
func (iit *InvertedIndexRoTx) findMergeRange(maxEndTxNum, maxSpan uint64) (bool, uint64, uint64) {
	var minFound bool
	var startTxNum, endTxNum uint64
	for _, item := range iit.files {
		if item.endTxNum > maxEndTxNum {
			continue
		}
		endStep := item.endTxNum / iit.ii.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := cmp.Min(spanStep*iit.ii.aggregationStep, maxSpan)
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

func (dt *DomainRoTx) BuildOptionalMissedIndices(ctx context.Context, ps *background.ProgressSet) (err error) {
	if err := dt.ht.iit.BuildOptionalMissedIndices(ctx, ps); err != nil {
		return err
	}
	return nil
}

func (iit *InvertedIndexRoTx) BuildOptionalMissedIndices(ctx context.Context, ps *background.ProgressSet) (err error) {
	return nil
}

// endTxNum is always a multiply of aggregation step but this txnum is not available in file (it will be first tx of file to follow after that)
func (dt *DomainRoTx) maxTxNumInDomainFiles(cold bool) uint64 {
	if len(dt.files) == 0 {
		return 0
	}
	if !cold {
		return dt.files[len(dt.files)-1].endTxNum
	}
	for i := len(dt.files) - 1; i >= 0; i-- {
		if !dt.files[i].src.frozen {
			continue
		}
		return dt.files[i].endTxNum
	}
	return 0
}

func (ht *HistoryRoTx) maxTxNumInFiles(onlyFrozen bool) uint64 {
	if len(ht.files) == 0 {
		return 0
	}
	var _max uint64
	if onlyFrozen {
		for i := len(ht.files) - 1; i >= 0; i-- {
			if !ht.files[i].src.frozen {
				continue
			}
			_max = ht.files[i].endTxNum
			break
		}
	} else {
		_max = ht.files[len(ht.files)-1].endTxNum
	}
	return cmp.Min(_max, ht.iit.maxTxNumInFiles(onlyFrozen))
}

func (iit *InvertedIndexRoTx) maxTxNumInFiles(onlyFrozen bool) uint64 {
	if len(iit.files) == 0 {
		return 0
	}
	if !onlyFrozen {
		return iit.lastTxNumInFiles()
	}

	// files contains [frozen..., cold...] in that order
	for i := len(iit.files) - 1; i >= 0; i-- {
		if !iit.files[i].src.frozen {
			continue
		}
		return iit.files[i].endTxNum
	}
	return 0
}

// staticFilesInRange returns list of static files with txNum in specified range [startTxNum; endTxNum)
// files are in the descending order of endTxNum
func (dt *DomainRoTx) staticFilesInRange(r DomainRanges) (valuesFiles, indexFiles, historyFiles []*filesItem, startJ int) {
	if r.index || r.history {
		var err error
		indexFiles, historyFiles, startJ, err = dt.ht.staticFilesInRange(HistoryRanges{
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
		for _, item := range dt.files {
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

func (iit *InvertedIndexRoTx) staticFilesInRange(startTxNum, endTxNum uint64) ([]*filesItem, int) {
	files := make([]*filesItem, 0, len(iit.files))
	var startJ int

	for _, item := range iit.files {
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
func (ii *InvertedIndex) staticFilesInRange(startTxNum, endTxNum uint64, ic *InvertedIndexRoTx) ([]*filesItem, int) {
	panic("deprecated: use InvertedIndexRoTx.staticFilesInRange")
}

func (ht *HistoryRoTx) staticFilesInRange(r HistoryRanges) (indexFiles, historyFiles []*filesItem, startJ int, err error) {
	if !r.history && r.index {
		indexFiles, startJ = ht.iit.staticFilesInRange(r.indexStartTxNum, r.indexEndTxNum)
		return indexFiles, historyFiles, startJ, nil
	}

	if r.history {
		// Get history files from HistoryRoTx (no "garbage/overalps"), but index files not from InvertedIndexRoTx
		// because index files may already be merged (before `kill -9`) and it means not visible in InvertedIndexRoTx
		startJ = 0
		for _, item := range ht.files {
			if item.startTxNum < r.historyStartTxNum {
				startJ++
				continue
			}
			if item.endTxNum > r.historyEndTxNum {
				break
			}

			historyFiles = append(historyFiles, item.src)
			idxFile, ok := ht.h.InvertedIndex.dirtyFiles.Get(item.src)
			if ok {
				indexFiles = append(indexFiles, idxFile)
			} else {
				walkErr := fmt.Errorf("History.staticFilesInRange: required file not found: v1-%s.%d-%d.efi", ht.h.filenameBase, item.startTxNum/ht.h.aggregationStep, item.endTxNum/ht.h.aggregationStep)
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
func (h *History) staticFilesInRange(r HistoryRanges, hc *HistoryRoTx) (indexFiles, historyFiles []*filesItem, startJ int, err error) {
	panic("deprecated: use HistoryRoTx.staticFilesInRange")
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

type valueTransformer func(val []byte, startTxNum, endTxNum uint64) ([]byte, error)

func (dt *DomainRoTx) mergeFiles(ctx context.Context, domainFiles, indexFiles, historyFiles []*filesItem, r DomainRanges, vt valueTransformer, ps *background.ProgressSet) (valuesIn, indexIn, historyIn *filesItem, err error) {
	if !r.any() {
		return
	}

	closeItem := true
	var kvWriter ArchiveWriter
	defer func() {
		if closeItem {
			if kvWriter != nil {
				kvWriter.Close()
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
	if indexIn, historyIn, err = dt.ht.mergeFiles(ctx, indexFiles, historyFiles, HistoryRanges{
		historyStartTxNum: r.historyStartTxNum,
		historyEndTxNum:   r.historyEndTxNum,
		history:           r.history,
		indexStartTxNum:   r.indexStartTxNum,
		indexEndTxNum:     r.indexEndTxNum,
		index:             r.index}, ps); err != nil {
		return nil, nil, nil, err
	}

	if !r.values {
		closeItem = false
		return
	}

	for _, f := range domainFiles {
		f := f
		defer f.decompressor.EnableReadAhead().DisableReadAhead()
	}

	fromStep, toStep := r.valuesStartTxNum/dt.d.aggregationStep, r.valuesEndTxNum/dt.d.aggregationStep
	kvFilePath := dt.d.kvFilePath(fromStep, toStep)
	kvFile, err := seg.NewCompressor(ctx, "merge domain "+dt.d.filenameBase, kvFilePath, dt.d.dirs.Tmp, seg.MinPatternScore, dt.d.compressWorkers, log.LvlTrace, dt.d.logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s compressor: %w", dt.d.filenameBase, err)
	}

	kvWriter = NewArchiveWriter(kvFile, dt.d.compression)
	if dt.d.noFsync {
		kvWriter.DisableFsync()
	}
	p := ps.AddNew("merge "+path.Base(kvFilePath), 1)
	defer ps.Delete(p)

	var cp CursorHeap
	heap.Init(&cp)
	for _, item := range domainFiles {
		g := NewArchiveGetter(item.decompressor.MakeGetter(), dt.d.compression)
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
			heap.Push(&cp, &CursorItem{
				t:          FILE_CURSOR,
				dg:         g,
				key:        key,
				val:        val,
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				reverse:    true,
			})
		}
	}
	// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
	// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
	// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
	// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
	// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
	var keyBuf, valBuf []byte
	var keyFileStartTxNum, keyFileEndTxNum uint64
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		lastFileStartTxNum, lastFileEndTxNum := cp[0].startTxNum, cp[0].endTxNum
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
		deleted := r.valuesStartTxNum == 0 && len(lastVal) == 0
		if !deleted {
			if keyBuf != nil {
				if vt != nil {
					if !bytes.Equal(keyBuf, keyCommitmentState) { // no replacement for state key
						valBuf, err = vt(valBuf, keyFileStartTxNum, keyFileEndTxNum)
						if err != nil {
							return nil, nil, nil, fmt.Errorf("merge: valTransform failed: %w", err)
						}
					}
				}
				if err = kvWriter.AddWord(keyBuf); err != nil {
					return nil, nil, nil, err
				}
				if err = kvWriter.AddWord(valBuf); err != nil {
					return nil, nil, nil, err
				}
			}
			keyBuf = append(keyBuf[:0], lastKey...)
			valBuf = append(valBuf[:0], lastVal...)
			keyFileStartTxNum, keyFileEndTxNum = lastFileStartTxNum, lastFileEndTxNum
		}
	}
	if keyBuf != nil {
		if vt != nil {
			if !bytes.Equal(keyBuf, keyCommitmentState) { // no replacement for state key
				valBuf, err = vt(valBuf, keyFileStartTxNum, keyFileEndTxNum)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("merge: valTransform failed: %w", err)
				}
			}
		}
		if err = kvWriter.AddWord(keyBuf); err != nil {
			return nil, nil, nil, err
		}
		if err = kvWriter.AddWord(valBuf); err != nil {
			return nil, nil, nil, err
		}
	}
	if err = kvWriter.Compress(); err != nil {
		return nil, nil, nil, err
	}
	kvWriter.Close()
	kvWriter = nil
	ps.Delete(p)

	valuesIn = newFilesItem(r.valuesStartTxNum, r.valuesEndTxNum, dt.d.aggregationStep)
	valuesIn.frozen = false
	if valuesIn.decompressor, err = seg.NewDecompressor(kvFilePath); err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", dt.d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
	}

	if UseBpsTree {
		btPath := dt.d.kvBtFilePath(fromStep, toStep)
		valuesIn.bindex, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesIn.decompressor, dt.d.compression, *dt.d.salt, ps, dt.d.dirs.Tmp, dt.d.logger, dt.d.noFsync)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s btindex [%d-%d]: %w", dt.d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}
	} else {
		if err = dt.d.buildMapIdx(ctx, fromStep, toStep, valuesIn.decompressor, ps); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", dt.d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}
		if valuesIn.index, err = recsplit.OpenIndex(dt.d.kvAccessorFilePath(fromStep, toStep)); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", dt.d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
		}
	}

	{
		bloomIndexPath := dt.d.kvExistenceIdxFilePath(fromStep, toStep)
		if dir.FileExist(bloomIndexPath) {
			valuesIn.existence, err = OpenExistenceFilter(bloomIndexPath)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("merge %s existence [%d-%d]: %w", dt.d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
			}
		}
	}

	closeItem = false
	dt.d.stats.MergesCount++
	return
}

func (iit *InvertedIndexRoTx) mergeFiles(ctx context.Context, files []*filesItem, startTxNum, endTxNum uint64, ps *background.ProgressSet) (*filesItem, error) {
	for _, h := range files {
		defer h.decompressor.EnableReadAhead().DisableReadAhead()
	}

	var outItem *filesItem
	var comp *seg.Compressor
	var decomp *seg.Decompressor
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
	fromStep, toStep := startTxNum/iit.ii.aggregationStep, endTxNum/iit.ii.aggregationStep

	datPath := iit.ii.efFilePath(fromStep, toStep)
	if comp, err = seg.NewCompressor(ctx, "merge idx "+iit.ii.filenameBase, datPath, iit.ii.dirs.Tmp, seg.MinPatternScore, iit.ii.compressWorkers, log.LvlTrace, iit.ii.logger); err != nil {
		return nil, fmt.Errorf("merge %s inverted index compressor: %w", iit.ii.filenameBase, err)
	}
	if iit.ii.noFsync {
		comp.DisableFsync()
	}
	write := NewArchiveWriter(comp, iit.ii.compression)
	p := ps.AddNew(path.Base(datPath), 1)
	defer ps.Delete(p)

	var cp CursorHeap
	heap.Init(&cp)

	for _, item := range files {
		g := NewArchiveGetter(item.decompressor.MakeGetter(), iit.ii.compression)
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
					return nil, fmt.Errorf("merge %s inverted index: %w", iit.ii.filenameBase, err)
				}
			} else {
				mergedOnce = true
			}
			// fmt.Printf("multi-way %s [%d] %x\n", ii.indexKeysTable, ci1.endTxNum, ci1.key)
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.Next(nil)
				ci1.val, _ = ci1.dg.Next(nil)
				// fmt.Printf("heap next push %s [%d] %x\n", ii.indexKeysTable, ci1.endTxNum, ci1.key)
				heap.Push(&cp, ci1)
			}
		}
		if keyBuf != nil {
			// fmt.Printf("pput %x->%x\n", keyBuf, valBuf)
			if err = write.AddWord(keyBuf); err != nil {
				return nil, err
			}
			if err = write.AddWord(valBuf); err != nil {
				return nil, err
			}
		}
		keyBuf = append(keyBuf[:0], lastKey...)
		if keyBuf == nil {
			keyBuf = []byte{}
		}
		valBuf = append(valBuf[:0], lastVal...)
	}
	if keyBuf != nil {
		// fmt.Printf("put %x->%x\n", keyBuf, valBuf)
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

	outItem = newFilesItem(startTxNum, endTxNum, iit.ii.aggregationStep)
	if outItem.decompressor, err = seg.NewDecompressor(datPath); err != nil {
		return nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", iit.ii.filenameBase, startTxNum, endTxNum, err)
	}
	ps.Delete(p)

	if err := iit.ii.buildMapIdx(ctx, fromStep, toStep, outItem.decompressor, ps); err != nil {
		return nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", iit.ii.filenameBase, startTxNum, endTxNum, err)
	}
	if outItem.index, err = recsplit.OpenIndex(iit.ii.efAccessorFilePath(fromStep, toStep)); err != nil {
		return nil, err
	}

	closeItem = false
	return outItem, nil
}

func (ht *HistoryRoTx) mergeFiles(ctx context.Context, indexFiles, historyFiles []*filesItem, r HistoryRanges, ps *background.ProgressSet) (indexIn, historyIn *filesItem, err error) {
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
	if indexIn, err = ht.iit.mergeFiles(ctx, indexFiles, r.indexStartTxNum, r.indexEndTxNum, ps); err != nil {
		return nil, nil, err
	}
	if r.history {
		for _, f := range indexFiles {
			defer f.decompressor.EnableReadAhead().DisableReadAhead()
		}
		for _, f := range historyFiles {
			defer f.decompressor.EnableReadAhead().DisableReadAhead()
		}

		var comp *seg.Compressor
		var decomp *seg.Decompressor
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
		fromStep, toStep := r.historyStartTxNum/ht.h.aggregationStep, r.historyEndTxNum/ht.h.aggregationStep
		datPath := ht.h.vFilePath(fromStep, toStep)
		idxPath := ht.h.vAccessorFilePath(fromStep, toStep)
		if comp, err = seg.NewCompressor(ctx, "merge hist "+ht.h.filenameBase, datPath, ht.h.dirs.Tmp, seg.MinPatternScore, ht.h.compressWorkers, log.LvlTrace, ht.h.logger); err != nil {
			return nil, nil, fmt.Errorf("merge %s history compressor: %w", ht.h.filenameBase, err)
		}
		compr := NewArchiveWriter(comp, ht.h.compression)
		if ht.h.noFsync {
			compr.DisableFsync()
		}
		p := ps.AddNew(path.Base(datPath), 1)
		defer ps.Delete(p)

		var cp CursorHeap
		heap.Init(&cp)
		for _, item := range indexFiles {
			g := NewArchiveGetter(item.decompressor.MakeGetter(), ht.h.compression)
			g.Reset(0)
			if g.HasNext() {
				var g2 ArchiveGetter
				for _, hi := range historyFiles { // full-scan, because it's ok to have different amount files. by unclean-shutdown.
					if hi.startTxNum == item.startTxNum && hi.endTxNum == item.endTxNum {
						g2 = NewArchiveGetter(hi.decompressor.MakeGetter(), ht.h.compression)
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
				// fmt.Printf("fput '%x'->%x\n", lastKey, ci1.val)
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
		if decomp, err = seg.NewDecompressor(datPath); err != nil {
			return nil, nil, err
		}
		ps.Delete(p)

		p = ps.AddNew(path.Base(idxPath), uint64(decomp.Count()/2))
		defer ps.Delete(p)
		if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   keyCount,
			Enums:      false,
			BucketSize: 2000,
			LeafSize:   8,
			TmpDir:     ht.h.dirs.Tmp,
			IndexFile:  idxPath,
			Salt:       ht.h.salt,
			NoFsync:    ht.h.noFsync,
		}, ht.h.logger); err != nil {
			return nil, nil, fmt.Errorf("create recsplit: %w", err)
		}
		rs.LogLvl(log.LvlTrace)

		var (
			txKey      [8]byte
			historyKey []byte
			keyBuf     []byte
			valOffset  uint64
		)

		g := NewArchiveGetter(indexIn.decompressor.MakeGetter(), ht.h.InvertedIndex.compression)
		g2 := NewArchiveGetter(decomp.MakeGetter(), ht.h.compression)

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
					txNum, err := efIt.Next()
					if err != nil {
						return nil, nil, err
					}
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
					return nil, nil, fmt.Errorf("build %s idx: %w", ht.h.filenameBase, err)
				}
			} else {
				break
			}
		}
		rs.Close()
		rs = nil
		if index, err = recsplit.OpenIndex(idxPath); err != nil {
			return nil, nil, fmt.Errorf("open %s idx: %w", ht.h.filenameBase, err)
		}
		historyIn = newFilesItem(r.historyStartTxNum, r.historyEndTxNum, ht.h.aggregationStep)
		historyIn.decompressor = decomp
		historyIn.index = index

		closeItem = false
	}

	closeIndex = false
	return
}

func (d *Domain) integrateMergedDirtyFiles(valuesOuts, indexOuts, historyOuts []*filesItem, valuesIn, indexIn, historyIn *filesItem) {
	d.History.integrateMergedFiles(indexOuts, historyOuts, indexIn, historyIn)
	if valuesIn != nil {
		d.dirtyFiles.Set(valuesIn)

		// `kill -9` may leave some garbage
		// but it still may be useful for merges, until we finish merge frozen file
		d.dirtyFiles.Walk(func(items []*filesItem) bool {
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
		d.dirtyFiles.Delete(out)
		out.canDelete.Store(true)
	}
}

func (ii *InvertedIndex) integrateMergedDirtyFiles(outs []*filesItem, in *filesItem) {
	if in != nil {
		ii.dirtyFiles.Set(in)

		// `kill -9` may leave some garbage
		// but it still may be useful for merges, until we finish merge frozen file
		if in.frozen {
			ii.dirtyFiles.Walk(func(items []*filesItem) bool {
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
		ii.dirtyFiles.Delete(out)

		if ii.filenameBase == traceFileLife {
			ii.logger.Warn(fmt.Sprintf("[agg] mark can delete: %s, triggered by merge of: %s", out.decompressor.FileName(), in.decompressor.FileName()))
		}
		out.canDelete.Store(true)
	}
}

func (h *History) integrateMergedFiles(indexOuts, historyOuts []*filesItem, indexIn, historyIn *filesItem) {
	h.InvertedIndex.integrateMergedDirtyFiles(indexOuts, indexIn)
	//TODO: handle collision
	if historyIn != nil {
		h.dirtyFiles.Set(historyIn)

		// `kill -9` may leave some garbage
		// but it still may be useful for merges, until we finish merge frozen file
		if historyIn.frozen {
			h.dirtyFiles.Walk(func(items []*filesItem) bool {
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
		h.dirtyFiles.Delete(out)
		out.canDelete.Store(true)
	}
}

func (dt *DomainRoTx) cleanAfterMerge(mergedDomain, mergedHist, mergedIdx *filesItem) {
	dt.ht.cleanAfterMerge(mergedHist, mergedIdx)
	if mergedDomain == nil {
		return
	}
	outs := dt.garbage(mergedDomain)
	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + dt.d.filenameBase)
		}
		dt.d.dirtyFiles.Delete(out)
		out.canDelete.Store(true)
		if out.refcount.Load() == 0 {
			if dt.d.filenameBase == traceFileLife && out.decompressor != nil {
				dt.d.logger.Info(fmt.Sprintf("[agg] cleanAfterMerge remove: %s", out.decompressor.FileName()))
			}
			// if it has no readers (invisible even for us) - it's safe to remove file right here
			out.closeFilesAndRemove()
		} else {
			if dt.d.filenameBase == traceFileLife && out.decompressor != nil {
				dt.d.logger.Warn(fmt.Sprintf("[agg] cleanAfterMerge mark as delete: %s, refcnt=%d", out.decompressor.FileName(), out.refcount.Load()))
			}
		}
	}
}

// cleanAfterMerge - sometime inverted_index may be already merged, but history not yet. and power-off happening.
// in this case we need keep small files, but when history already merged to `frozen` state - then we can cleanup
// all earlier small files, by mark tem as `canDelete=true`
func (ht *HistoryRoTx) cleanAfterMerge(merged, mergedIdx *filesItem) {
	if merged == nil {
		return
	}
	if merged.endTxNum == 0 {
		return
	}
	outs := ht.garbage(merged)
	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + ht.h.filenameBase)
		}
		ht.h.dirtyFiles.Delete(out)
		out.canDelete.Store(true)

		// if it has no readers (invisible even for us) - it's safe to remove file right here
		if out.refcount.Load() == 0 {
			if ht.h.filenameBase == traceFileLife && out.decompressor != nil {
				ht.h.logger.Info(fmt.Sprintf("[agg] cleanAfterMerge remove: %s", out.decompressor.FileName()))
			}
			out.closeFilesAndRemove()
		} else {
			if ht.h.filenameBase == traceFileLife && out.decompressor != nil {
				ht.h.logger.Info(fmt.Sprintf("[agg] cleanAfterMerge mark as delete: %s", out.decompressor.FileName()))
			}
		}
	}
	ht.iit.cleanAfterMerge(mergedIdx)
}

// cleanAfterMerge - mark all small files before `f` as `canDelete=true`
func (iit *InvertedIndexRoTx) cleanAfterMerge(merged *filesItem) {
	if merged == nil {
		return
	}
	if merged.endTxNum == 0 {
		return
	}
	outs := iit.garbage(merged)
	for _, out := range outs {
		if out == nil {
			panic("must not happen: " + iit.ii.filenameBase)
		}
		iit.ii.dirtyFiles.Delete(out)
		out.canDelete.Store(true)
		if out.refcount.Load() == 0 {
			if iit.ii.filenameBase == traceFileLife && out.decompressor != nil {
				iit.ii.logger.Info(fmt.Sprintf("[agg] cleanAfterMerge remove: %s", out.decompressor.FileName()))
			}
			// if it has no readers (invisible even for us) - it's safe to remove file right here
			out.closeFilesAndRemove()
		} else {
			if iit.ii.filenameBase == traceFileLife && out.decompressor != nil {
				iit.ii.logger.Info(fmt.Sprintf("[agg] cleanAfterMerge mark as delete: %s\n", out.decompressor.FileName()))
			}
		}
	}
}

// garbage - returns list of garbage files after merge step is done. at startup pass here last frozen file
func (dt *DomainRoTx) garbage(merged *filesItem) (outs []*filesItem) {
	if merged == nil {
		return
	}
	// `kill -9` may leave some garbage
	// AggContext doesn't have such files, only Agg.files does
	dt.d.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen {
				continue
			}
			if item.isSubsetOf(merged) {
				if dt.d.restrictSubsetFileDeletions {
					continue
				}
				fmt.Printf("garbage: %s is subset of %s", item.decompressor.FileName(), merged.decompressor.FileName())
				outs = append(outs, item)
			}
			// delete garbage file only if it's before merged range and it has bigger file (which indexed and visible for user now - using `DomainRoTx`)
			if item.isBefore(merged) && dt.hasCoverFile(item) {
				outs = append(outs, item)
			}
		}
		return true
	})
	return outs
}

// garbage - returns list of garbage files after merge step is done. at startup pass here last frozen file
func (ht *HistoryRoTx) garbage(merged *filesItem) (outs []*filesItem) {
	if merged == nil {
		return
	}
	// `kill -9` may leave some garbage
	// AggContext doesn't have such files, only Agg.files does
	ht.h.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen {
				continue
			}
			if item.isSubsetOf(merged) {
				outs = append(outs, item)
			}
			// delete garbage file only if it's before merged range and it has bigger file (which indexed and visible for user now - using `DomainRoTx`)
			if item.isBefore(merged) && ht.hasCoverFile(item) {
				outs = append(outs, item)
			}
		}
		return true
	})
	return outs
}

func (iit *InvertedIndexRoTx) garbage(merged *filesItem) (outs []*filesItem) {
	if merged == nil {
		return
	}
	// `kill -9` may leave some garbage
	// AggContext doesn't have such files, only Agg.files does
	iit.ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen {
				continue
			}
			if item.isSubsetOf(merged) {
				outs = append(outs, item)
			}
			// delete garbage file only if it's before merged range and it has bigger file (which indexed and visible for user now - using `DomainRoTx`)
			if item.isBefore(merged) && iit.hasCoverFile(item) {
				outs = append(outs, item)
			}
		}
		return true
	})
	return outs
}
func (dt *DomainRoTx) hasCoverFile(item *filesItem) bool {
	for _, f := range dt.files {
		if item.isSubsetOf(f.src) {
			return true
		}
	}
	return false
}
func (ht *HistoryRoTx) hasCoverFile(item *filesItem) bool {
	for _, f := range ht.files {
		if item.isSubsetOf(f.src) {
			return true
		}
	}
	return false
}
func (iit *InvertedIndexRoTx) hasCoverFile(item *filesItem) bool {
	for _, f := range iit.files {
		if item.isSubsetOf(f.src) {
			return true
		}
	}
	return false
}
