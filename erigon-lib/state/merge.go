// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"strings"

	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/page"
	"github.com/erigontech/erigon-lib/datastruct/existence"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/multiencseq"
	"github.com/erigontech/erigon-lib/seg"
)

func (d *Domain) dirtyFilesEndTxNumMinimax() uint64 {
	if d == nil {
		return 0
	}

	minimax := d.History.dirtyFilesEndTxNumMinimax()
	if _max, ok := d.dirtyFiles.Max(); ok {
		endTxNum := _max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

func (ii *InvertedIndex) dirtyFilesEndTxNumMinimax() uint64 {
	var minimax uint64
	if _max, ok := ii.dirtyFiles.Max(); ok {
		endTxNum := _max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}
func (h *History) dirtyFilesEndTxNumMinimax() uint64 {
	if h.snapshotsDisabled {
		return math.MaxUint64
	}
	minimax := h.InvertedIndex.dirtyFilesEndTxNumMinimax()
	if _max, ok := h.dirtyFiles.Max(); ok {
		endTxNum := _max.endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

type DomainRanges struct {
	name    kv.Domain
	values  MergeRange
	history HistoryRanges
	aggStep uint64
}

func NewDomainRanges(name kv.Domain, values MergeRange, history HistoryRanges, aggStep uint64) DomainRanges {
	return DomainRanges{name: name, values: values, history: history, aggStep: aggStep}
}

func (r DomainRanges) String() string {
	var b strings.Builder
	if r.values.needMerge {
		b.WriteString(r.values.String("val", r.aggStep))
	}
	if r.history.any() {
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.history.String(r.aggStep))
	}
	return b.String()
}

func (r DomainRanges) any() bool { return r.values.needMerge || r.history.any() }

func (dt *DomainRoTx) FirstStepNotInFiles() uint64 { return dt.files.EndTxNum() / dt.d.aggregationStep }
func (ht *HistoryRoTx) FirstStepNotInFiles() uint64 {
	return ht.files.EndTxNum() / ht.h.aggregationStep
}
func (iit *InvertedIndexRoTx) FirstStepNotInFiles() uint64 {
	return iit.files.EndTxNum() / iit.ii.aggregationStep
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
		name:    domainName,
		history: hr,
		aggStep: dt.d.aggregationStep,
	}
	for _, item := range dt.files {
		if item.endTxNum > maxEndTxNum {
			break
		}
		endStep := item.endTxNum / dt.d.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * dt.d.aggregationStep
		fromTxNum := item.endTxNum - span
		if fromTxNum < item.startTxNum {
			if !r.values.needMerge || fromTxNum < r.values.from {
				r.values = MergeRange{"", true, fromTxNum, item.endTxNum}
			}
		}
	}
	return r
}

func (ht *HistoryRoTx) findMergeRange(maxEndTxNum, maxSpan uint64) HistoryRanges {
	var r HistoryRanges
	mr := ht.iit.findMergeRange(maxEndTxNum, maxSpan)
	r.index = *mr

	for _, item := range ht.files {
		if item.endTxNum > maxEndTxNum {
			continue
		}
		endStep := item.endTxNum / ht.h.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := min(spanStep*ht.h.aggregationStep, maxSpan)
		startTxNum := item.endTxNum - span

		foundSuperSet := r.history.from == item.startTxNum && item.endTxNum >= r.history.to
		if foundSuperSet {
			r.history = MergeRange{from: startTxNum, to: item.endTxNum}
		} else if startTxNum < item.startTxNum {
			if !r.history.needMerge || startTxNum < r.history.from {
				r.history = MergeRange{"", true, startTxNum, item.endTxNum}
			}
		}
	}

	if r.history.needMerge && r.index.needMerge {
		// history is behind idx: then merge only history
		historyIsAhead := r.history.to > r.index.to
		if historyIsAhead {
			r.history = MergeRange{}
			return r
		}

		historyIsBehind := r.history.to < r.index.to
		if historyIsBehind {
			r.index = MergeRange{}
			return r
		}
	}
	return r
}

// 0-1,1-2,2-3,3-4: allow merge 0-4
// 0-2,2-3,3-4: allow merge 0-4
// 0-2,2-4: allow merge 0-4
//
// 0-1,1-2,2-3: allow merge 0-2
//
// 0-2,2-3: nothing to merge
func (iit *InvertedIndexRoTx) findMergeRange(maxEndTxNum, maxSpan uint64) *MergeRange {
	var minFound bool
	var startTxNum, endTxNum uint64
	for _, item := range iit.files {
		if item.endTxNum > maxEndTxNum {
			continue
		}
		endStep := item.endTxNum / iit.ii.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := min(spanStep*iit.ii.aggregationStep, maxSpan)
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
	return &MergeRange{iit.name.String(), minFound, startTxNum, endTxNum}
}

type HistoryRanges struct {
	history MergeRange
	index   MergeRange
}

func NewHistoryRanges(history MergeRange, index MergeRange) HistoryRanges {
	return HistoryRanges{history: history, index: index}
}

func (r HistoryRanges) String(aggStep uint64) string {
	var str string
	if r.history.needMerge {
		str += r.history.String("hist", aggStep)
	}
	if r.index.needMerge {
		if str != "" {
			str += ", "
		}
		str += r.index.String("idx", aggStep)
	}
	return str
}
func (r HistoryRanges) any() bool {
	return r.history.needMerge || r.index.needMerge
}

// staticFilesInRange returns list of static files with txNum in specified range [startTxNum; endTxNum)
// files are in the descending order of endTxNum
func (dt *DomainRoTx) staticFilesInRange(r DomainRanges) (valuesFiles, indexFiles, historyFiles []*filesItem) {
	if r.history.any() {
		var err error
		indexFiles, historyFiles, err = dt.ht.staticFilesInRange(r.history)
		if err != nil {
			panic(err)
		}
	}
	if r.values.needMerge {
		for _, item := range dt.files {
			if item.startTxNum < r.values.from {
				continue
			}
			if item.endTxNum > r.values.to {
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

func (iit *InvertedIndexRoTx) staticFilesInRange(startTxNum, endTxNum uint64) []*filesItem {
	files := make([]*filesItem, 0, len(iit.files))

	for _, item := range iit.files {
		if item.startTxNum < startTxNum {
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

	return files
}

func (ht *HistoryRoTx) staticFilesInRange(r HistoryRanges) (indexFiles, historyFiles []*filesItem, err error) {
	if !r.history.needMerge && r.index.needMerge {
		indexFiles = ht.iit.staticFilesInRange(r.index.from, r.index.to)
		return indexFiles, historyFiles, nil
	}

	if r.history.needMerge {
		// Get history files from HistoryRoTx (no "garbage/overalps"), but index files not from InvertedIndexRoTx
		// because index files may already be merged (before `kill -9`) and it means not visible in InvertedIndexRoTx
		for _, item := range ht.files {
			if item.startTxNum < r.history.from {
				continue
			}
			if item.endTxNum > r.history.to {
				break
			}

			historyFiles = append(historyFiles, item.src)
			idxFile, ok := ht.h.InvertedIndex.dirtyFiles.Get(item.src)
			if ok {
				indexFiles = append(indexFiles, idxFile)
			} else {
				walkErr := fmt.Errorf("History.staticFilesInRange: required file not found: %s-%s.%d-%d.efi", ht.h.InvertedIndex.version.AccessorEFI.String(), ht.h.filenameBase, item.startTxNum/ht.h.aggregationStep, item.endTxNum/ht.h.aggregationStep)
				return nil, nil, walkErr
			}
		}

		for _, f := range historyFiles {
			if f == nil {
				panic("must not happen")
			}
		}
		if r.index.needMerge && len(indexFiles) != len(historyFiles) {
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

func mergeNumSeqs(preval, val []byte, preBaseNum, baseNum uint64, buf []byte, outBaseNum uint64) ([]byte, error) {
	preSeq := multiencseq.ReadMultiEncSeq(preBaseNum, preval)
	seq := multiencseq.ReadMultiEncSeq(baseNum, val)
	preIt := preSeq.Iterator(0)
	efIt := seq.Iterator(0)
	newSeq := multiencseq.NewBuilder(outBaseNum, preSeq.Count()+seq.Count(), seq.Max())
	for preIt.HasNext() {
		v, err := preIt.Next()
		if err != nil {
			return nil, err
		}
		newSeq.AddOffset(v)
	}
	for efIt.HasNext() {
		v, err := efIt.Next()
		if err != nil {
			return nil, err
		}
		newSeq.AddOffset(v)
	}
	newSeq.Build()
	return newSeq.AppendBytes(buf), nil
}

type valueTransformer func(val []byte, startTxNum, endTxNum uint64) ([]byte, error)

const DomainMinStepsToCompress = 16

func (dt *DomainRoTx) mergeFiles(ctx context.Context, domainFiles, indexFiles, historyFiles []*filesItem, r DomainRanges, vt valueTransformer, ps *background.ProgressSet) (valuesIn, indexIn, historyIn *filesItem, err error) {
	if !r.any() {
		return
	}
	defer func() {
		// Merge is background operation. It must not crush application.
		// Convert panic to error.
		if rec := recover(); rec != nil {
			err = fmt.Errorf("[snapshots] background mergeFiles: domain=%s, %s, %s, %s", dt.name, r.String(), rec, dbg.Stack())
		}
	}()

	closeFiles := true
	var kvWriter *seg.Writer
	defer func() {
		if closeFiles {
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
	if indexIn, historyIn, err = dt.ht.mergeFiles(ctx, indexFiles, historyFiles, r.history, ps); err != nil {
		return nil, nil, nil, err
	}

	if !r.values.needMerge {
		closeFiles = false
		return
	}

	fromStep, toStep := r.values.from/r.aggStep, r.values.to/r.aggStep
	kvFilePath := dt.d.kvFilePath(fromStep, toStep)

	kvFile, err := seg.NewCompressor(ctx, "merge domain "+dt.d.filenameBase, kvFilePath, dt.d.dirs.Tmp, dt.d.CompressCfg, log.LvlTrace, dt.d.logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s compressor: %w", dt.d.filenameBase, err)
	}

	compression := dt.d.Compression
	if toStep-fromStep < DomainMinStepsToCompress {
		compression = seg.CompressNone
	}
	kvWriter = seg.NewWriter(kvFile, compression)
	if dt.d.noFsync {
		kvWriter.DisableFsync()
	}
	p := ps.AddNew("merge "+path.Base(kvFilePath), 1)
	defer ps.Delete(p)

	var cp CursorHeap
	heap.Init(&cp)
	for _, item := range domainFiles {
		g := seg.NewReader(item.decompressor.MakeGetter(), dt.d.Compression)
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
			heap.Push(&cp, &CursorItem{
				t:          FILE_CURSOR,
				idx:        g,
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
	var lastKey, lastVal []byte
	var keyFileStartTxNum, keyFileEndTxNum uint64
	for cp.Len() > 0 {
		lastKey = append(lastKey[:0], cp[0].key...)
		lastVal = append(lastVal[:0], cp[0].val...)
		lastFileStartTxNum, lastFileEndTxNum := cp[0].startTxNum, cp[0].endTxNum
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(&cp).(*CursorItem)
			if ci1.idx.HasNext() {
				ci1.key, _ = ci1.idx.Next(ci1.key[:0])
				ci1.val, _ = ci1.idx.Next(ci1.val[:0])
				heap.Push(&cp, ci1)
			}
		}

		// For the rest of types, empty value means deletion
		deleted := r.values.from == 0 && len(lastVal) == 0
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
				if _, err = kvWriter.Write(keyBuf); err != nil {
					return nil, nil, nil, err
				}
				if _, err = kvWriter.Write(valBuf); err != nil {
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
		if _, err = kvWriter.Write(keyBuf); err != nil {
			return nil, nil, nil, err
		}
		if _, err = kvWriter.Write(valBuf); err != nil {
			return nil, nil, nil, err
		}
	}
	if err = kvWriter.Compress(); err != nil {
		return nil, nil, nil, err
	}
	kvWriter.Close()
	kvWriter = nil
	ps.Delete(p)

	valuesIn = newFilesItem(r.values.from, r.values.to, dt.d.aggregationStep)
	valuesIn.frozen = false
	if valuesIn.decompressor, err = seg.NewDecompressor(kvFilePath); err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
	}

	if dt.d.Accessors.Has(AccessorBTree) {
		btPath := dt.d.kvBtAccessorFilePath(fromStep, toStep)
		btM := DefaultBtreeM
		if toStep == 0 && dt.d.filenameBase == "commitment" {
			btM = 128
		}
		valuesIn.bindex, err = CreateBtreeIndexWithDecompressor(btPath, btM, valuesIn.decompressor, dt.d.Compression, *dt.salt, ps, dt.d.dirs.Tmp, dt.d.logger, dt.d.noFsync, dt.d.Accessors)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s btindex [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
		}
	}
	if dt.d.Accessors.Has(AccessorHashMap) {
		if err = dt.d.buildHashMapAccessor(ctx, fromStep, toStep, valuesIn.decompressor, ps); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildHashMapAccessor [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
		}
		if valuesIn.index, err = recsplit.OpenIndex(dt.d.kviAccessorFilePath(fromStep, toStep)); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildHashMapAccessor [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
		}
	}

	if dt.d.Accessors.Has(AccessorExistence) {
		bloomIndexPath := dt.d.kvExistenceIdxFilePath(fromStep, toStep)
		exists, err := dir.FileExist(bloomIndexPath)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s FileExist err [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
		}
		if exists {
			valuesIn.existence, err = existence.OpenFilter(bloomIndexPath)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("merge %s existence [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
			}
		}
	}

	closeFiles = false
	return
}

func (iit *InvertedIndexRoTx) mergeFiles(ctx context.Context, files []*filesItem, startTxNum, endTxNum uint64, ps *background.ProgressSet) (*filesItem, error) {
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
	if comp, err = seg.NewCompressor(ctx, "merge idx "+iit.ii.filenameBase, datPath, iit.ii.dirs.Tmp, iit.ii.CompressorCfg, log.LvlTrace, iit.ii.logger); err != nil {
		return nil, fmt.Errorf("merge %s inverted index compressor: %w", iit.ii.filenameBase, err)
	}
	if iit.ii.noFsync {
		comp.DisableFsync()
	}
	write := seg.NewWriter(comp, iit.ii.Compression)
	p := ps.AddNew(path.Base(datPath), 1)
	defer ps.Delete(p)

	var cp CursorHeap
	heap.Init(&cp)

	for _, item := range files {
		g := seg.NewReader(item.decompressor.MakeGetter(), iit.ii.Compression)
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
			//fmt.Printf("heap push %s [%d] %x\n", item.decompressor.FilePath(), item.endTxNum, key)
			heap.Push(&cp, &CursorItem{
				t:          FILE_CURSOR,
				idx:        g,
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
	var lastKey, lastVal []byte
	for cp.Len() > 0 {
		lastKey = append(lastKey[:0], cp[0].key...)
		lastVal = append(lastVal[:0], cp[0].val...)

		// Pre-rebase the first sequence
		preSeq := multiencseq.ReadMultiEncSeq(cp[0].startTxNum, lastVal)
		preIt := preSeq.Iterator(0)
		newSeq := multiencseq.NewBuilder(startTxNum, preSeq.Count(), preSeq.Max())
		for preIt.HasNext() {
			v, err := preIt.Next()
			if err != nil {
				return nil, err
			}
			newSeq.AddOffset(v)
		}
		newSeq.Build()
		lastVal = newSeq.AppendBytes(nil)
		var mergedOnce bool

		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(&cp).(*CursorItem)
			if mergedOnce {
				if lastVal, err = mergeNumSeqs(ci1.val, lastVal, ci1.startTxNum, startTxNum, nil, startTxNum); err != nil {
					return nil, fmt.Errorf("merge %s inverted index: %w", iit.ii.filenameBase, err)
				}
			} else {
				mergedOnce = true
			}
			// fmt.Printf("multi-way %s [%d] %x\n", ii.keysTable, ci1.endTxNum, ci1.key)
			if ci1.idx.HasNext() {
				ci1.key, _ = ci1.idx.Next(ci1.key[:0])
				ci1.val, _ = ci1.idx.Next(ci1.val[:0])
				// fmt.Printf("heap next push %s [%d] %x\n", ii.keysTable, ci1.endTxNum, ci1.key)
				heap.Push(&cp, ci1)
			}
		}
		if keyBuf != nil {
			// fmt.Printf("pput %x->%x\n", keyBuf, valBuf)
			if _, err = write.Write(keyBuf); err != nil {
				return nil, err
			}
			if _, err = write.Write(valBuf); err != nil {
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
		if _, err = write.Write(keyBuf); err != nil {
			return nil, err
		}
		if _, err = write.Write(valBuf); err != nil {
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

	if err := iit.ii.buildMapAccessor(ctx, fromStep, toStep, outItem.decompressor, ps); err != nil {
		return nil, fmt.Errorf("merge %s buildHashMapAccessor [%d-%d]: %w", iit.ii.filenameBase, startTxNum, endTxNum, err)
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

	if indexIn, err = ht.iit.mergeFiles(ctx, indexFiles, r.index.from, r.index.to, ps); err != nil {
		return nil, nil, err
	}
	if r.history.needMerge {
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
		fromStep, toStep := r.history.from/ht.h.aggregationStep, r.history.to/ht.h.aggregationStep
		datPath := ht.h.vFilePath(fromStep, toStep)
		idxPath := ht.h.vAccessorFilePath(fromStep, toStep)
		if comp, err = seg.NewCompressor(ctx, "merge hist "+ht.h.filenameBase, datPath, ht.h.dirs.Tmp, ht.h.CompressorCfg, log.LvlTrace, ht.h.logger); err != nil {
			return nil, nil, fmt.Errorf("merge %s history compressor: %w", ht.h.filenameBase, err)
		}
		compr := seg.NewWriter(comp, ht.h.Compression)
		if ht.h.noFsync {
			compr.DisableFsync()
		}
		pagedWr := page.NewWriter(compr, ht.h.historyValuesOnCompressedPage, true)
		p := ps.AddNew(path.Base(datPath), 1)
		defer ps.Delete(p)

		var cp CursorHeap
		heap.Init(&cp)
		for _, item := range indexFiles {
			g := seg.NewReader(item.decompressor.MakeGetter(), ht.h.Compression)
			g.Reset(0)
			if g.HasNext() {
				var g2 *seg.PagedReader
				for _, hi := range historyFiles { // full-scan, because it's ok to have different amount files. by unclean-shutdown.
					if hi.startTxNum == item.startTxNum && hi.endTxNum == item.endTxNum {
						g2 = seg.NewPagedReader(seg.NewReader(hi.decompressor.MakeGetter(), ht.h.Compression), ht.h.historyValuesOnCompressedPage, true)
						break
					}
				}
				if g2 == nil {
					panic(fmt.Sprintf("for file: %s, not found corresponding file to merge", g.FileName()))
				}
				key, _ := g.Next(nil)
				val, _ := g.Next(nil)
				heap.Push(&cp, &CursorItem{
					t:          FILE_CURSOR,
					idx:        g,
					hist:       g2,
					key:        key,
					val:        val,
					startTxNum: item.startTxNum,
					endTxNum:   item.endTxNum,
					reverse:    false,
				})
			}
		}
		// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
		// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
		// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
		// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
		// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
		var lastKey, valBuf []byte
		var keyCount int
		for cp.Len() > 0 {
			lastKey = append(lastKey[:0], cp[0].key...)
			// Advance all the items that have this key (including the top)
			for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
				ci1 := heap.Pop(&cp).(*CursorItem)
				count := multiencseq.Count(ci1.startTxNum, ci1.val)
				for i := uint64(0); i < count; i++ {
					if !ci1.hist.HasNext() {
						panic(fmt.Errorf("assert: no value??? %s, i=%d, count=%d, lastKey=%x, ci1.key=%x", ci1.hist.FileName(), i, count, lastKey, ci1.key))
					}

					var k, v []byte
					k, v, valBuf, _ = ci1.hist.Next2(valBuf[:0])
					if err = pagedWr.Add(k, v); err != nil {
						return nil, nil, err
					}
				}

				// fmt.Printf("fput '%x'->%x\n", lastKey, ci1.val)
				keyCount += int(count)
				if ci1.idx.HasNext() {
					ci1.key, _ = ci1.idx.Next(ci1.key[:0])
					ci1.val, _ = ci1.idx.Next(ci1.val[:0])
					heap.Push(&cp, ci1)
				}
			}
		}
		if err := pagedWr.Flush(); err != nil {
			return nil, nil, err
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

		if err = ht.h.buildVI(ctx, idxPath, decomp, indexIn.decompressor, indexIn.startTxNum, ps); err != nil {
			return nil, nil, err
		}

		if index, err = recsplit.OpenIndex(idxPath); err != nil {
			return nil, nil, fmt.Errorf("open %s idx: %w", ht.h.filenameBase, err)
		}
		historyIn = newFilesItem(r.history.from, r.history.to, ht.h.aggregationStep)
		historyIn.decompressor = decomp
		historyIn.index = index

		closeItem = false
	}

	closeIndex = false
	return
}

func (d *Domain) integrateMergedDirtyFiles(valuesIn, indexIn, historyIn *filesItem) {
	d.History.integrateMergedDirtyFiles(indexIn, historyIn)
	if valuesIn != nil {
		d.dirtyFiles.Set(valuesIn)
	}
}

func (ii *InvertedIndex) integrateMergedDirtyFiles(in *filesItem) {
	if in != nil {
		ii.dirtyFiles.Set(in)
	}
}

func (h *History) integrateMergedDirtyFiles(indexIn, historyIn *filesItem) {
	h.InvertedIndex.integrateMergedDirtyFiles(indexIn)
	//TODO: handle collision
	if historyIn != nil {
		h.dirtyFiles.Set(historyIn)
	}
}

func (dt *DomainRoTx) cleanAfterMerge(mergedDomain, mergedHist, mergedIdx *filesItem) {
	dt.ht.cleanAfterMerge(mergedHist, mergedIdx)
	outs := dt.garbage(mergedDomain)
	deleteMergeFile(dt.d.dirtyFiles, outs, dt.d.filenameBase, dt.d.logger)
}

// cleanAfterMerge - sometime inverted_index may be already merged, but history not yet. and power-off happening.
// in this case we need keep small files, but when history already merged to `frozen` state - then we can cleanup
// all earlier small files, by mark tem as `canDelete=true`
func (ht *HistoryRoTx) cleanAfterMerge(merged, mergedIdx *filesItem) {
	ht.iit.cleanAfterMerge(mergedIdx)
	if merged != nil && merged.endTxNum == 0 {
		return
	}
	outs := ht.garbage(merged)
	deleteMergeFile(ht.h.dirtyFiles, outs, ht.h.filenameBase, ht.h.logger)
}

// cleanAfterMerge - mark all small files before `f` as `canDelete=true`
func (iit *InvertedIndexRoTx) cleanAfterMerge(merged *filesItem) {
	if merged != nil && merged.endTxNum == 0 {
		return
	}
	outs := iit.garbage(merged)
	deleteMergeFile(iit.ii.dirtyFiles, outs, iit.ii.filenameBase, iit.ii.logger)
}

// garbage - returns list of garbage files after merge step is done. at startup pass here last frozen file
func (dt *DomainRoTx) garbage(merged *filesItem) (outs []*filesItem) {
	var checker func(startTxNum, endTxNum uint64) bool
	dchecker := dt.d.checker
	dname := dt.d.name
	if dchecker != nil {
		checker = func(startTxNum, endTxNum uint64) bool {
			return dchecker.CheckDependentPresent(dname, Any, startTxNum, endTxNum)
		}
	}
	return garbage(dt.d.dirtyFiles, dt.files, merged, checker)
}

// garbage - returns list of garbage files after merge step is done. at startup pass here last frozen file
func (ht *HistoryRoTx) garbage(merged *filesItem) (outs []*filesItem) {
	return garbage(ht.h.dirtyFiles, ht.files, merged, nil)
}

func (iit *InvertedIndexRoTx) garbage(merged *filesItem) (outs []*filesItem) {
	return garbage(iit.ii.dirtyFiles, iit.files, merged, nil)
}

func garbage(dirtyFiles *btree.BTreeG[*filesItem], visibleFiles []visibleFile, merged *filesItem, checker func(startTxNum, endTxNum uint64) bool) (outs []*filesItem) {
	// `kill -9` may leave some garbage
	// AggRoTx doesn't have such files, only Agg.files does
	dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen {
				continue
			}

			if merged == nil {
				if hasCoverVisibleFile(visibleFiles, item) {
					outs = append(outs, item)
				}
				continue
			}
			// this case happens when in previous process run, the merged file was created,
			// but the processed ended before subsumed files could be deleted.
			// delete garbage file only if it's before merged range and it has bigger file (which indexed and visible for user now - using `DomainRoTx`)
			if item.isBefore(merged) && hasCoverVisibleFile(visibleFiles, item) {
				outs = append(outs, item)
				continue
			}

			if item.isProperSubsetOf(merged) {
				if checker == nil || !checker(item.startTxNum, item.endTxNum) {
					// no dependent file is present for item, can delete safely...
					outs = append(outs, item)
				}
			}
		}
		return true
	})
	return outs
}

func hasCoverVisibleFile(visibleFiles []visibleFile, item *filesItem) bool {
	for _, f := range visibleFiles {
		if item.isProperSubsetOf(f.src) {
			return true
		}
	}
	return false
}

type Ranges struct {
	domain        [kv.DomainLen]DomainRanges
	invertedIndex []*MergeRange
}

func NewRanges(domain [kv.DomainLen]DomainRanges, invertedIndex []*MergeRange) Ranges {
	return Ranges{domain: domain, invertedIndex: invertedIndex}
}

func (r Ranges) String() string {
	ss := []string{}
	for _, d := range &r.domain {
		if d.any() {
			ss = append(ss, fmt.Sprintf("%s(%s)", d.name, d.String()))
		}
	}

	aggStep := r.domain[kv.AccountsDomain].aggStep
	for _, mr := range r.invertedIndex {
		if mr != nil && mr.needMerge {
			ss = append(ss, fmt.Sprintf("%s(%d-%d)", mr.name, mr.from/aggStep, mr.to/aggStep))
		}
	}
	return strings.Join(ss, ", ")
}

func (r Ranges) any() bool {
	for _, d := range &r.domain {
		if d.any() {
			return true
		}
	}
	for _, ii := range r.invertedIndex {
		if ii != nil && ii.needMerge {
			return true
		}
	}
	return false
}
