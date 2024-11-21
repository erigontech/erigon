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
	"encoding/binary"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"strings"

	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
)

func (d *Domain) dirtyFilesEndTxNumMinimax() uint64 {
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
				r.values = MergeRange{true, fromTxNum, item.endTxNum}
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
			r.history = MergeRange{false, startTxNum, item.endTxNum}
		} else if startTxNum < item.startTxNum {
			if !r.history.needMerge || startTxNum < r.history.from {
				r.history = MergeRange{true, startTxNum, item.endTxNum}
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

// 0-1,1-2,2-3,3-4: allow merge 0-1
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
	return &MergeRange{minFound, startTxNum, endTxNum}
}

type HistoryRanges struct {
	history MergeRange
	index   MergeRange
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

func (dt *DomainRoTx) BuildOptionalMissedIndices(ctx context.Context, ps *background.ProgressSet) (err error) {
	if err := dt.ht.iit.BuildOptionalMissedIndices(ctx, ps); err != nil {
		return err
	}
	return nil
}

func (iit *InvertedIndexRoTx) BuildOptionalMissedIndices(ctx context.Context, ps *background.ProgressSet) (err error) {
	return nil
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
				walkErr := fmt.Errorf("History.staticFilesInRange: required file not found: v1-%s.%d-%d.efi", ht.h.filenameBase, item.startTxNum/ht.h.aggregationStep, item.endTxNum/ht.h.aggregationStep)
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

const DomainMinStepsToCompress = 16

func (dt *DomainRoTx) mergeFiles(ctx context.Context, domainFiles, indexFiles, historyFiles []*filesItem, r DomainRanges, vt valueTransformer, ps *background.ProgressSet) (valuesIn, indexIn, historyIn *filesItem, err error) {
	if !r.any() {
		return
	}

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

	for _, f := range domainFiles {
		f := f
		defer f.decompressor.EnableReadAhead().DisableReadAhead()
	}

	fromStep, toStep := r.values.from/r.aggStep, r.values.to/r.aggStep
	kvFilePath := dt.d.kvFilePath(fromStep, toStep)

	kvFile, err := seg.NewCompressor(ctx, "merge domain "+dt.d.filenameBase, kvFilePath, dt.d.dirs.Tmp, dt.d.compressCfg, log.LvlTrace, dt.d.logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s compressor: %w", dt.d.filenameBase, err)
	}

	compression := dt.d.compression
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
		g := seg.NewReader(item.decompressor.MakeGetter(), dt.d.compression)
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

	valuesIn = newFilesItem(r.values.from, r.values.to, dt.d.aggregationStep)
	valuesIn.frozen = false
	if valuesIn.decompressor, err = seg.NewDecompressor(kvFilePath); err != nil {
		return nil, nil, nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
	}

	if UseBpsTree {
		btPath := dt.d.kvBtFilePath(fromStep, toStep)
		btM := DefaultBtreeM
		if toStep == 0 && dt.d.filenameBase == "commitment" {
			btM = 128
		}
		valuesIn.bindex, err = CreateBtreeIndexWithDecompressor(btPath, btM, valuesIn.decompressor, dt.d.compression, *dt.d.salt, ps, dt.d.dirs.Tmp, dt.d.logger, dt.d.noFsync)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s btindex [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
		}
	} else {
		if err = dt.d.buildAccessor(ctx, fromStep, toStep, valuesIn.decompressor, ps); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildAccessor [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
		}
		if valuesIn.index, err = recsplit.OpenIndex(dt.d.kvAccessorFilePath(fromStep, toStep)); err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s buildAccessor [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
		}
	}

	{
		bloomIndexPath := dt.d.kvExistenceIdxFilePath(fromStep, toStep)
		exists, err := dir.FileExist(bloomIndexPath)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("merge %s FileExist err [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
		}
		if exists {
			valuesIn.existence, err = OpenExistenceFilter(bloomIndexPath)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("merge %s existence [%d-%d]: %w", dt.d.filenameBase, r.values.from, r.values.to, err)
			}
		}
	}

	closeFiles = false
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
	if comp, err = seg.NewCompressor(ctx, "merge idx "+iit.ii.filenameBase, datPath, iit.ii.dirs.Tmp, iit.ii.compressCfg, log.LvlTrace, iit.ii.logger); err != nil {
		return nil, fmt.Errorf("merge %s inverted index compressor: %w", iit.ii.filenameBase, err)
	}
	if iit.ii.noFsync {
		comp.DisableFsync()
	}
	write := seg.NewWriter(comp, iit.ii.compression)
	p := ps.AddNew(path.Base(datPath), 1)
	defer ps.Delete(p)

	var cp CursorHeap
	heap.Init(&cp)

	for _, item := range files {
		g := seg.NewReader(item.decompressor.MakeGetter(), iit.ii.compression)
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

	if err := iit.ii.buildMapAccessor(ctx, fromStep, toStep, outItem.decompressor, ps); err != nil {
		return nil, fmt.Errorf("merge %s buildAccessor [%d-%d]: %w", iit.ii.filenameBase, startTxNum, endTxNum, err)
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
		fromStep, toStep := r.history.from/ht.h.aggregationStep, r.history.to/ht.h.aggregationStep
		datPath := ht.h.vFilePath(fromStep, toStep)
		idxPath := ht.h.vAccessorFilePath(fromStep, toStep)
		if comp, err = seg.NewCompressor(ctx, "merge hist "+ht.h.filenameBase, datPath, ht.h.dirs.Tmp, ht.h.compressCfg, log.LvlTrace, ht.h.logger); err != nil {
			return nil, nil, fmt.Errorf("merge %s history compressor: %w", ht.h.filenameBase, err)
		}
		compr := seg.NewWriter(comp, ht.h.compression)
		if ht.h.noFsync {
			compr.DisableFsync()
		}
		p := ps.AddNew(path.Base(datPath), 1)
		defer ps.Delete(p)

		var cp CursorHeap
		heap.Init(&cp)
		for _, item := range indexFiles {
			g := seg.NewReader(item.decompressor.MakeGetter(), ht.h.compression)
			g.Reset(0)
			if g.HasNext() {
				var g2 *seg.Reader
				for _, hi := range historyFiles { // full-scan, because it's ok to have different amount files. by unclean-shutdown.
					if hi.startTxNum == item.startTxNum && hi.endTxNum == item.endTxNum {
						g2 = seg.NewReader(hi.decompressor.MakeGetter(), ht.h.compression)
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

		g := seg.NewReader(indexIn.decompressor.MakeGetter(), ht.h.InvertedIndex.compression)
		g2 := seg.NewReader(decomp.MakeGetter(), ht.h.compression)

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
		historyIn = newFilesItem(r.history.from, r.history.to, ht.h.aggregationStep)
		historyIn.decompressor = decomp
		historyIn.index = index

		closeItem = false
	}

	closeIndex = false
	return
}

func (d *Domain) integrateMergedDirtyFiles(valuesOuts, indexOuts, historyOuts []*filesItem, valuesIn, indexIn, historyIn *filesItem) {
	d.History.integrateMergedDirtyFiles(indexOuts, historyOuts, indexIn, historyIn)
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
	deleteMergeFile(ii.dirtyFiles, outs, ii.filenameBase, ii.logger)
}

func (h *History) integrateMergedDirtyFiles(indexOuts, historyOuts []*filesItem, indexIn, historyIn *filesItem) {
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
	deleteMergeFile(h.dirtyFiles, historyOuts, h.filenameBase, h.logger)
}

func (dt *DomainRoTx) cleanAfterMerge(mergedDomain, mergedHist, mergedIdx *filesItem) {
	dt.ht.cleanAfterMerge(mergedHist, mergedIdx)
	if mergedDomain == nil {
		return
	}
	outs := dt.garbage(mergedDomain)
	deleteMergeFile(dt.d.dirtyFiles, outs, dt.d.filenameBase, dt.d.logger)
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
	deleteMergeFile(ht.h.dirtyFiles, outs, ht.h.filenameBase, ht.h.logger)
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
	deleteMergeFile(iit.ii.dirtyFiles, outs, iit.ii.filenameBase, iit.ii.logger)
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
			if item.isBefore(merged) && hasCoverVisibleFile(dt.files, item) {
				outs = append(outs, item)
			}
		}
		return true
	})
	return outs
}

// garbage - returns list of garbage files after merge step is done. at startup pass here last frozen file
func (ht *HistoryRoTx) garbage(merged *filesItem) (outs []*filesItem) {
	return garbage(ht.h.dirtyFiles, ht.files, merged)
}

func (iit *InvertedIndexRoTx) garbage(merged *filesItem) (outs []*filesItem) {
	return garbage(iit.ii.dirtyFiles, iit.files, merged)
}

func garbage(dirtyFiles *btree.BTreeG[*filesItem], visibleFiles []visibleFile, merged *filesItem) (outs []*filesItem) {
	if merged == nil {
		return
	}
	// `kill -9` may leave some garbage
	// AggContext doesn't have such files, only Agg.files does
	dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.frozen {
				continue
			}
			if item.isSubsetOf(merged) {
				outs = append(outs, item)
			}
			// delete garbage file only if it's before merged range and it has bigger file (which indexed and visible for user now - using `DomainRoTx`)
			if item.isBefore(merged) && hasCoverVisibleFile(visibleFiles, item) {
				outs = append(outs, item)
			}
		}
		return true
	})
	return outs
}
func hasCoverVisibleFile(visibleFiles []visibleFile, item *filesItem) bool {
	for _, f := range visibleFiles {
		if item.isSubsetOf(f.src) {
			return true
		}
	}
	return false
}

func (ac *AggregatorRoTx) DbgDomain(idx kv.Domain) *DomainRoTx            { return ac.d[idx] }
func (ac *AggregatorRoTx) DbgII(idx kv.InvertedIdxPos) *InvertedIndexRoTx { return ac.iis[idx] }
