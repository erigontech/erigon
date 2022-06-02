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
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
)

func (d *Domain) endTxNumMinimax() uint64 {
	var minimax uint64
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		if d.files[fType].Len() > 0 {
			endTxNum := d.files[fType].Max().(*filesItem).endTxNum
			if minimax == 0 || endTxNum < minimax {
				minimax = endTxNum
			}
		}
	}
	return minimax
}

func (ii *InvertedIndex) endTxNumMinimax() uint64 {
	var minimax uint64
	if ii.files.Len() > 0 {
		endTxNum := ii.files.Max().(*filesItem).endTxNum
		if minimax == 0 || endTxNum < minimax {
			minimax = endTxNum
		}
	}
	return minimax
}

// findMergeRange assumes that all fTypes in d.files have items at least as far as maxEndTxNum
// That is why only Values type is inspected
func (d *Domain) findMergeRange(maxEndTxNum, maxSpan uint64) (bool, uint64, uint64) {
	var minFound bool
	var startTxNum, endTxNum uint64
	d.files[Values].Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		if item.endTxNum > maxEndTxNum {
			return false
		}
		endStep := item.endTxNum / d.aggregationStep
		spanStep := endStep & -endStep // Extract rightmost bit in the binary representation of endStep, this corresponds to size of maximally possible merge ending at endStep
		span := spanStep * d.aggregationStep
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

func (ii *InvertedIndex) findMergeRange(maxEndTxNum, maxSpan uint64) (bool, uint64, uint64) {
	var minFound bool
	var startTxNum, endTxNum uint64
	ii.files.Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
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

// staticFilesInRange returns list of static files with txNum in specified range [startTxNum; endTxNum)
// files are in the descending order of endTxNum
func (d *Domain) staticFilesInRange(startTxNum, endTxNum uint64) ([][NumberOfTypes]*filesItem, int) {
	var files [][NumberOfTypes]*filesItem
	var startJ int
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		startJ = 0
		j := 0
		d.files[fType].Ascend(func(i btree.Item) bool {
			item := i.(*filesItem)
			if item.startTxNum < startTxNum {
				startJ++
				return true
			}
			if item.endTxNum > endTxNum {
				return false
			}
			for j >= len(files) {
				files = append(files, [NumberOfTypes]*filesItem{})
			}
			files[j][fType] = item
			j++
			return true
		})
	}
	return files, startJ
}

func (ii *InvertedIndex) staticFilesInRange(startTxNum, endTxNum uint64) ([]*filesItem, int) {
	var files []*filesItem
	var startJ int
	j := 0
	ii.files.Ascend(func(i btree.Item) bool {
		item := i.(*filesItem)
		if item.startTxNum < startTxNum {
			startJ++
			return true
		}
		if item.endTxNum > endTxNum {
			return false
		}
		for j >= len(files) {
			files = append(files, item)
		}
		j++
		return true
	})
	return files, startJ
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

func (d *Domain) mergeFiles(files [][NumberOfTypes]*filesItem, startTxNum, endTxNum uint64, maxSpan uint64) ([NumberOfTypes]*filesItem, error) {
	var outItems [NumberOfTypes]*filesItem
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
			for fType := FileType(0); fType < NumberOfTypes; fType++ {
				outItem := outItems[fType]
				if outItem != nil {
					if outItem.decompressor != nil {
						outItem.decompressor.Close()
					}
					if outItem.index != nil {
						outItem.index.Close()
					}
					outItems[fType] = nil
				}
			}
		}
	}()
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		valCompressed := fType != EfHistory
		removeVals := fType == History && (endTxNum-startTxNum) == maxSpan
		tmpPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.tmp", d.filenameBase, fType.String(), startTxNum, endTxNum))
		datPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.dat", d.filenameBase, fType.String(), startTxNum, endTxNum))
		if removeVals {
			if comp, err = compress.NewCompressor(context.Background(), "merge", tmpPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return outItems, fmt.Errorf("merge %s history compressor: %w", d.filenameBase, err)
			}
		} else {
			if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return outItems, fmt.Errorf("merge %s history compressor: %w", d.filenameBase, err)
			}
		}
		var cp CursorHeap
		heap.Init(&cp)
		for _, filesByType := range files {
			item := filesByType[fType]
			g := item.decompressor.MakeGetter()
			g.Reset(0)
			if g.HasNext() {
				key, _ := g.Next(nil)
				val, _ := g.Next(nil)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: g, key: key, val: val, endTxNum: item.endTxNum})
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
			var mergedOnce bool
			// Advance all the items that have this key (including the top)
			for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
				ci1 := cp[0]
				if mergedOnce && fType == EfHistory {
					if lastVal, err = mergeEfs(ci1.val, lastVal, nil); err != nil {
						return outItems, fmt.Errorf("merge %s efhistory: %w", d.filenameBase, err)
					}
				} else {
					mergedOnce = true
				}
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.Next(ci1.key[:0])
					if valCompressed {
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
			if fType == Values {
				if d.prefixLen > 0 {
					skip = startTxNum == 0 && len(lastVal) == 0 && len(lastKey) != d.prefixLen
				} else {
					// For the rest of types, empty value means deletion
					skip = startTxNum == 0 && len(lastVal) == 0
				}
			}
			if !skip {
				if keyBuf != nil && (d.prefixLen == 0 || len(keyBuf) != d.prefixLen || bytes.HasPrefix(lastKey, keyBuf)) {
					if err = comp.AddWord(keyBuf); err != nil {
						return outItems, err
					}
					count++ // Only counting keys, not values
					if valCompressed {
						if err = comp.AddWord(valBuf); err != nil {
							return outItems, err
						}
					} else {
						if err = comp.AddUncompressedWord(valBuf); err != nil {
							return outItems, err
						}
					}
				}
				keyBuf = append(keyBuf[:0], lastKey...)
				valBuf = append(valBuf[:0], lastVal...)
			}
		}
		if keyBuf != nil {
			if err = comp.AddWord(keyBuf); err != nil {
				return outItems, err
			}
			count++ // Only counting keys, not values
			if valCompressed {
				if err = comp.AddWord(valBuf); err != nil {
					return outItems, err
				}
			} else {
				if err = comp.AddUncompressedWord(valBuf); err != nil {
					return outItems, err
				}
			}
		}
		if err = comp.Compress(); err != nil {
			return outItems, err
		}
		comp.Close()
		comp = nil
		idxPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.idx", d.filenameBase, fType.String(), startTxNum, endTxNum))
		outItem := &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		outItems[fType] = outItem
		if removeVals {
			if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, d.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
				return outItems, fmt.Errorf("merge %s remove vals compressor: %w", d.filenameBase, err)
			}
			if decomp, err = compress.NewDecompressor(tmpPath); err != nil {
				return outItems, fmt.Errorf("merge %s remove vals decompressor %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			g := decomp.MakeGetter()
			var val []byte
			var count int
			g.Reset(0)
			for g.HasNext() {
				g.Skip() // Skip key on on the first pass
				val, _ = g.Next(val[:0])
				if err = comp.AddWord(val); err != nil {
					return outItems, fmt.Errorf("merge %s remove vals add val %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
				}
				count++
			}
			if err = comp.Compress(); err != nil {
				return outItems, err
			}
			comp = nil
			if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				return outItems, fmt.Errorf("merge %s remove vals decompressor(no val) %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			var rs *recsplit.RecSplit
			if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
				KeyCount:   count,
				Enums:      false,
				BucketSize: 2000,
				LeafSize:   8,
				TmpDir:     d.dir,
				StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
					0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
					0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
				IndexFile: idxPath,
			}); err != nil {
				return outItems, fmt.Errorf("merge %s remove vals recsplit %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			g1 := outItem.decompressor.MakeGetter()
			var key []byte
			for {
				g.Reset(0)
				g1.Reset(0)
				var lastOffset uint64
				for g.HasNext() {
					key, _ = g.Next(key[:0])
					g.Skip() // Skip value
					_, pos := g1.Next(nil)
					if err = rs.AddKey(key, lastOffset); err != nil {
						return outItems, fmt.Errorf("merge %s remove vals recsplit add key %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
					}
					lastOffset = pos
				}
				if err = rs.Build(); err != nil {
					if rs.Collision() {
						log.Info("Building reduceHistoryFiles. Collision happened. It's ok. Restarting...")
						rs.ResetNextSalt()
					} else {
						return outItems, fmt.Errorf("merge %s remove vals recsplit build %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
					}
				} else {
					break
				}
			}
			decomp.Close()
			decomp = nil
			if outItem.index, err = recsplit.OpenIndex(idxPath); err != nil {
				return outItems, fmt.Errorf("merge %s open index %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
		} else {
			if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				return outItems, fmt.Errorf("merge %s decompressor %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
			if outItem.index, err = buildIndex(outItem.decompressor, idxPath, d.dir, count, fType == History /* values */); err != nil {
				return outItems, fmt.Errorf("merge %s buildIndex %s [%d-%d]: %w", d.filenameBase, fType.String(), startTxNum, endTxNum, err)
			}
		}
		outItem.getter = outItem.decompressor.MakeGetter()
		outItem.getterMerge = outItem.decompressor.MakeGetter()
		outItem.indexReader = recsplit.NewIndexReader(outItem.index)
		outItem.readerMerge = recsplit.NewIndexReader(outItem.index)
	}
	closeItem = false
	return outItems, nil
}

func (ii *InvertedIndex) mergeFiles(files []*filesItem, startTxNum, endTxNum uint64, maxSpan uint64) (*filesItem, error) {
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
	datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.dat", ii.filenameBase, startTxNum, endTxNum))
	if comp, err = compress.NewCompressor(context.Background(), "merge", datPath, ii.dir, compress.MinPatternScore, 1, log.LvlDebug); err != nil {
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
			heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: g, key: key, val: val, endTxNum: item.endTxNum})
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
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.Next(ci1.key[:0])
				ci1.val, _ = ci1.dg.NextUncompressed()
				heap.Fix(&cp, 0)
			} else {
				heap.Pop(&cp)
			}
		}
		if keyBuf != nil {
			if err = comp.AddWord(keyBuf); err != nil {
				return nil, err
			}
			count++ // Only counting keys, not values
			if err = comp.AddUncompressedWord(valBuf); err != nil {
				return nil, err
			}
		}
		keyBuf = append(keyBuf[:0], lastKey...)
		valBuf = append(valBuf[:0], lastVal...)
	}
	if keyBuf != nil {
		if err = comp.AddWord(keyBuf); err != nil {
			return nil, err
		}
		count++ // Only counting keys, not values
		if err = comp.AddUncompressedWord(valBuf); err != nil {
			return nil, err
		}
	}
	if err = comp.Compress(); err != nil {
		return nil, err
	}
	comp.Close()
	comp = nil
	idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.idx", ii.filenameBase, startTxNum, endTxNum))
	outItem = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
	if outItem.decompressor, err = compress.NewDecompressor(datPath); err != nil {
		return nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	if outItem.index, err = buildIndex(outItem.decompressor, idxPath, ii.dir, count, false /* values */); err != nil {
		return nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", ii.filenameBase, startTxNum, endTxNum, err)
	}
	outItem.getter = outItem.decompressor.MakeGetter()
	outItem.getterMerge = outItem.decompressor.MakeGetter()
	outItem.indexReader = recsplit.NewIndexReader(outItem.index)
	outItem.readerMerge = recsplit.NewIndexReader(outItem.index)
	closeItem = false
	return outItem, nil
}

func (d *Domain) integrateMergedFiles(outs [][NumberOfTypes]*filesItem, in [NumberOfTypes]*filesItem) {
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		d.files[fType].ReplaceOrInsert(in[fType])
		for _, out := range outs {
			d.files[fType].Delete(out[fType])
			out[fType].decompressor.Close()
			out[fType].index.Close()
		}
	}
}

func (ii *InvertedIndex) integrateMergedFiles(outs []*filesItem, in *filesItem) {
	ii.files.ReplaceOrInsert(in)
	for _, out := range outs {
		ii.files.Delete(out)
		out.decompressor.Close()
		out.index.Close()
	}
}

func (d *Domain) deleteFiles(outs [][NumberOfTypes]*filesItem) error {
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		for _, out := range outs {
			datPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.dat", d.filenameBase, fType.String(), out[fType].startTxNum, out[fType].endTxNum))
			if err := os.Remove(datPath); err != nil {
				return err
			}
			idxPath := filepath.Join(d.dir, fmt.Sprintf("%s-%s.%d-%d.idx", d.filenameBase, fType.String(), out[fType].startTxNum, out[fType].endTxNum))
			if err := os.Remove(idxPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ii *InvertedIndex) deleteFiles(outs []*filesItem) error {
	for _, out := range outs {
		datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.dat", ii.filenameBase, out.startTxNum, out.endTxNum))
		if err := os.Remove(datPath); err != nil {
			return err
		}
		idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.idx", ii.filenameBase, out.startTxNum, out.endTxNum))
		if err := os.Remove(idxPath); err != nil {
			return err
		}
	}
	return nil
}
