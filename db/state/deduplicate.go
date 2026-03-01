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
	"path"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
)

// this is an internal function which helps to filter content of the history snapshots files
// and deduplicate values in it.
//
// This function is supposed to be used only as part of the snapshot tooling
// to help rebuilding existing snapshots. It should not be used for for
// background merging process because it is not memory-efficient
func (ht *HistoryRoTx) deduplicateFiles(ctx context.Context, indexFiles, historyFiles []*FilesItem, r HistoryRanges, ps *background.ProgressSet) error {
	if !r.any() {
		return nil
	}

	if len(indexFiles) > 1 || len(historyFiles) > 1 {
		return fmt.Errorf("wrong deduplication interval from %d to %d", r.history.from, r.history.to)
	}

	var decomp *seg.Decompressor

	fromStep, toStep := kv.Step(r.history.from/ht.stepSize), kv.Step(r.history.to/ht.stepSize)
	datPath := ht.h.vNewFilePath(fromStep, toStep)
	idxPath := ht.h.vAccessorNewFilePath(fromStep, toStep)

	comp, err := seg.NewCompressor(ctx, "dedup hist "+ht.h.FilenameBase, datPath, ht.h.dirs.Tmp, ht.h.CompressorCfg, log.LvlTrace, ht.h.logger)
	if err != nil {
		return fmt.Errorf("deduo %s history compressor: %w", ht.h.FilenameBase, err)
	}

	pagedWr := ht.dataWriter(comp)

	var cp CursorHeap
	heap.Init(&cp)

	dedupKeyEFs := make(map[string]map[uint64]struct{}) // string because slice can not be a key in Golang

	for _, item := range indexFiles {
		defer item.closeFilesAndRemove()

		g := ht.iit.dataReader(item.decompressor)
		g.Reset(0)
		if g.HasNext() {
			var g2 *seg.PagedReader
			for _, hi := range historyFiles { // full-scan, because it's ok to have different amount files. by unclean-shutdown.
				if hi.startTxNum == item.startTxNum && hi.endTxNum == item.endTxNum {
					g2 = seg.NewPagedReader(ht.dataReader(hi.decompressor), true)
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
				kvReader:   g,
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
	var dedupCount int
	for cp.Len() > 0 {
		lastKey = append(lastKey[:0], cp[0].key...)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(&cp).(*CursorItem)
			count := multiencseq.Count(ci1.startTxNum, ci1.val)

			seq := multiencseq.ReadMultiEncSeq(ci1.startTxNum, ci1.val)
			ss := seq.Iterator(0)

			var dedupVal *[]byte
			var histKeyBuf []byte
			var prevTxNum uint64

			for ss.HasNext() {
				txNum, err := ss.Next()
				if err != nil {
					panic(fmt.Sprintf("failed to extract txNum from ef. File: %s Key: %x", ci1.kvReader.FileName(), ci1.key))
				}

				if !ci1.hist.HasNext() {
					panic(fmt.Errorf("assert: no value??? %s, txNum=%d, count=%d, lastKey=%x, ci1.key=%x", ci1.hist.FileName(), txNum, count, lastKey, ci1.key))
				}

				v, _ := ci1.hist.Next(valBuf[:0])

				// if dedupVal == nil -> we can not insert to the page because next val can be duplicate --> remember the value and decide next iter
				if dedupVal == nil {
					dd := bytes.Clone(v) // i am not sure if there is a way to avoid extra copy here
					dedupVal = &dd
					prevTxNum = txNum

					continue
				}

				// if dedupVal is the same as current --> can not insert to the page bacause next val can be duplicate as well --> mark prev tx as duplicate
				if bytes.Equal(*dedupVal, v) {
					if dedupKeyEFs[string(ci1.key)] == nil {
						dedupKeyEFs[string(ci1.key)] = make(map[uint64]struct{})
					}

					dedupKeyEFs[string(ci1.key)][prevTxNum] = struct{}{}
					prevTxNum = txNum
					dedupCount++
					continue
				}

				histKeyBuf = historyKey(prevTxNum, ci1.key, histKeyBuf)

				if err = pagedWr.Add(histKeyBuf, *dedupVal); err != nil {
					return err
				}

				dd := bytes.Clone(v) // i am not sure if there is a way to avoid extra copy here
				dedupVal = &dd
				prevTxNum = txNum
			}

			if dedupVal != nil {
				histKeyBuf = historyKey(prevTxNum, ci1.key, histKeyBuf)

				if err = pagedWr.Add(histKeyBuf, *dedupVal); err != nil {
					return err
				}
			}

			// fmt.Printf("fput '%x'->%x\n", lastKey, ci1.val)
			if ci1.kvReader.HasNext() {
				ci1.key, _ = ci1.kvReader.Next(ci1.key[:0])
				ci1.val, _ = ci1.kvReader.Next(ci1.val[:0])
				heap.Push(&cp, ci1)
			}
		}
	}
	if err := pagedWr.Compress(); err != nil {
		return err
	}
	comp.Close()
	comp = nil
	if decomp, err = seg.NewDecompressor(datPath); err != nil {
		return err
	}

	fmt.Println("Values to deduplicate:", dedupCount)

	indexIn, err := ht.iit.deduplicateFiles(ctx, indexFiles, r.index.from, r.index.to, ps, dedupKeyEFs)
	if err != nil {
		return err
	}

	if err = ht.h.buildVI(ctx, idxPath, decomp, indexIn.decompressor, indexIn.startTxNum, ps); err != nil {
		return err
	}

	for _, item := range indexFiles {
		if item.StartStep(ht.stepSize) == indexIn.StartStep(ht.stepSize) && item.EndStep(ht.stepSize) == indexIn.EndStep(ht.stepSize) {
			continue
		}

		item.closeFilesAndRemove()
	}

	for _, item := range historyFiles {
		if item.StartStep(ht.stepSize) == indexIn.StartStep(ht.stepSize) && item.EndStep(ht.stepSize) == indexIn.EndStep(ht.stepSize) {
			continue
		}

		item.closeFilesAndRemove()
	}

	return nil
}

func (iit *InvertedIndexRoTx) deduplicateFiles(ctx context.Context, files []*FilesItem, startTxNum, endTxNum uint64, ps *background.ProgressSet, dedupKeyEFs map[string]map[uint64]struct{}) (*FilesItem, error) {
	if startTxNum == endTxNum {
		panic(fmt.Sprintf("assert: startTxNum(%d) == endTxNum(%d)", startTxNum, endTxNum))
	}

	var outItem *FilesItem
	var comp *seg.Compressor
	var err error
	var closeItem = true
	defer func() {
		if closeItem {
			if comp != nil {
				comp.Close()
			}
			if outItem != nil {
				outItem.closeFilesAndRemove()
			}
		}
	}()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	fromStep, toStep := kv.Step(startTxNum/iit.stepSize), kv.Step(endTxNum/iit.stepSize)

	datPath := iit.ii.efNewFilePath(fromStep, toStep)
	if comp, err = seg.NewCompressor(ctx, iit.ii.FilenameBase+".ii.merge", datPath, iit.ii.dirs.Tmp, iit.ii.CompressorCfg, log.LvlTrace, iit.ii.logger); err != nil {
		return nil, fmt.Errorf("merge %s inverted index compressor: %w", iit.ii.FilenameBase, err)
	}
	if iit.ii.noFsync {
		comp.DisableFsync()
	}

	write := iit.dataWriter(comp, false)
	p := ps.AddNew(path.Base(datPath), 1)
	defer ps.Delete(p)

	var cp CursorHeap
	heap.Init(&cp)

	for _, item := range files {
		g := iit.dataReader(item.decompressor)
		g.Reset(0)
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
			//fmt.Printf("heap push %s [%d] %x\n", item.decompressor.FilePath(), item.endTxNum, key)
			heap.Push(&cp, &CursorItem{
				t:          FILE_CURSOR,
				kvReader:   g,
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

		var toInsert []uint64

		for preIt.HasNext() {
			v, err := preIt.Next()
			if err != nil {
				return nil, err
			}

			if dedupKeyEFs != nil && dedupKeyEFs[string(lastKey)] != nil {
				if _, ok := dedupKeyEFs[string(lastKey)][v]; ok {
					continue
				}
			}

			toInsert = append(toInsert, v)
		}

		newSeq := multiencseq.NewBuilder(startTxNum, uint64(len(toInsert)), preSeq.Max())
		for i := range toInsert {
			newSeq.AddOffset(toInsert[i])
		}
		newSeq.Build()
		lastVal = newSeq.AppendBytes(nil)
		var mergedOnce bool

		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(&cp).(*CursorItem)
			if mergedOnce {
				var dedupEF map[uint64]struct{}

				if dedupKeyEFs != nil {
					dedupEF = dedupKeyEFs[string(lastKey)]
				}

				if lastVal, err = dedupNumSeqs(ci1.val, lastVal, ci1.startTxNum, startTxNum, nil, startTxNum, dedupEF); err != nil {
					return nil, fmt.Errorf("merge %s inverted index: %w", iit.ii.FilenameBase, err)
				}
			} else {
				mergedOnce = true
			}
			// fmt.Printf("multi-way %s [%d] %x\n", ii.KeysTable, ci1.endTxNum, ci1.key)
			if ci1.kvReader.HasNext() {
				ci1.key, _ = ci1.kvReader.Next(ci1.key[:0])
				ci1.val, _ = ci1.kvReader.Next(ci1.val[:0])
				// fmt.Printf("heap next push %s [%d] %x\n", ii.KeysTable, ci1.endTxNum, ci1.key)
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
		// fmt.Printf("Put %x->%x\n", keyBuf, valBuf)
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

	outItem = newFilesItem(startTxNum, endTxNum, iit.stepSize, iit.stepsInFrozenFile)
	if outItem.decompressor, err = seg.NewDecompressor(datPath); err != nil {
		return nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", iit.ii.FilenameBase, startTxNum, endTxNum, err)
	}
	ps.Delete(p)

	if err := iit.ii.buildMapAccessor(ctx, fromStep, toStep, iit.dataReader(outItem.decompressor), ps); err != nil {
		return nil, fmt.Errorf("merge %s buildHashMapAccessor [%d-%d]: %w", iit.ii.FilenameBase, startTxNum, endTxNum, err)
	}
	if outItem.index, err = iit.ii.openHashMapAccessor(iit.ii.efAccessorNewFilePath(fromStep, toStep)); err != nil {
		return nil, err
	}

	closeItem = false
	return outItem, nil
}

func dedupNumSeqs(preval, val []byte, preBaseNum, baseNum uint64, buf []byte, outBaseNum uint64, dedupEF map[uint64]struct{}) ([]byte, error) {
	preSeq := multiencseq.ReadMultiEncSeq(preBaseNum, preval)
	seq := multiencseq.ReadMultiEncSeq(baseNum, val)
	preIt := preSeq.Iterator(0)
	efIt := seq.Iterator(0)

	var toInsert []uint64

	for preIt.HasNext() {
		v, err := preIt.Next()
		if err != nil {
			return nil, err
		}

		if dedupEF != nil {
			if _, ok := dedupEF[v]; ok {
				continue
			}
		}

		toInsert = append(toInsert, v)
	}
	for efIt.HasNext() {
		v, err := efIt.Next()
		if err != nil {
			return nil, err
		}

		if dedupEF != nil {
			if _, ok := dedupEF[v]; ok {
				continue
			}
		}

		toInsert = append(toInsert, v)
	}

	newSeq := multiencseq.NewBuilder(outBaseNum, uint64(len(toInsert)), seq.Max())
	for i := range toInsert {
		newSeq.AddOffset(toInsert[i])
	}

	newSeq.Build()
	return newSeq.AppendBytes(buf), nil
}
