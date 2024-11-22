// Copyright 2024 The Erigon Authors
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
	"context"
	"github.com/erigontech/erigon-lib/common/datadir"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"

	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
)

func emptyTestInvertedIndex(aggStep uint64) *InvertedIndex {
	salt := uint32(1)
	cfg := iiCfg{salt: &salt, dirs: datadir.New(os.TempDir()), db: nil, aggregationStep: aggStep, filenameBase: "test", indexList: 0}

	ii, err := NewInvertedIndex(cfg, log.New())
	ii.indexList = 0
	if err != nil {
		panic(err)
	}
	return ii
}
func TestFindMergeRangeCornerCases(t *testing.T) {
	t.Parallel()

	t.Run("ii: > 2 unmerged files", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		ic := ii.BeginFilesRo()
		defer ic.Close()

		mr := ic.findMergeRange(4, 32)
		assert.True(t, mr.needMerge)
		assert.Equal(t, 0, int(mr.from))
		assert.Equal(t, 4, int(mr.to))

		idxF := ic.staticFilesInRange(mr.from, mr.to)
		assert.Equal(t, 3, len(idxF))
	})
	t.Run("hist: > 2 unmerged files", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h := &History{
			histCfg:       histCfg{filenameBase: "test"},
			InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.2-3.v",
			"v1-test.3-4.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()
		r := hc.findMergeRange(4, 32)
		assert.True(t, r.history.needMerge)
		assert.Equal(t, 2, int(r.history.to))
		assert.Equal(t, 2, int(r.index.to))
	})
	t.Run("not equal amount of files", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h := &History{
			histCfg:       histCfg{filenameBase: "test"},
			InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.True(t, r.index.needMerge)
		assert.True(t, r.history.needMerge)
		assert.Equal(t, 0, int(r.history.from))
		assert.Equal(t, 2, int(r.history.to))
		assert.Equal(t, 2, int(r.index.to))
	})
	t.Run("idx merged, history not yet", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h := &History{
			histCfg:       histCfg{filenameBase: "test"},
			InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.True(t, r.history.needMerge)
		assert.False(t, r.index.needMerge)
		assert.Equal(t, 0, int(r.history.from))
		assert.Equal(t, 2, int(r.history.to))
	})
	t.Run("idx merged, history not yet, 2", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
			"v1-test.0-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h := &History{
			histCfg:       histCfg{filenameBase: "test"},
			InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.2-3.v",
			"v1-test.3-4.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.False(t, r.index.needMerge)
		assert.True(t, r.history.needMerge)
		assert.Equal(t, 2, int(r.history.to))
		idxFiles, histFiles, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 2, len(histFiles))
	})
	t.Run("idx merged and small files lost", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h := &History{
			histCfg:       histCfg{filenameBase: "test"},
			InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.2-3.v",
			"v1-test.3-4.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.False(t, r.index.needMerge)
		assert.True(t, r.history.needMerge)
		assert.Equal(t, 2, int(r.history.to))
		_, _, err := hc.staticFilesInRange(r)
		require.Error(t, err)
	})

	t.Run("history merged, but index not and history garbage left", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
		h := &History{
			histCfg:       histCfg{filenameBase: "test"},
			InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.0-2.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.True(t, r.index.needMerge)
		assert.False(t, r.history.needMerge)
		assert.Equal(t, uint64(2), r.index.to)
		idxFiles, histFiles, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 0, len(histFiles))
	})
	t.Run("history merge progress ahead of idx", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
			"v1-test.0-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h := &History{
			histCfg:       histCfg{filenameBase: "test"},
			InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.0-2.v",
			"v1-test.2-3.v",
			"v1-test.3-4.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.True(t, r.index.needMerge)
		assert.True(t, r.history.needMerge)
		assert.Equal(t, 4, int(r.index.to))
		idxFiles, histFiles, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 3, len(idxFiles))
		require.Equal(t, 3, len(histFiles))
	})
	t.Run("idx merge progress ahead of history", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
			"v1-test.0-2.ef",
			"v1-test.2-3.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h := &History{
			histCfg:       histCfg{filenameBase: "test"},
			InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.2-3.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.False(t, r.index.needMerge)
		assert.True(t, r.history.needMerge)
		assert.Equal(t, 2, int(r.history.to))
		idxFiles, histFiles, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 2, len(histFiles))
	})
	t.Run("idx merged, but garbage left", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
			"v1-test.0-2.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanDirtyFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.0-2.v",
			"v1-test.2-3.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		hc := h.BeginFilesRo()
		defer hc.Close()
		r := hc.findMergeRange(4, 32)
		assert.False(t, r.index.needMerge)
		assert.False(t, r.history.needMerge)
	})
	t.Run("idx merged, but garbage left2", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanDirtyFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
			"v1-test.0-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())
		ic := ii.BeginFilesRo()
		defer ic.Close()
		mr := ic.findMergeRange(4, 32)
		assert.True(t, mr.needMerge)
		require.Equal(t, 0, int(mr.from))
		require.Equal(t, 4, int(mr.to))
		idxFiles := ic.staticFilesInRange(mr.from, mr.to)
		require.Equal(t, 3, len(idxFiles))
	})
}
func Test_mergeEliasFano(t *testing.T) {
	t.Skip()

	firstList := []int{1, 298164, 298163, 13, 298160, 298159}
	sort.Ints(firstList)
	uniq := make(map[int]struct{})

	first := eliasfano32.NewEliasFano(uint64(len(firstList)), uint64(firstList[len(firstList)-1]))
	for _, v := range firstList {
		uniq[v] = struct{}{}
		first.AddOffset(uint64(v))
	}
	first.Build()
	firstBytes := first.AppendBytes(nil)

	fit := first.Iterator()
	for fit.HasNext() {
		v, _ := fit.Next()
		require.Contains(t, firstList, int(v))
	}

	secondList := []int{
		1, 644951, 644995, 682653, 13,
		644988, 644987, 644946, 644994,
		644942, 644945, 644941, 644940,
		644939, 644938, 644792, 644787}
	sort.Ints(secondList)
	second := eliasfano32.NewEliasFano(uint64(len(secondList)), uint64(secondList[len(secondList)-1]))

	for _, v := range secondList {
		second.AddOffset(uint64(v))
		uniq[v] = struct{}{}
	}
	second.Build()
	secondBytes := second.AppendBytes(nil)

	sit := second.Iterator()
	for sit.HasNext() {
		v, _ := sit.Next()
		require.Contains(t, secondList, int(v))
	}

	menc, err := mergeEfs(firstBytes, secondBytes, nil)
	require.NoError(t, err)

	merged, _ := eliasfano32.ReadEliasFano(menc)
	require.NoError(t, err)
	require.EqualValues(t, len(uniq), merged.Count())
	require.EqualValues(t, merged.Count(), eliasfano32.Count(menc))
	mergedLists := append(firstList, secondList...)
	sort.Ints(mergedLists)
	require.EqualValues(t, mergedLists[len(mergedLists)-1], merged.Max())
	require.EqualValues(t, merged.Max(), eliasfano32.Max(menc))

	mit := merged.Iterator()
	for mit.HasNext() {
		v, _ := mit.Next()
		require.Contains(t, mergedLists, int(v))
	}
}

func TestMergeFiles(t *testing.T) {
	t.Parallel()

	db, d := testDbAndDomain(t, log.New())
	defer db.Close()
	defer d.Close()

	dc := d.BeginFilesRo()
	defer dc.Close()

	txs := d.aggregationStep * 8
	data := generateTestData(t, 20, 52, txs, txs, 100)

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	w := dc.NewWriter()

	prev := []byte{}
	prevStep := uint64(0)
	for key, upd := range data {
		for _, v := range upd {
			w.SetTxNum(v.txNum)
			err := w.PutWithPrev([]byte(key), nil, v.value, prev, prevStep)

			prev, prevStep = v.value, v.txNum/d.aggregationStep
			require.NoError(t, err)
		}
	}

	require.NoError(t, w.Flush(context.Background(), rwTx))
	w.close()
	err = rwTx.Commit()
	require.NoError(t, err)

	collateAndMerge(t, db, nil, d, txs)

	rwTx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	dc = d.BeginFilesRo()
	defer dc.Close()

}
