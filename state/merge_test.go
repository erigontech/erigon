package state

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func TestFindMergeRangeCornerCases(t *testing.T) {
	t.Run("> 2 unmerged files", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-2.ef",
			"test.2-3.ef",
			"test.3-4.ef",
		})
		ii.reCalcRoFiles()

		ic := ii.MakeContext()
		defer ic.Close()

		needMerge, from, to := ii.findMergeRange(4, 32)
		assert.True(t, needMerge)
		assert.Equal(t, 0, int(from))
		assert.Equal(t, 4, int(to))

		idxF, _ := ic.staticFilesInRange(from, to)
		assert.Equal(t, 3, len(idxF))

		ii = &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-1.ef",
			"test.1-2.ef",
			"test.2-3.ef",
			"test.3-4.ef",
		})
		ii.reCalcRoFiles()
		ic = ii.MakeContext()
		defer ic.Close()

		needMerge, from, to = ii.findMergeRange(4, 32)
		assert.True(t, needMerge)
		assert.Equal(t, 0, int(from))
		assert.Equal(t, 2, int(to))

		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
			"test.2-3.v",
			"test.3-4.v",
		})
		h.reCalcRoFiles()
		ic = ii.MakeContext()
		defer ic.Close()

		r := h.findMergeRange(4, 32)
		assert.True(t, r.history)
		assert.Equal(t, 2, int(r.historyEndTxNum))
		assert.Equal(t, 2, int(r.indexEndTxNum))
	})
	t.Run("not equal amount of files", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-1.ef",
			"test.1-2.ef",
			"test.2-3.ef",
			"test.3-4.ef",
		})
		ii.reCalcRoFiles()

		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
		})
		h.reCalcRoFiles()

		hc := h.MakeContext()
		defer hc.Close()

		r := h.findMergeRange(4, 32)
		assert.True(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 0, int(r.historyStartTxNum))
		assert.Equal(t, 2, int(r.historyEndTxNum))
		assert.Equal(t, 2, int(r.indexEndTxNum))
	})
	t.Run("idx merged, history not yet", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-2.ef",
			"test.2-3.ef",
			"test.3-4.ef",
		})
		ii.reCalcRoFiles()

		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
		})
		h.reCalcRoFiles()

		hc := h.MakeContext()
		defer hc.Close()

		r := h.findMergeRange(4, 32)
		assert.True(t, r.history)
		assert.False(t, r.index)
		assert.Equal(t, 0, int(r.historyStartTxNum))
		assert.Equal(t, 2, int(r.historyEndTxNum))
	})
	t.Run("idx merged, history not yet, 2", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-1.ef",
			"test.1-2.ef",
			"test.2-3.ef",
			"test.3-4.ef",
			"test.0-4.ef",
		})
		ii.reCalcRoFiles()

		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
			"test.2-3.v",
			"test.3-4.v",
		})
		h.reCalcRoFiles()

		hc := h.MakeContext()
		defer hc.Close()

		r := h.findMergeRange(4, 32)
		assert.False(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 2, int(r.historyEndTxNum))
		idxFiles, histFiles, _, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 2, len(histFiles))
	})
	t.Run("idx merged and small files lost", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-4.ef",
		})
		ii.reCalcRoFiles()

		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
			"test.2-3.v",
			"test.3-4.v",
		})
		h.reCalcRoFiles()

		hc := h.MakeContext()
		defer hc.Close()

		r := h.findMergeRange(4, 32)
		assert.False(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 2, int(r.historyEndTxNum))
		_, _, _, err := hc.staticFilesInRange(r)
		require.Error(t, err)
	})

	t.Run("history merged, but index not and history garbage left", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-1.ef",
			"test.1-2.ef",
		})
		ii.reCalcRoFiles()

		// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
			"test.0-2.v",
		})
		h.reCalcRoFiles()

		hc := h.MakeContext()
		defer hc.Close()

		r := h.findMergeRange(4, 32)
		assert.True(t, r.index)
		assert.False(t, r.history)
		assert.Equal(t, uint64(2), r.indexEndTxNum)
		idxFiles, histFiles, _, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 0, len(histFiles))
	})
	t.Run("history merge progress ahead of idx", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-1.ef",
			"test.1-2.ef",
			"test.0-2.ef",
			"test.2-3.ef",
			"test.3-4.ef",
		})
		ii.reCalcRoFiles()

		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
			"test.0-2.v",
			"test.2-3.v",
			"test.3-4.v",
		})
		h.reCalcRoFiles()

		hc := h.MakeContext()
		defer hc.Close()

		r := h.findMergeRange(4, 32)
		assert.True(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 4, int(r.indexEndTxNum))
		idxFiles, histFiles, _, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 3, len(idxFiles))
		require.Equal(t, 3, len(histFiles))
	})
	t.Run("idx merge progress ahead of history", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-1.ef",
			"test.1-2.ef",
			"test.0-2.ef",
			"test.2-3.ef",
		})
		ii.reCalcRoFiles()

		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
			"test.2-3.v",
		})
		h.reCalcRoFiles()

		hc := h.MakeContext()
		defer hc.Close()

		r := h.findMergeRange(4, 32)
		assert.False(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 2, int(r.historyEndTxNum))
		idxFiles, histFiles, _, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 2, len(histFiles))
	})
	t.Run("idx merged, but garbage left", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-1.ef",
			"test.1-2.ef",
			"test.0-2.ef",
		})
		ii.reCalcRoFiles()

		h := &History{InvertedIndex: ii, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"test.0-1.v",
			"test.1-2.v",
			"test.0-2.v",
			"test.2-3.v",
		})
		h.reCalcRoFiles()

		hc := h.MakeContext()
		defer hc.Close()
		r := h.findMergeRange(4, 32)
		assert.False(t, r.index)
		assert.False(t, r.history)
	})
	t.Run("idx merged, but garbage left2", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1, files: btree2.NewBTreeG[*filesItem](filesItemLess)}
		ii.scanStateFiles([]string{
			"test.0-1.ef",
			"test.1-2.ef",
			"test.0-2.ef",
			"test.2-3.ef",
			"test.3-4.ef",
		})
		ii.reCalcRoFiles()
		ic := ii.MakeContext()
		defer ic.Close()
		needMerge, from, to := ii.findMergeRange(4, 32)
		assert.True(t, needMerge)
		require.Equal(t, 0, int(from))
		require.Equal(t, 4, int(to))
		idxFiles, _ := ic.staticFilesInRange(from, to)
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
