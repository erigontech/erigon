package state

import (
	"context"
	"sort"
	"testing"

	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func emptyTestInvertedIndex(aggStep uint64) *InvertedIndex {
	salt := uint32(1)
	logger := log.New()
	return &InvertedIndex{iiCfg: iiCfg{salt: &salt, db: nil},
		logger:       logger,
		filenameBase: "test", aggregationStep: aggStep, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
}
func TestFindMergeRangeCornerCases(t *testing.T) {
	t.Run("> 2 unmerged files", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
			"v1-test.0-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles()

		ic := ii.BeginFilesRo()
		defer ic.Close()

		mr := ic.findMergeRange(4, 32)
		assert.True(t, mr.needMerge)
		assert.Equal(t, 0, int(mr.from))
		assert.Equal(t, 4, int(mr.to))

		idxF := ic.staticFilesInRange(mr.from, mr.to)
		assert.Equal(t, 3, len(idxF))

		ii = emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
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
		ii.reCalcVisibleFiles()
		ic = ii.BeginFilesRo()
		defer ic.Close()

		mr = ic.findMergeRange(4, 32)
		assert.True(t, mr.needMerge)
		assert.Equal(t, 0, int(mr.from))
		assert.Equal(t, 2, int(mr.to))

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
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
		h.reCalcVisibleFiles()
		ic.Close()

		hc := h.BeginFilesRo()
		defer hc.Close()
		r := hc.findMergeRange(4, 32)
		assert.True(t, r.history)
		assert.Equal(t, 2, int(r.historyEndTxNum))
		assert.Equal(t, 2, int(r.indexEndTxNum))
	})
	t.Run("not equal amount of files", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
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
		ii.reCalcVisibleFiles()

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles()

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.True(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 0, int(r.historyStartTxNum))
		assert.Equal(t, 2, int(r.historyEndTxNum))
		assert.Equal(t, 2, int(r.indexEndTxNum))
	})
	t.Run("idx merged, history not yet", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
			"v1-test.0-2.ef",
			"v1-test.2-3.ef",
			"v1-test.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles()

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles()

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.True(t, r.history)
		assert.False(t, r.index)
		assert.Equal(t, 0, int(r.historyStartTxNum))
		assert.Equal(t, 2, int(r.historyEndTxNum))
	})
	t.Run("idx merged, history not yet, 2", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
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
		ii.reCalcVisibleFiles()

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
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
		h.reCalcVisibleFiles()

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.False(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 2, int(r.historyEndTxNum))
		idxFiles, histFiles, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 2, len(histFiles))
	})
	t.Run("idx merged and small files lost", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
			"v1-test.0-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles()

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
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
		h.reCalcVisibleFiles()

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.False(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 2, int(r.historyEndTxNum))
		_, _, err := hc.staticFilesInRange(r)
		require.Error(t, err)
	})

	t.Run("history merged, but index not and history garbage left", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles()

		// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.0-2.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles()

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.True(t, r.index)
		assert.False(t, r.history)
		assert.Equal(t, uint64(2), r.indexEndTxNum)
		idxFiles, histFiles, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 0, len(histFiles))
	})
	t.Run("history merge progress ahead of idx", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
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
		ii.reCalcVisibleFiles()

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
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
		h.reCalcVisibleFiles()

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.True(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 4, int(r.indexEndTxNum))
		idxFiles, histFiles, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 3, len(idxFiles))
		require.Equal(t, 3, len(histFiles))
	})
	t.Run("idx merge progress ahead of history", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
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
		ii.reCalcVisibleFiles()

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
			"v1-test.0-1.v",
			"v1-test.1-2.v",
			"v1-test.2-3.v",
		})
		h.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := h.vFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		h.reCalcVisibleFiles()

		hc := h.BeginFilesRo()
		defer hc.Close()

		r := hc.findMergeRange(4, 32)
		assert.False(t, r.index)
		assert.True(t, r.history)
		assert.Equal(t, 2, int(r.historyEndTxNum))
		idxFiles, histFiles, err := hc.staticFilesInRange(r)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxFiles))
		require.Equal(t, 2, len(histFiles))
	})
	t.Run("idx merged, but garbage left", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
			"v1-test.0-1.ef",
			"v1-test.1-2.ef",
			"v1-test.0-2.ef",
		})
		ii.dirtyFiles.Scan(func(item *filesItem) bool {
			fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
			item.decompressor = &seg.Decompressor{FileName1: fName}
			return true
		})
		ii.reCalcVisibleFiles()

		h := &History{InvertedIndex: ii, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
		h.scanStateFiles([]string{
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
		h.reCalcVisibleFiles()

		hc := h.BeginFilesRo()
		defer hc.Close()
		r := hc.findMergeRange(4, 32)
		assert.False(t, r.index)
		assert.False(t, r.history)
	})
	t.Run("idx merged, but garbage left2", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.scanStateFiles([]string{
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
		ii.reCalcVisibleFiles()
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
