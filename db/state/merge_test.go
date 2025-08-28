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
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

func TestDomainRoTx_findMergeRange(t *testing.T) {
	t.Parallel()

	newDomainRoTx := func(aggStep uint64, files []visibleFile) *DomainRoTx {
		return &DomainRoTx{
			name:     kv.AccountsDomain,
			files:    files,
			stepSize: aggStep,
			ht:       &HistoryRoTx{iit: &InvertedIndexRoTx{}},
		}
	}

	createFile := func(startTxNum, endTxNum uint64) visibleFile {
		return visibleFile{
			startTxNum: startTxNum,
			endTxNum:   endTxNum,
			src:        &FilesItem{startTxNum: startTxNum, endTxNum: endTxNum},
		}
	}

	t.Run("empty_and_single_file", func(t *testing.T) {
		dt := newDomainRoTx(1, []visibleFile{})
		result := dt.findMergeRange(100, 32)
		assert.False(t, result.values.needMerge)
		assert.Equal(t, uint64(1), result.aggStep)

		dt = newDomainRoTx(1, []visibleFile{createFile(0, 2)})
		result = dt.findMergeRange(4, 32)
		assert.False(t, result.values.needMerge)
	})

	t.Run("earlier_first", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 2),
			createFile(2, 4),
			createFile(4, 5),
			createFile(5, 6),
		}
		dt := newDomainRoTx(1, files)
		result := dt.findMergeRange(16, 32)
		assert.True(t, result.values.needMerge)
		assert.Equal(t, uint64(0), result.values.from)
		assert.Equal(t, uint64(4), result.values.to)
	})

	t.Run("small_first", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 1),
			createFile(1, 2),
			createFile(2, 3),
			createFile(3, 4),
		}
		dt := newDomainRoTx(1, files)
		result := dt.findMergeRange(4, 32)
		assert.True(t, result.values.needMerge)
		assert.Equal(t, uint64(0), result.values.from)
		assert.Equal(t, uint64(2), result.values.to)
	})

	t.Run("aggregation_steps", func(t *testing.T) {
		for _, aggStep := range []uint64{1, 2, 4, 8} {
			endTx := aggStep * 4
			files := []visibleFile{
				createFile(0, aggStep),
				createFile(aggStep, aggStep*2),
			}
			dt := newDomainRoTx(aggStep, files)
			result := dt.findMergeRange(endTx, 32)
			assert.Equal(t, aggStep, result.aggStep)
			assert.True(t, result.values.needMerge)
		}
	})

}

func emptyTestInvertedIndex(aggStep uint64) *InvertedIndex {
	salt := uint32(1)
	cfg := statecfg.Schema.AccountsDomain.Hist.IiCfg

	dirs := datadir.New(os.TempDir())
	ii, err := NewInvertedIndex(cfg, aggStep, dirs, log.New())
	ii.Accessors = 0
	ii.salt.Store(&salt)
	if err != nil {
		panic(err)
	}
	return ii
}

func TestFindMergeRangeCornerCases(t *testing.T) {
	t.Parallel()

	newTestDomain := func() (*InvertedIndex, *History) {
		d := emptyTestDomain(1)
		d.History.InvertedIndex.Accessors = 0
		d.History.Accessors = 0
		return d.History.InvertedIndex, d.History
	}
	t.Run("ii: > 2 unmerged files", func(t *testing.T) {
		ii, _ := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-2.ef",
			"v1.0-accounts.2-3.ef",
			"v1.0-accounts.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		ic := ii.BeginFilesRo()
		defer ic.Close()

		mr := ic.findMergeRange(4, 32)
		assert.True(t, mr.needMerge)
		assert.Equal(t, 0, int(mr.from))
		assert.Equal(t, 4, int(mr.to))
		assert.Equal(t, ii.Name.String(), mr.name)

		idxF := ic.staticFilesInRange(mr.from, mr.to)
		assert.Len(t, idxF, 3)
	})
	t.Run("hist: > 2 unmerged files", func(t *testing.T) {
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
			"v1.0-accounts.2-3.ef",
			"v1.0-accounts.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
			"v1.0-accounts.2-3.v",
			"v1.0-accounts.3-4.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
			"v1.0-accounts.2-3.ef",
			"v1.0-accounts.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-2.ef",
			"v1.0-accounts.2-3.ef",
			"v1.0-accounts.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
			"v1.0-accounts.2-3.ef",
			"v1.0-accounts.3-4.ef",
			"v1.0-accounts.0-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
			"v1.0-accounts.2-3.v",
			"v1.0-accounts.3-4.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		require.Len(t, idxFiles, 2)
		require.Len(t, histFiles, 2)
	})
	t.Run("idx merged and small files lost", func(t *testing.T) {
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
			"v1.0-accounts.2-3.v",
			"v1.0-accounts.3-4.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		// `kill -9` may leave small garbage files, but if big one already exists we assume it's good(fsynced) and no reason to merge again
		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
			"v1.0-accounts.0-2.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		require.Len(t, idxFiles, 2)
		require.Empty(t, histFiles)
	})
	t.Run("history merge progress ahead of idx", func(t *testing.T) {
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
			"v1.0-accounts.0-2.ef",
			"v1.0-accounts.2-3.ef",
			"v1.0-accounts.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
			"v1.0-accounts.0-2.v",
			"v1.0-accounts.2-3.v",
			"v1.0-accounts.3-4.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		require.Len(t, idxFiles, 3)
		require.Len(t, histFiles, 3)
	})
	t.Run("idx merge progress ahead of history", func(t *testing.T) {
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
			"v1.0-accounts.0-2.ef",
			"v1.0-accounts.2-3.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
			"v1.0-accounts.2-3.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		require.Len(t, idxFiles, 2)
		require.Len(t, histFiles, 2)
	})
	t.Run("idx merged, but garbage left", func(t *testing.T) {
		ii, h := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
			"v1.0-accounts.0-2.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

		h.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
			"v1.0-accounts.0-2.v",
			"v1.0-accounts.2-3.v",
		})
		h.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
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
		ii, _ := newTestDomain()
		ii.scanDirtyFiles([]string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
			"v1.0-accounts.0-2.ef",
			"v1.0-accounts.2-3.ef",
			"v1.0-accounts.3-4.ef",
		})
		ii.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())
		ic := ii.BeginFilesRo()
		defer ic.Close()
		mr := ic.findMergeRange(4, 32)
		assert.True(t, mr.needMerge)
		require.Zero(t, int(mr.from))
		require.Equal(t, 4, int(mr.to))
		require.Equal(t, ii.Name.String(), mr.name)
		idxFiles := ic.staticFilesInRange(mr.from, mr.to)
		require.Len(t, idxFiles, 3)
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

	menc, err := mergeNumSeqs(firstBytes, secondBytes, 0, 0, nil, 0)
	require.NoError(t, err)

	merged, _ := eliasfano32.ReadEliasFano(menc)
	require.NoError(t, err)
	require.EqualValues(t, len(uniq), merged.Count())
	require.Equal(t, merged.Count(), eliasfano32.Count(menc))
	mergedLists := append(firstList, secondList...)
	sort.Ints(mergedLists)
	require.EqualValues(t, mergedLists[len(mergedLists)-1], merged.Max())
	require.Equal(t, merged.Max(), eliasfano32.Max(menc))

	mit := merged.Iterator()
	for mit.HasNext() {
		v, _ := mit.Next()
		require.Contains(t, mergedLists, int(v))
	}
}

func TestMergeFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	db, d := testDbAndDomain(t, log.New())
	defer db.Close()
	defer d.Close()

	dc := d.BeginFilesRo()
	defer dc.Close()

	txs := d.stepSize * 8
	data := generateTestData(t, 20, 52, txs, txs, 100)

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	w := dc.NewWriter()

	prev := []byte{}
	prevStep := kv.Step(0)
	for key, upd := range data {
		for _, v := range upd {
			err := w.PutWithPrev([]byte(key), v.value, v.txNum, prev, prevStep)

			prev, prevStep = v.value, kv.Step(v.txNum/d.stepSize)
			require.NoError(t, err)
		}
	}

	require.NoError(t, w.Flush(context.Background(), rwTx))
	w.Close()
	err = rwTx.Commit()
	require.NoError(t, err)

	collateAndMerge(t, db, nil, d, txs)

	rwTx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	dc = d.BeginFilesRo()
	defer dc.Close()
}

func TestMergeFilesWithDependency(t *testing.T) {
	t.Parallel()

	newTestDomain := func(dom kv.Domain) *Domain {
		cfg := statecfg.Schema.GetDomainCfg(dom)

		salt := uint32(1)
		dirs := datadir.New(os.TempDir())
		cfg.Hist.IiCfg.Name = kv.InvertedIdx(0)
		cfg.Hist.IiCfg.Version = statecfg.IIVersionTypes{DataEF: version.V1_0_standart, AccessorEFI: version.V1_0_standart}

		d, err := NewDomain(cfg, 1, dirs, log.New())
		if err != nil {
			panic(err)
		}
		d.salt.Store(&salt)

		d.History.InvertedIndex.Accessors = 0
		d.History.Accessors = 0
		d.Accessors = 0
		return d
	}

	setup := func() (account, storage, commitment *Domain) {
		account, storage, commitment = newTestDomain(0), newTestDomain(1), newTestDomain(3)
		checker := NewDependencyIntegrityChecker(account.dirs, log.New())
		info := &DependentInfo{
			entity: FromDomain(commitment.Name),
			filesGetter: func() *btree2.BTreeG[*FilesItem] {
				return commitment.dirtyFiles
			},
			accessors: commitment.Accessors,
		}
		checker.AddDependency(FromDomain(account.Name), info)
		checker.AddDependency(FromDomain(storage.Name), info)
		account.SetChecker(checker)
		storage.SetChecker(checker)
		return
	}

	setupFiles := func(d *Domain, mergedMissing bool) {
		kvf := fmt.Sprintf("v1.0-%s", d.Name.String()) + ".%d-%d.kv"
		files := []string{fmt.Sprintf(kvf, 0, 1), fmt.Sprintf(kvf, 1, 2)}
		if !mergedMissing {
			files = append(files, fmt.Sprintf(kvf, 0, 2))
		}
		d.scanDirtyFiles(files)
		d.dirtyFiles.Scan(func(item *FilesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
	}

	t.Run("all merged files present", func(t *testing.T) {
		account, storage, commitment := setup()
		setupFiles(account, false)
		setupFiles(storage, false)
		setupFiles(commitment, false)

		account.reCalcVisibleFiles(account.dirtyFilesEndTxNumMinimax())
		storage.reCalcVisibleFiles(storage.dirtyFilesEndTxNumMinimax())
		commitment.reCalcVisibleFiles(commitment.dirtyFilesEndTxNumMinimax())

		checkFn := func(files visibleFiles) {
			assert.Equal(t, 1, len(files))
			assert.Equal(t, 0, int(files[0].startTxNum))
			assert.Equal(t, 2, int(files[0].endTxNum))
		}

		ac, sc, cc := account.BeginFilesRo(), storage.BeginFilesRo(), commitment.BeginFilesRo()
		defer ac.Close()
		defer sc.Close()
		defer cc.Close()

		checkFn(ac.files)
		checkFn(sc.files)
		checkFn(cc.files)
	})

	t.Run("commitment merged missing", func(t *testing.T) {
		account, storage, commitment := setup()
		setupFiles(account, false)
		setupFiles(storage, false)
		setupFiles(commitment, true)

		account.reCalcVisibleFiles(account.dirtyFilesEndTxNumMinimax())
		storage.reCalcVisibleFiles(storage.dirtyFilesEndTxNumMinimax())
		commitment.reCalcVisibleFiles(commitment.dirtyFilesEndTxNumMinimax())

		checkFn := func(files visibleFiles) {
			assert.Equal(t, 2, len(files))
			assert.Equal(t, 0, int(files[0].startTxNum))
			assert.Equal(t, 1, int(files[0].endTxNum))
			assert.Equal(t, 1, int(files[1].startTxNum))
			assert.Equal(t, 2, int(files[1].endTxNum))
		}

		ac, sc, cc := account.BeginFilesRo(), storage.BeginFilesRo(), commitment.BeginFilesRo()
		defer ac.Close()
		defer sc.Close()
		defer cc.Close()

		checkFn(ac.files)
		checkFn(sc.files)
		checkFn(cc.files)
	})

	t.Run("check garbage in all merged", func(t *testing.T) {
		account, storage, commitment := setup()
		setupFiles(account, false)
		setupFiles(storage, false)
		setupFiles(commitment, false)

		account.reCalcVisibleFiles(account.dirtyFilesEndTxNumMinimax())
		storage.reCalcVisibleFiles(storage.dirtyFilesEndTxNumMinimax())
		commitment.reCalcVisibleFiles(commitment.dirtyFilesEndTxNumMinimax())

		checkFn := func(dtx *DomainRoTx, garbageCount int) {
			var mergedF *FilesItem
			items := dtx.d.dirtyFiles.Items()

			if len(items) == 3 {
				mergedF = items[2]
			}
			assert.Len(t, dtx.garbage(mergedF), garbageCount)
		}

		ac, sc, cc := account.BeginFilesRo(), storage.BeginFilesRo(), commitment.BeginFilesRo()
		defer ac.Close()
		defer sc.Close()
		defer cc.Close()

		checkFn(ac, 0) // should give 0 because corresponding commitment garbage is not deleted
		checkFn(sc, 0)
		checkFn(cc, 2)

		// delete the smaller files
		commitment.dirtyFiles.Delete(&FilesItem{startTxNum: 0, endTxNum: 1})
		commitment.dirtyFiles.Delete(&FilesItem{startTxNum: 1, endTxNum: 2})

		// refresh visible files
		ac.Close()
		sc.Close()
		cc.Close()

		ac, sc, cc = account.BeginFilesRo(), storage.BeginFilesRo(), commitment.BeginFilesRo()
		defer ac.Close()
		defer sc.Close()
		defer cc.Close()

		checkFn(ac, 2)
		checkFn(sc, 2)
		checkFn(cc, 0)
	})

	t.Run("check garbage in all merged (external gc)", func(t *testing.T) {
		account, storage, commitment := setup()
		setupFiles(account, false)
		setupFiles(storage, false)
		setupFiles(commitment, false)

		account.reCalcVisibleFiles(account.dirtyFilesEndTxNumMinimax())
		storage.reCalcVisibleFiles(storage.dirtyFilesEndTxNumMinimax())
		commitment.reCalcVisibleFiles(commitment.dirtyFilesEndTxNumMinimax())

		checkFn := func(dtx *DomainRoTx, garbageCount int) {
			assert.Len(t, dtx.garbage(nil), garbageCount)
		}

		ac, sc, cc := account.BeginFilesRo(), storage.BeginFilesRo(), commitment.BeginFilesRo()
		defer ac.Close()
		defer sc.Close()
		defer cc.Close()

		checkFn(ac, 2)
		checkFn(sc, 2)
		checkFn(cc, 2)

		// delete the smaller files
		commitment.dirtyFiles.Delete(&FilesItem{startTxNum: 0, endTxNum: 1})
		commitment.dirtyFiles.Delete(&FilesItem{startTxNum: 1, endTxNum: 2})

		// refresh visible files
		ac.Close()
		sc.Close()
		cc.Close()

		ac, sc, cc = account.BeginFilesRo(), storage.BeginFilesRo(), commitment.BeginFilesRo()
		defer ac.Close()
		defer sc.Close()
		defer cc.Close()

		checkFn(ac, 2)
		checkFn(sc, 2)
		checkFn(cc, 0)
	})

	t.Run("check garbage commitment not merged", func(t *testing.T) {
		account, storage, commitment := setup()
		setupFiles(account, false)
		setupFiles(storage, false)
		setupFiles(commitment, true)

		account.reCalcVisibleFiles(account.dirtyFilesEndTxNumMinimax())
		storage.reCalcVisibleFiles(storage.dirtyFilesEndTxNumMinimax())
		commitment.reCalcVisibleFiles(commitment.dirtyFilesEndTxNumMinimax())

		checkFn := func(dtx *DomainRoTx) {
			var mergedF *FilesItem
			items := dtx.d.dirtyFiles.Items()

			if len(items) == 3 {
				mergedF = items[2]
			}
			assert.Len(t, dtx.garbage(mergedF), 0)
		}

		ac, sc, cc := account.BeginFilesRo(), storage.BeginFilesRo(), commitment.BeginFilesRo()
		defer ac.Close()
		defer sc.Close()
		defer cc.Close()

		checkFn(ac)
		checkFn(sc)
		checkFn(cc)
	})

	t.Run("check garbage commitment not merged (external gc i.e. mergedFile=nil)", func(t *testing.T) {
		account, storage, commitment := setup()
		setupFiles(account, false)
		setupFiles(storage, false)
		setupFiles(commitment, true)

		account.reCalcVisibleFiles(account.dirtyFilesEndTxNumMinimax())
		storage.reCalcVisibleFiles(storage.dirtyFilesEndTxNumMinimax())
		commitment.reCalcVisibleFiles(commitment.dirtyFilesEndTxNumMinimax())

		checkFn := func(dtx *DomainRoTx) {
			assert.Len(t, dtx.garbage(nil), 0)
		}

		ac, sc, cc := account.BeginFilesRo(), storage.BeginFilesRo(), commitment.BeginFilesRo()
		defer ac.Close()
		defer sc.Close()
		defer cc.Close()

		checkFn(ac)
		checkFn(sc)
		checkFn(cc)
	})
}

func TestHistoryAndIIAlignment(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)

	agg, _ := newAggregatorOld(context.Background(), dirs, 1, db, logger)
	setup := func() (account *Domain) {
		agg.RegisterDomain(statecfg.Schema.GetDomainCfg(kv.AccountsDomain), nil, dirs, logger)
		domain := agg.d[kv.AccountsDomain]
		domain.History.InvertedIndex.Accessors = 0
		domain.History.Accessors = 0
		domain.Accessors = 0
		return domain
	}

	account := setup()
	require.NotNil(t, account.InvertedIndex.checker)

	h := account.History
	ii := h.InvertedIndex

	h.scanDirtyFiles([]string{
		"v1.0-accounts.0-1.v",
		"v1.0-accounts.1-2.v",
		"v1.0-accounts.2-3.v",
		"v1.0-accounts.3-4.v",
	})

	h.dirtyFiles.Scan(func(item *FilesItem) bool {
		item.decompressor = &seg.Decompressor{}
		return true
	})

	ii.scanDirtyFiles([]string{
		"v1.0-accounts.0-1.ef",
		"v1.0-accounts.1-2.ef",
		"v1.0-accounts.2-3.ef",
		"v1.0-accounts.3-4.ef",
		"v1.0-accounts.0-4.ef",
	})
	ii.dirtyFiles.Scan(func(item *FilesItem) bool {
		item.decompressor = &seg.Decompressor{}
		return true
	})
	h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

	roTx := h.BeginFilesRo()
	defer roTx.Close()

	for i, f := range roTx.files {
		require.Equal(t, uint64(i), f.startTxNum)
		require.Equal(t, uint64(i+1), f.endTxNum)
	}

	for i, f := range roTx.iit.files {
		require.Equal(t, uint64(i), f.startTxNum)
		require.Equal(t, uint64(i+1), f.endTxNum)
	}

	require.Len(t, roTx.garbage(&FilesItem{startTxNum: 0, endTxNum: 4}), 4)

	// no garbage with iit, since history is not merged
	require.Len(t, roTx.iit.garbage(&FilesItem{startTxNum: 0, endTxNum: 4}), 0)
}
