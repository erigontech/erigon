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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

func TestGCReadAfterRemoveFile(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := t.Context()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)
		collateAndMergeHistory(t, db, h, txs, true)

		t.Run("read after: remove when have reader", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - create immutable view
			// - del cold file
			// - read from canDelete file
			// - close view
			// - open new view
			// - make sure there is no canDelete file
			hc := h.beginForTests()

			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			deleteMergeFile(h.dirtyFiles, []*FilesItem{lastOnFs}, "", h.logger)
			require.NotNil(lastOnFs.decompressor)

			lastInView := hc.files[len(hc.files)-1]

			g := seg.NewPagedReader(hc.statelessGetter(len(hc.files)-1), true)
			require.Equal(lastInView.startTxNum, lastOnFs.startTxNum)
			require.Equal(lastInView.endTxNum, lastOnFs.endTxNum)
			if g.HasNext() {
				k, _ := g.Next(nil)
				require.Len(k, 8)
				v, _ := g.Next(nil)
				require.Len(v, 8)
			}

			require.NotNil(lastOnFs.decompressor)
			//replace of locality index must not affect current HistoryRoTx, but expect to be closed after last reader
			hc.Close()
			require.Nil(lastOnFs.decompressor)

			nonDeletedOnFs, _ := h.dirtyFiles.Max()
			require.False(nonDeletedOnFs.frozen)
			require.NotNil(nonDeletedOnFs.decompressor) // non-canDelete files are not closed

			hc = h.beginForTests()
			newLastInView := hc.files[len(hc.files)-1]
			require.False(lastOnFs.frozen)
			require.False(lastInView.startTxNum == newLastInView.startTxNum && lastInView.endTxNum == newLastInView.endTxNum)

			hc.Close()
		})

		t.Run("read after: remove when no btReaders", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - del cold file
			// - new reader must not see canDelete file
			hc := h.beginForTests()
			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			deleteMergeFile(h.dirtyFiles, []*FilesItem{lastOnFs}, "", h.logger)

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)
		})
	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestDomainGCReadAfterRemoveFile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := t.Context()

	test := func(t *testing.T, h *Domain, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)
		err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
			collateAndMerge(t, tx, h, txs)
			return nil
		})
		require.NoError(err)

		t.Run("read after: remove when have reader", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - create immutable view
			// - del cold file
			// - read from canDelete file
			// - close view
			// - open new view
			// - make sure there is no canDelete file
			hc := h.beginForTests()
			_ = hc
			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.

			deleteMergeFile(h.dirtyFiles, []*FilesItem{lastOnFs}, "", h.logger)

			require.NotNil(lastOnFs.decompressor)

			lastInView := hc.files[len(hc.files)-1]
			g := lastInView.src.decompressor.MakeGetter()
			require.Equal(lastInView.startTxNum, lastOnFs.startTxNum)
			require.Equal(lastInView.endTxNum, lastOnFs.endTxNum)
			if g.HasNext() {
				k, _ := g.Next(nil)
				require.Len(k, 8)
				v, _ := g.Next(nil)
				require.Len(v, 8)
			}

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)

			nonDeletedOnFs, _ := h.dirtyFiles.Max()
			require.False(nonDeletedOnFs.frozen)
			require.NotNil(nonDeletedOnFs.decompressor) // non-canDelete files are not closed

			hc = h.beginForTests()
			newLastInView := hc.files[len(hc.files)-1]
			require.False(lastOnFs.frozen)
			require.False(lastInView.startTxNum == newLastInView.startTxNum && lastInView.endTxNum == newLastInView.endTxNum)

			hc.Close()
		})

		t.Run("read after: remove when no btReaders", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - del cold file
			// - new reader must not see canDelete file
			hc := h.beginForTests()
			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			deleteMergeFile(h.dirtyFiles, []*FilesItem{lastOnFs}, "", h.logger)

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)
		})
	}
	logger := log.New()
	db, d, txs := filledDomain(t, logger)
	test(t, d, db, txs)
}

// generateAllDomainsOverlap writes {0,1},{1,2} subset files shadowed by a {0,2}
// covering file for every state domain, so that overlap-cleanup has garbage to
// retire after OpenFolder.
func generateAllDomainsOverlap(t *testing.T, agg *Aggregator) {
	t.Helper()
	ranges := []testFileRange{{0, 1}, {1, 2}, {0, 2}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder())
}

func mustExist(t *testing.T, path string, want bool) {
	t.Helper()
	got, err := dir.FileExist(path)
	require.NoError(t, err)
	require.Equalf(t, want, got, "file %s exist=%v, want %v", path, got, want)
}

// TestAggregatorReadAfterRetire verifies the bundle-refcount reclamation: a file
// retired (overlap-cleaned) while a reader still pins its generation is NOT
// removed from disk; it is physically deleted only once the last pinning reader
// closes. A reader opened after retirement no longer sees it.
func TestAggregatorReadAfterRetire(t *testing.T) {
	stepSize := uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	generateAllDomainsOverlap(t, agg)

	// subset (overlap) files for the accounts history — shadowed by 0-2, non-frozen
	subset01 := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-1.v")
	subset12 := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.1-2.v")
	covering := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-2.v")
	mustExist(t, subset01, true)
	mustExist(t, subset12, true)
	mustExist(t, covering, true)

	// pin the current generation (overlaps are present in dirtyFiles)
	at := agg.BeginFilesRo()

	// retire the overlap files; they get attached to the generation `at` pins
	require.NoError(t, agg.RemoveOverlapsAfterMerge(t.Context()))

	// still pinned by `at` → must NOT be deleted yet
	mustExist(t, subset01, true)
	mustExist(t, subset12, true)

	// last reader closes → reclaimer deletes the retired files
	at.Close()
	mustExist(t, subset01, false)
	mustExist(t, subset12, false)
	mustExist(t, covering, true) // covering file stays visible

	// a freshly opened reader sees only the covering file
	at2 := agg.BeginFilesRo()
	defer at2.Close()
	hf := at2.d[kv.AccountsDomain].ht.files
	require.Len(t, hf, 1)
	require.Equal(t, uint64(0), hf[0].startTxNum/stepSize)
	require.Equal(t, uint64(2), hf[0].endTxNum/stepSize)
}

// TestAggregatorRetireDeferredWhileDebugPins verifies that a file retired by an
// overlap-cleanup is NOT physically deleted while a DebugBeginDirtyFilesRo
// (BuildMissedAccessors) still pins the generation — deletion is deferred until
// that debug tx closes. Guards against the reclaimer deleting a file out from
// under a concurrent accessor build.
func TestAggregatorRetireDeferredWhileDebugPins(t *testing.T) {
	stepSize := uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	generateAllDomainsOverlap(t, agg)

	subset01 := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-1.v")
	mustExist(t, subset01, true)

	// pin the current generation (incl. the soon-to-be-retired overlaps in its dirtyFiles)
	rotx := agg.DebugBeginDirtyFilesRo()

	// retire the overlap files; they are pinned by rotx so must be parked, not deleted
	require.NoError(t, agg.RemoveOverlapsAfterMerge(t.Context()))
	mustExist(t, subset01, true) // deferred — still on disk

	// releasing the debug pin triggers the deferred deletion
	rotx.Close()
	mustExist(t, subset01, false)
}

// TestAggregatorReclaimConcurrent stresses BeginFilesRo/Close against a
// concurrent retirement under the race detector: no double-free / use-after-free
// of FilesItem regardless of how Close and reclamation interleave.
func TestAggregatorReclaimConcurrent(t *testing.T) {
	stepSize := uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	generateAllDomainsOverlap(t, agg)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tx := agg.BeginFilesRo()
				// touch visible files so a use-after-free would be observed
				_ = tx.d[kv.AccountsDomain].ht.files.EndTxNum()
				tx.Close()
			}
		}()
	}

	require.NoError(t, agg.RemoveOverlapsAfterMerge(t.Context()))
	close(stop)
	wg.Wait()

	mustExist(t, filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-1.v"), false)
}
