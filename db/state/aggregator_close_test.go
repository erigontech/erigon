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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

// The merge goroutine BuildFiles2 spawns must register on wg before the build
// goroutine's wg.Done, so wg.Wait (what Close does) never races that Add from zero.
func TestAggregatorCloseWaitsForBackgroundMerge(t *testing.T) {
	logger := log.New()
	for range 64 {
		dirs := datadir.New(t.TempDir())
		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		agg := NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), db)
		require.NoError(t, agg.OpenFolder())

		require.NoError(t, agg.BuildFiles2(t.Context(), 0, 0, true))
		agg.background.Wait()
		agg.Close()
		db.Close()
	}
}

// MergeLoop is also called from goroutines the aggregator did not spawn (e.g. the
// node's background-maintenance goroutine), so its wg registration must be ordered
// against Close's Wait — an unordered Add from zero is WaitGroup reuse, flagged by -race.
func TestAggregatorCloseVsConcurrentMergeLoop(t *testing.T) {
	t.Parallel()
	logger := log.New()
	for range 4 {
		dirs := datadir.New(t.TempDir())
		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		agg := NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), db)
		require.NoError(t, agg.OpenFolder())

		start := make(chan struct{})
		var loops sync.WaitGroup
		for i := range 8 {
			loops.Go(func() {
				<-start
				time.Sleep(time.Duration(i) * 250 * time.Microsecond)
				_ = agg.MergeLoop(context.Background())
			})
		}
		close(start)
		agg.Close()
		loops.Wait()
		db.Close()
	}
}

// BuildFilesInBackground twin of TestAggregatorCloseVsConcurrentMergeLoop.
func TestAggregatorCloseVsConcurrentBuildFilesInBackground(t *testing.T) {
	t.Parallel()
	logger := log.New()
	for range 4 {
		dirs := datadir.New(t.TempDir())
		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		agg := NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), db)
		require.NoError(t, agg.OpenFolder())

		start := make(chan struct{})
		fins := make(chan chan struct{}, 8)
		var loops sync.WaitGroup
		for i := range 8 {
			loops.Go(func() {
				<-start
				time.Sleep(time.Duration(i) * 250 * time.Microsecond)
				fins <- agg.BuildFilesInBackground(1_000_000)
			})
		}
		close(start)
		agg.Close()
		loops.Wait()
		close(fins)
		for fin := range fins {
			<-fin
		}
		db.Close()
	}
}

// BuildFiles2 twin of TestAggregatorCloseVsConcurrentMergeLoop.
func TestAggregatorCloseVsConcurrentBuildFiles2(t *testing.T) {
	t.Parallel()
	logger := log.New()
	for range 4 {
		dirs := datadir.New(t.TempDir())
		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		agg := NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), db)
		require.NoError(t, agg.OpenFolder())

		start := make(chan struct{})
		var loops sync.WaitGroup
		for i := range 8 {
			loops.Go(func() {
				<-start
				time.Sleep(time.Duration(i) * 250 * time.Microsecond)
				_ = agg.BuildFiles2(context.Background(), 0, 0, true)
			})
		}
		close(start)
		agg.Close()
		loops.Wait()
		db.Close()
	}
}

// Close must be safe to call concurrently with itself.
func TestAggregatorConcurrentClose(t *testing.T) {
	t.Parallel()
	logger := log.New()
	for range 4 {
		dirs := datadir.New(t.TempDir())
		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		agg := NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), db)
		require.NoError(t, agg.OpenFolder())

		start := make(chan struct{})
		var closes sync.WaitGroup
		for range 4 {
			closes.Go(func() {
				<-start
				agg.Close()
			})
		}
		close(start)
		closes.Wait()
		db.Close()
	}
}

// Close releases the cached data held by the commitment BranchCache while
// keeping the cache object itself usable for a later reopen.
func TestAggregatorCloseReleasesBranchCache(t *testing.T) {
	prev := dbg.UseStateCache
	dbg.SetUseStateCache(true)
	t.Cleanup(func() { dbg.SetUseStateCache(prev) })

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	defer db.Close()
	agg := NewTest(dirs).StepSize(16).Logger(logger).MustOpen(t.Context(), db)
	require.NoError(t, agg.OpenFolder())

	cd := agg.d[kv.CommitmentDomain]
	require.NotNil(t, cd)
	require.NotNil(t, cd.branchCache, "precondition: BranchCache is set when USE_STATE_CACHE is on")

	prefix := []byte{0x01, 0x02}
	cd.branchCache.Put(prefix, []byte{0xaa, 0xbb}, 1, 1)
	_, _, ok := cd.branchCache.Get(prefix)
	require.True(t, ok, "precondition: entry is cached before Close")

	agg.Close()

	require.NotNil(t, cd.branchCache, "Close keeps the cache object reusable")
	_, _, ok = cd.branchCache.Get(prefix)
	require.False(t, ok, "Close must clear the cached branch data")
}

// A reader that pinned the visible-file generation before Aggregator.OpenFolder
// runs must keep reading the files it still references, even if OpenFolder finds
// some of them removed from disk. The buggy path closed those files in place
// (nil-ing FilesItem.decompressor), crashing the reader that still held them.
func TestAggregatorOpenFolderKeepsReaderFilesAlive(t *testing.T) {
	t.Parallel()
	// Unix-only: the test's whole point is to unlink a .kv file the aggregator still has
	// mmapped (an external actor removing a live snapshot). Windows forbids deleting a mapped
	// file, so neither the setup nor the bug it reproduces can occur there.
	if runtime.GOOS == "windows" {
		t.Skip("deletes a file that is still mmapped; not possible on Windows")
	}
	// generate* helpers hardcode stepSize=10; keep the aggregator consistent.
	const stepSize = uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	dirs := agg.Dirs()

	// All state domains must have matching coverage or the integrity checker hides
	// the accounts files (and the reader would then hold nothing).
	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
	generateAccountsFile(t, dirs, ranges)
	generateStorageFile(t, dirs, ranges)
	generateCodeFile(t, dirs, ranges)
	generateCommitmentFile(t, dirs, ranges)
	require.NoError(t, agg.OpenFolder())

	// Reader pins the current generation, which still references the newest file.
	at := agg.BeginFilesRo()
	defer at.Close()

	// An external actor removes the newest accounts file from disk (e.g. after an
	// unwind or merge cleanup), then the folder is reopened while `at` is still live.
	require.NoError(t, dir.RemoveFile(domainFileBySuffix(t, dirs.SnapDomain, "accounts.2-3.kv")))
	require.NoError(t, agg.OpenFolder())

	var err error
	require.NotPanics(t, func() {
		_, _, _, _, err = at.DebugGetLatestFromFiles(kv.AccountsDomain, make([]byte, 20), 0)
	})
	require.NoError(t, err)
}

// OpenFolder retires vanished files as close-only, never re-deleting them: if the
// same file is recreated (e.g. re-downloaded) before the pinning reader drains,
// reclamation must not clobber the recreation.
func TestAggregatorOpenFolderReclaimKeepsRecreatedFile(t *testing.T) {
	t.Parallel()
	// Unix-only: unlinks a .kv the aggregator still has mmapped (see the sibling test);
	// Windows forbids deleting a mapped file.
	if runtime.GOOS == "windows" {
		t.Skip("deletes a file that is still mmapped; not possible on Windows")
	}
	const stepSize = uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	dirs := agg.Dirs()

	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
	generateAccountsFile(t, dirs, ranges)
	generateStorageFile(t, dirs, ranges)
	generateCodeFile(t, dirs, ranges)
	generateCommitmentFile(t, dirs, ranges)
	require.NoError(t, agg.OpenFolder())

	at := agg.BeginFilesRo() // pins the generation that references accounts.2-3
	defer func() {
		if at != nil {
			at.Close()
		}
	}()

	// External actor removes the newest accounts file; the folder reopens (retiring
	// it), then the same file is recreated on disk before the reader drains.
	require.NoError(t, dir.RemoveFile(domainFileBySuffix(t, dirs.SnapDomain, "accounts.2-3.kv")))
	require.NoError(t, agg.OpenFolder())
	generateAccountsFile(t, dirs, []testFileRange{{2, 3}})
	recreated := domainFileBySuffix(t, dirs.SnapDomain, "accounts.2-3.kv")

	at.Close() // drains the generation -> reclaims the retired file
	at = nil   // prevent double-close in the deferred cleanup

	exists, err := dir.FileExist(recreated)
	require.NoError(t, err)
	require.True(t, exists, "reclaiming a vanished file must not delete a same-name recreation")
}

func domainFileBySuffix(t *testing.T, snapDir, suffix string) string {
	t.Helper()
	files, err := dir.ListFiles(snapDir, ".kv")
	require.NoError(t, err)
	for _, f := range files {
		if strings.HasSuffix(f, suffix) {
			return f
		}
	}
	require.Failf(t, "file not found", "no %q under %s", suffix, snapDir)
	return ""
}
