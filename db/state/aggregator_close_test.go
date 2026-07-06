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
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
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
