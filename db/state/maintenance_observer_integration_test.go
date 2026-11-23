// Copyright 2026 The Erigon Authors
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

package state_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
)

func TestMaintenanceObserverFilePublication(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	writeStepsKeys(t, db, agg, mkAddrs(0x10, 2), 0, 3)
	var published uint64
	var points []state.MaintenancePoint
	agg.SetMaintenanceObserver(func(ctx context.Context, event state.MaintenanceEvent) {
		assert.NoError(t, ctx.Err(), "maintenance callbacks must receive a live operation context")
		points = append(points, event.Point)
		if event.Point == state.MaintenanceFilesPublished {
			published += stepSize
		}
		assert.NoError(t, db.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
			for _, domain := range kv.StateDomains {
				assert.Equal(t, published, tx.Debug().DomainFiles(domain).EndRootNum(),
					"%s visibility at maintenance point %d", domain, event.Point)
			}
			return nil
		}))
	})
	<-agg.BuildFilesInBackground(db, 3*stepSize, unboundedFinalityCtx)
	require.Positive(t, published)
	var want []state.MaintenancePoint
	for range published / stepSize {
		want = append(want, state.MaintenanceBuildStarted, state.MaintenanceFilesBuilt, state.MaintenanceFilesPublished)
	}
	require.Equal(t, want, points)
}

func TestMaintenanceObserverBuildCompletion(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	writeStepsKeys(t, db, agg, mkAddrs(0x10, 2), 0, 3)
	var observed []<-chan struct{}
	agg.SetMaintenanceObserver(func(_ context.Context, event state.MaintenanceEvent) {
		if event.Point == state.MaintenanceBuildStarted {
			observed = append(observed, event.FilesDone)
		}
	})
	mergeReached, mergeAllowed := make(chan struct{}), make(chan struct{})
	releaseMerge := sync.OnceFunc(func() { close(mergeAllowed) })
	defer releaseMerge()
	var firstMerge sync.Once
	agg.OnFilesChange(func([]string) {
		files := agg.BeginFilesRo()
		defer files.Close()
		for _, file := range files.Files(kv.AccountsDomain) {
			if file.EndRootNum()-file.StartRootNum() > stepSize {
				firstMerge.Do(func() {
					close(mergeReached)
					<-mergeAllowed
				})
			}
		}
	}, func([]string) {})
	finished := agg.BuildFilesInBackground(db, 3*stepSize, unboundedFinalityCtx)
	select {
	case <-mergeReached:
	case <-time.After(time.Minute):
		t.Fatal("the build job did not reach a merge")
	}
	select {
	case <-finished:
		t.Error("FilesDone closed before the job's merge returned")
	default:
	}
	releaseMerge()
	<-finished
	require.NotEmpty(t, observed)
	for _, filesDone := range observed {
		require.Equal(t, (<-chan struct{})(finished), filesDone, "the observer must receive the exact build/merge job's completion signal")
	}
}

func TestMaintenanceObserverPrunePrecedesBuild(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 10)
	writeStepsKeys(t, db, agg, mkAddrs(0x10, 2), 0, 3)
	committed := make(chan struct{})
	var built atomic.Bool
	agg.SetMaintenanceObserver(func(_ context.Context, event state.MaintenanceEvent) {
		switch event.Point {
		case state.MaintenancePruneCommitted:
			assert.False(t, agg.HasBackgroundFilesBuild2(), "PruneCommitted must precede starting the next build job")
			close(committed)
		case state.MaintenanceBuildStarted:
			built.Store(true)
			select {
			case <-committed:
			default:
				t.Error("file building began before PruneCommitted")
			}
		}
	})
	started, finished, err := agg.CollateAndPrune(t.Context(), db, func(kv.TemporalRwTx) (kv.FinalityContext, error) {
		return unboundedFinalityCtx, nil
	})
	require.NoError(t, err)
	<-finished
	require.True(t, started)
	require.True(t, built.Load(), "the prune pass must exercise an eligible build")
}

func TestMaintenanceObserverPruneTransaction(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 10)
	key, value := []byte("maintenance-observer"), []byte("pending")
	var points []state.MaintenancePoint
	agg.SetMaintenanceObserver(func(ctx context.Context, event state.MaintenanceEvent) {
		points = append(points, event.Point)
		switch event.Point {
		case state.MaintenancePruneReady:
			require.NotNil(t, event.PruneTx)
			got, err := event.PruneTx.GetOne(kv.Sequence, key)
			require.NoError(t, err)
			require.Equal(t, value, got)
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				got, err := tx.GetOne(kv.Sequence, key)
				require.Empty(t, got, "PruneReady must precede the commit")
				return err
			}))
		case state.MaintenancePruneCommitted:
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				got, err := tx.GetOne(kv.Sequence, key)
				require.Equal(t, value, got, "PruneCommitted must follow the commit")
				return err
			}))
		}
	})
	_, finished, err := agg.CollateAndPrune(t.Context(), db, func(tx kv.TemporalRwTx) (kv.FinalityContext, error) {
		return unboundedFinalityCtx, tx.Put(kv.Sequence, key, value)
	})
	require.NoError(t, err)
	<-finished
	require.Equal(t, []state.MaintenancePoint{state.MaintenancePruneReady, state.MaintenancePruneCommitted}, points)
}
