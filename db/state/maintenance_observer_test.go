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

package state

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
)

func TestMaintenanceObserverClose(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build bool
	}{
		{"aggregator_context", true},
		{"caller_context", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testMaintenanceObserver(t, func(t *testing.T, agg *Aggregator) {
				defer agg.ctxCancel()
				callerCtx, cancel := context.WithCancel(t.Context())
				defer cancel()
				operationCtx := callerCtx
				if tc.build {
					operationCtx = agg.ctx
				}
				observed := make(chan error, 1)
				agg.SetMaintenanceObserver(func(ctx context.Context, _ MaintenanceEvent) {
					<-ctx.Done()
					observed <- ctx.Err()
				})
				finishWork := make(chan struct{})
				defer close(finishWork)
				agg.buildingFiles.Store(tc.build)
				require.True(t, agg.background.TryGo(func() {
					defer agg.buildingFiles.Store(false)
					agg.observeMaintenance(operationCtx, MaintenanceEvent{})
					<-finishWork
				}))
				synctest.Wait()
				closed := make(chan struct{})
				go func() {
					agg.Close()
					close(closed)
				}()
				synctest.Wait()
				require.Len(t, observed, 1, "Close must cancel a blocked observer before waiting for maintenance")
				require.ErrorIs(t, <-observed, context.Canceled)
				require.NoError(t, operationCtx.Err(), "releasing an observer must not cancel maintenance")
				select {
				case <-closed:
					t.Fatal("Close returned before maintenance finished")
				default:
				}
			})
		})
	}
}

func TestMaintenanceObserverOperationCancellation(t *testing.T) {
	testMaintenanceObserver(t, func(t *testing.T, agg *Aggregator) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		observed := make(chan error, 1)
		agg.SetMaintenanceObserver(func(ctx context.Context, _ MaintenanceEvent) {
			<-ctx.Done()
			observed <- ctx.Err()
		})
		go agg.observeMaintenance(ctx, MaintenanceEvent{Point: MaintenancePruneReady})
		synctest.Wait()
		cancel()
		synctest.Wait()
		require.Len(t, observed, 1)
		require.ErrorIs(t, <-observed, context.Canceled)
		require.NoError(t, agg.ctx.Err(), "operation cancellation must not stop the aggregator")
	})
}

func testMaintenanceObserver(t *testing.T, run func(*testing.T, *Aggregator)) {
	t.Helper()
	// Diagnostic goroutines may live for the process lifetime, so start them
	// outside the bubble. Contexts used by fake-time waits must live inside it.
	agg, err := newAggregator(t.Context(), datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	agg.ctxCancel()
	synctest.Test(t, func(t *testing.T) {
		agg.ctx, agg.ctxCancel = context.WithCancel(t.Context())
		agg.observerCtx, agg.observerCancel = context.WithCancel(agg.ctx)
		defer agg.Close()
		run(t, agg)
	})
}
