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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
)

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
	finished := agg.BuildFilesInBackground(db, 3*stepSize, unboundedFinalityCtx)
	<-finished
	require.NotEmpty(t, observed)
	for _, filesDone := range observed {
		require.Equal(t, (<-chan struct{})(finished), filesDone, "the observer must receive the exact build/merge job's completion signal")
	}
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
