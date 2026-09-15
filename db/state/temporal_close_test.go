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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/temporal"
)

// A temporal tx pins the aggregator's files through its aggtx, so Close must not
// tear those files down while such a tx is still open. Detached readers exist at
// shutdown (the background block-retire goroutine is one), and they are only
// joined by the mdbx read-tx drain inside RwDB.Close.
func TestTemporalDBClose_KeepsFilesUntilOpenTxIsDone(t *testing.T) {
	t.Parallel()

	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: 2})
	require.NotZero(t, agg.EndTxNumMinimax())

	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		db.Close()
	}()

	// Close is in progress once the underlying mdbx db refuses new read txs.
	raw := db.(*temporal.DB).InternalDB()
	for {
		rtx, err := raw.BeginRo(t.Context())
		if err != nil {
			break
		}
		rtx.Rollback()
		time.Sleep(time.Millisecond)
	}

	require.NotZero(t, agg.EndTxNumMinimax(), "state files were closed while a temporal tx was still open")

	tx.Rollback()
	select {
	case <-closed:
	case <-time.After(time.Minute):
		t.Fatal("Close did not return after the temporal tx was rolled back")
	}
	require.Zero(t, agg.EndTxNumMinimax())
}
