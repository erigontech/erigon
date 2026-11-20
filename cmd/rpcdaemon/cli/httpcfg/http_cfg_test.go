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

package httpcfg

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/exec"
)

func TestReadTxLimitCoversExecReaders(t *testing.T) {
	t.Parallel()
	// The limit must exceed every long-lived read tx a parallel batch holds
	// (see execPermanentReadTxs and execReadAheadTxs) even when GOMAXPROCS is
	// set below NumCPU and shrinks the derived default.
	require.Greater(t, RoTxsLimit(0, runtime.NumCPU(), runtime.NumCPU(), runtime.NumCPU(), runtime.NumCPU()), int64(4*runtime.NumCPU()+execPermanentReadTxs+execReadAheadTxs))
}

func TestRoTxsLimit(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	defaultLimit := int64(DefaultDBReadConcurrency())
	floor := func(execWorkers, parallelCommitmentReaders, warmupWorkers, blockReadAheadWorkers int) int64 {
		return int64(execWorkers + parallelCommitmentReaders + warmupWorkers + blockReadAheadWorkers + execPermanentReadTxs + execReadAheadTxs + dbReadTxsReserved)
	}
	for _, tc := range []struct {
		name                                                                              string
		cfg, execWorkers, parallelCommitmentReaders, warmupWorkers, blockReadAheadWorkers int
		want                                                                              int64
	}{
		{"default passes through when above floor", 0, 4, 4, 4, 4, defaultLimit},
		{"high explicit value passes through", 5000, 8, 8, 8, 8, 5000},
		{"low explicit value raised to floor", 8, 64, 6, 6, 6, floor(64, 6, 6, 6)},
		{"all worker pools are counted", 8, 8, 6, 6, 6, 49},
		{"disabled pools add no readers", 8, 8, 6, 0, 0, 37},
		{"default floored below worker count", 0, int(defaultLimit) + 1, 6, 6, 6, floor(int(defaultLimit)+1, 6, 6, 6)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, RoTxsLimit(tc.cfg, tc.execWorkers, tc.parallelCommitmentReaders, tc.warmupWorkers, tc.blockReadAheadWorkers))
		})
	}
}

func TestRoTxsLimitWithBranchPrefetcher(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	limit := RoTxsLimit(1, 1, 0, 0, 0)
	rawDB := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.New()), t.TempDir()).
		RoTxsLimiter(semaphore.NewWeighted(limit)).MustOpen()
	agg := state.NewTest(datadir.New(t.TempDir())).MustOpen(t.Context())
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)

	ctx := kv.WithNonBlockingAcquire(t.Context())
	for range dbReadTxsReserved {
		tx, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(tx.Rollback)
	}
	p := exec.NewBranchPrefetcher(ctx, db)
	t.Cleanup(p.Close)
	for range 1 + execPermanentReadTxs + execReadAheadTxs {
		tx, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err, "execution reader needs a permit while the prefetcher is open")
		t.Cleanup(tx.Rollback)
	}
}
