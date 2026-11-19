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

package antiquary

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

func TestShouldBackoffHistoricalStateRead(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "missing GLOAS data", err: historical_states_reader.ErrMissingGloasData, want: true},
		{name: "missing history vector", err: historical_states_reader.ErrMissingHistoryVectorData, want: true},
		{name: "corrupt queue diff", err: base_encoding.ErrCorruptQueueDiff, want: true},
		{name: "history rebuild required", err: state_accessors.ErrHistoryRebuildRequired, want: true},
		{name: "generic error", err: errors.New("read failed"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := fmt.Errorf("failed to read builders: %w", test.err)
			require.Equal(t, test.want, shouldBackoffHistoricalStateRead(err))
		})
	}
}

func runTest(t *testing.T, blocks []*cltypes.SignedBeaconBlock, preState, postState *state.CachingBeaconState) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	reader := tests.LoadChain(blocks, db, t)
	sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, sn.OnHeadState(postState))
	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, nil, nil, reader, sn, log.New(), true, true, true, false, nil)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
}

func TestStateAntiquaryElectra(t *testing.T) {
	blocks, preState, postState := tests.GetElectraRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryCapella(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryBellatrix(t *testing.T) {
	blocks, preState, postState := tests.GetBellatrixRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryPhase0(t *testing.T) {
	blocks, preState, postState := tests.GetPhase0Random()
	runTest(t, blocks, preState, postState)
}

type finalityReadDB struct {
	kv.RwDB
	firstFinalityRead     chan struct{}
	firstFinalityReadOnce sync.Once
}

func (d *finalityReadDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	tx, err := d.RwDB.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	return &finalityReadTx{Tx: tx, onRead: func() {
		d.firstFinalityReadOnce.Do(func() { close(d.firstFinalityRead) })
	}}, nil
}

type finalityReadTx struct {
	kv.Tx
	onRead func()
}

func (tx *finalityReadTx) GetOne(table string, key []byte) ([]byte, error) {
	value, err := tx.Tx.GetOne(table, key)
	if table == kv.HighestFinalized {
		tx.onRead()
	}
	return value, err
}

func TestLoopStatesReconstructsFinalizedSlot(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()
	db := &finalityReadDB{RwDB: mdbxtest.NewTestDB(t, dbcfg.ChainDB), firstFinalityRead: make(chan struct{})}
	reader := tests.LoadChain(blocks, db, t)
	syncedData := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, syncedData.OnHeadState(postState))
	finalized := blocks[len(blocks)-1].Block.Slot
	require.Greater(t, finalized, uint64(0))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestFinalized(tx, finalized-1)
	}))

	a := NewAntiquary(t.Context(), nil, preState, state_accessors.NewStaticValidatorTable(), &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, nil, nil, reader, syncedData, log.New(), true, true, true, false, nil)
	a.sn = &freezeblocks.CaplinSnapshots{}
	a.backfilled.Store(true)
	require.False(t, syncedData.Syncing())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		a.loopStates(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-db.firstFinalityRead:
	case <-done:
		t.Fatal("state loop stopped before its first finality read")
	case <-timer.C:
		t.Fatal("state loop did not perform its first finality read")
	}
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestFinalized(tx, finalized)
	}))

	var progress uint64
	var viewErr error
	require.Eventually(t, func() bool {
		viewErr = db.View(ctx, func(tx kv.Tx) error {
			var err error
			progress, err = state_accessors.GetStateProcessingProgress(tx)
			return err
		})
		return viewErr == nil && progress >= finalized
	}, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, viewErr)
	require.Equal(t, finalized, progress)
}

func TestIncrementToFinalizedOverAMissedEpochStart(t *testing.T) {
	blocks, preState, postState := tests.GetElectraRandom()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	reader := tests.LoadChain(blocks, db, t)
	sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, sn.OnHeadState(postState))
	genesis, err := preState.Copy()
	require.NoError(t, err)
	a := NewAntiquary(context.Background(), nil, preState, state_accessors.NewStaticValidatorTable(), &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, nil, nil, reader, sn, log.New(), true, true, true, false, nil)
	const missed = 8288
	require.Equal(t, uint64(8258), blocks[len(blocks)-2].Block.Slot)
	require.Equal(t, uint64(8290), blocks[len(blocks)-1].Block.Slot)
	ctx := context.Background()

	require.NoError(t, a.IncrementBeaconState(ctx, missed+1))
	require.NoError(t, a.IncrementBeaconState(ctx, missed+33))
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		validators := state_accessors.NewStaticValidatorTable()
		require.NoError(t, state_accessors.ReadValidatorsTable(tx, validators))
		historical := historical_states_reader.NewHistoricalStatesReader(&clparams.MainnetBeaconConfig, reader, validators, genesis, nil, sn)
		got, err := historical.ReadHistoricalState(ctx, tx, 8290)
		require.NoError(t, err)
		require.True(t, bytes.Equal(postState.RawBalances(), got.RawBalances()), "historical balances at 8290 differ from the post-state")
		return nil
	}))
}

type commitCountingDB struct {
	kv.RwDB
	commits *atomic.Int64
}

func (d *commitCountingDB) BeginRw(ctx context.Context) (kv.RwTx, error) {
	tx, err := d.RwDB.BeginRw(ctx) //nolint:gocritic // wrapper returns the tx to the caller; it owns Rollback
	if err != nil {
		return nil, err
	}
	return &commitCountingTx{RwTx: tx, commits: d.commits}, nil
}

type commitCountingTx struct {
	kv.RwTx
	commits *atomic.Int64
}

func (t *commitCountingTx) Commit() error {
	t.commits.Add(1)
	return t.RwTx.Commit()
}

// A single antiquary run must reconstruct the same slots whether committed in
// one big transaction or in maxSlotsPerCommit-bounded batches, and the bounded
// run must flush more often — bounding the per-commit retired-page list is what
// keeps libmdbx's gc_fill_returned from overflowing on large DBs.
func TestStateAntiquaryCommitIsBounded(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()
	to := blocks[len(blocks)-1].Block.Slot + 33

	run := func(maxSlotsPerCommit uint64) (commits int64, progress uint64) {
		db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
		reader := tests.LoadChain(blocks, db, t)
		sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
		require.NoError(t, sn.OnHeadState(postState))
		ctx := context.Background()
		vt := state_accessors.NewStaticValidatorTable()
		counter := &atomic.Int64{}
		countingDB := &commitCountingDB{RwDB: db, commits: counter}
		a := NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, countingDB, nil, nil, reader, sn, log.New(), true, true, true, false, nil)
		a.maxSlotsPerCommit = maxSlotsPerCommit
		require.NoError(t, a.IncrementBeaconState(ctx, to))
		require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
			var e error
			progress, e = state_accessors.GetStateProcessingProgress(tx)
			return e
		}))
		return counter.Load(), progress
	}

	singleCommits, singleProgress := run(1 << 62)
	boundedCommits, boundedProgress := run(8)

	require.Equal(t, singleProgress, boundedProgress)
	require.Greater(t, boundedCommits, singleCommits)
	require.GreaterOrEqual(t, boundedCommits, singleCommits+2)
}
