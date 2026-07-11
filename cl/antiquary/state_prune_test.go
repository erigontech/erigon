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

package antiquary

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/node/ethconfig"
)

func seedStateSlots(t *testing.T, db kv.RwDB, table string, slots []uint64) {
	t.Helper()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for _, s := range slots {
			if err := tx.Put(table, base_encoding.Encode64ToBytes4(s), []byte{1}); err != nil {
				return err
			}
		}
		return nil
	}))
}

func tableSlots(t *testing.T, db kv.RwDB, table string) []uint64 {
	t.Helper()
	slots := []uint64{}
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		return tx.ForEach(table, nil, func(k, _ []byte) error {
			slots = append(slots, base_encoding.Decode64FromBytes4(k))
			return nil
		})
	}))
	return slots
}

func pruneMarker(t *testing.T, db kv.RwDB, table string) uint64 {
	t.Helper()
	var m uint64
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		m, err = state_accessors.ReadStatePruneProgress(tx, table)
		return err
	}))
	return m
}

func slotRange(from, to uint64) []uint64 {
	slots := make([]uint64, 0, to-from)
	for s := from; s < to; s++ {
		slots = append(slots, s)
	}
	return slots
}

func TestPruneStateTablesBelowBoundary(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.BlockRoot, slotRange(0, 100))
	boundaryFn := func(string) uint64 { return 50 }

	next, err := pruneStateTables(t.Context(), db, []string{kv.BlockRoot}, boundaryFn, 0, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)
	require.Equal(t, slotRange(50, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(50), pruneMarker(t, db, kv.BlockRoot))

	// second call is a no-op: marker already at boundary, no RW txn opened
	commits := &atomic.Int64{}
	counting := &commitCountingDB{RwDB: db, commits: commits}
	next, err = pruneStateTables(t.Context(), counting, []string{kv.BlockRoot}, boundaryFn, 0, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)
	require.Zero(t, commits.Load())
	require.Equal(t, slotRange(50, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(50), pruneMarker(t, db, kv.BlockRoot))
}

func TestPruneStateTablesSparseKeysJumpToBoundary(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.BlockRoot, []uint64{0, 32, 64})

	next, err := pruneStateTables(t.Context(), db, []string{kv.BlockRoot}, func(string) uint64 { return 50 }, 0, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)
	require.Equal(t, []uint64{64}, tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(50), pruneMarker(t, db, kv.BlockRoot))
}

type cancelOnCommitDB struct {
	kv.RwDB
	onCommit func()
}

func (d *cancelOnCommitDB) BeginRw(ctx context.Context) (kv.RwTx, error) {
	tx, err := d.RwDB.BeginRw(ctx) //nolint:gocritic // wrapper returns the tx to the caller; it owns Rollback
	if err != nil {
		return nil, err
	}
	return &cancelOnCommitTx{RwTx: tx, onCommit: d.onCommit}, nil
}

type cancelOnCommitTx struct {
	kv.RwTx
	onCommit func()
}

func (t *cancelOnCommitTx) Commit() error {
	err := t.RwTx.Commit()
	t.onCommit()
	return err
}

func TestPruneStateTablesResumesAfterDeadline(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.BlockRoot, slotRange(0, 100))
	boundaryFn := func(string) uint64 { return 50 }

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	interrupted := &cancelOnCommitDB{RwDB: db, onCommit: cancel}

	next, err := pruneStateTables(ctx, interrupted, []string{kv.BlockRoot}, boundaryFn, 0, 10, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)
	require.Equal(t, slotRange(10, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(10), pruneMarker(t, db, kv.BlockRoot))

	next, err = pruneStateTables(t.Context(), db, []string{kv.BlockRoot}, boundaryFn, next, 10, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)
	require.Equal(t, slotRange(50, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(50), pruneMarker(t, db, kv.BlockRoot))
}

func TestPruneStateTablesBatchCommits(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.BlockRoot, slotRange(0, 100))
	commits := &atomic.Int64{}
	counting := &commitCountingDB{RwDB: db, commits: commits}

	next, err := pruneStateTables(t.Context(), counting, []string{kv.BlockRoot}, func(string) uint64 { return 100 }, 0, 10, log.New())
	require.NoError(t, err)
	require.Equal(t, 0, next)
	require.Empty(t, tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(100), pruneMarker(t, db, kv.BlockRoot))
	require.Equal(t, int64(10), commits.Load())
}

func TestPruneStateTablesResumesAtInterruptedTable(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tables := []string{kv.BlockRoot, kv.StateRoot, kv.EpochData}
	for _, table := range tables {
		seedStateSlots(t, db, table, slotRange(0, 100))
	}
	boundaryFn := func(string) uint64 { return 50 }

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	interrupted := &cancelOnCommitDB{RwDB: db, onCommit: cancel}

	// the first commit finishes kv.BlockRoot and cancels: the index of the next
	// unvisited table must come back so the following pass starts there
	next, err := pruneStateTables(ctx, interrupted, tables, boundaryFn, 0, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 1, next)
	require.Equal(t, slotRange(50, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, slotRange(0, 100), tableSlots(t, db, kv.StateRoot))
	require.Equal(t, slotRange(0, 100), tableSlots(t, db, kv.EpochData))

	next, err = pruneStateTables(t.Context(), db, tables, boundaryFn, next, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 1, next)
	for _, table := range tables {
		require.Equal(t, slotRange(50, 100), tableSlots(t, db, table), "table %s", table)
		require.Equal(t, uint64(50), pruneMarker(t, db, table), "table %s", table)
	}
}

func TestPruneStateTablesErrorReturnsFailingTable(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.BlockRoot, slotRange(0, 100))
	tables := []string{kv.BlockRoot, "NonexistentTable"}

	next, err := pruneStateTables(t.Context(), db, tables, func(string) uint64 { return 50 }, 0, 1000, log.New())
	require.Error(t, err)
	require.Equal(t, 1, next)
	// the healthy table was still pruned before the failure
	require.Equal(t, slotRange(50, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(50), pruneMarker(t, db, kv.BlockRoot))
}

func TestStatePruneBudget(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig // 12s slots: base 4s, cap 8s

	require.Equal(t, 4*time.Second, statePruneBudget(cfg, 0))
	require.Equal(t, 6*time.Second, statePruneBudget(cfg, 1000))
	require.Equal(t, 8*time.Second, statePruneBudget(cfg, 1_000_000))
}

func TestStatePruneEnvConfig(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	newA := func() *Antiquary {
		return NewAntiquary(context.Background(), nil, nil, nil, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, nil, nil, nil, nil, log.New(), true, true, true, false, nil)
	}
	require.False(t, newA().statePruneDisabled)
	require.Zero(t, newA().statePruneTimeout)

	t.Setenv("CAPLIN_STATE_PRUNE_DISABLE", "true")
	t.Setenv("CAPLIN_STATE_PRUNE_TIMEOUT", "150ms")
	a := newA()
	require.True(t, a.statePruneDisabled)
	require.Equal(t, 150*time.Millisecond, a.statePruneTimeout)
}

func TestStatePruneBacklog(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()
	a := NewAntiquary(ctx, nil, nil, nil, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, nil, nil, nil, nil, log.New(), true, true, true, false, nil)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		if err := state_accessors.SetStatePruneProgress(tx, kv.BlockRoot, 10); err != nil {
			return err
		}
		return state_accessors.SetStatePruneProgress(tx, kv.EpochData, 70)
	}))
	boundaries := map[string]uint64{kv.BlockRoot: 50, kv.StateRoot: 100, kv.EpochData: 0}
	tables := []string{kv.BlockRoot, kv.StateRoot, kv.EpochData}

	// (50-10) + (100-0); zero-boundary tables contribute nothing
	backlog := a.statePruneBacklog(ctx, tables, func(table string) uint64 { return boundaries[table] })
	require.Equal(t, uint64(140), backlog)

	// a marker at or past the boundary contributes nothing
	boundaries[kv.EpochData] = 70
	backlog = a.statePruneBacklog(ctx, tables, func(table string) uint64 { return boundaries[table] })
	require.Equal(t, uint64(140), backlog)
}

func TestFloorStatePruneMarkers(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()
	stateSn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), snapshotsync.MakeCaplinStateSnapshotsTypes(db), log.New())
	a := NewAntiquary(ctx, nil, nil, nil, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, stateSn, nil, nil, nil, log.New(), true, true, true, false, nil)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		if err := state_accessors.SetStatePruneProgress(tx, kv.BlockRoot, 100); err != nil {
			return err
		}
		return state_accessors.SetStatePruneProgress(tx, kv.StateRoot, 30)
	}))

	require.NoError(t, a.floorStatePruneMarkers(ctx, 50))
	require.Equal(t, uint64(50), pruneMarker(t, db, kv.BlockRoot))
	require.Equal(t, uint64(30), pruneMarker(t, db, kv.StateRoot))
}

func TestStatePruneKillSwitch(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.BlockRoot, slotRange(0, 100))
	ctx := context.Background()
	stateSn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), snapshotsync.MakeCaplinStateSnapshotsTypes(db), log.New())
	a := NewAntiquary(ctx, nil, nil, nil, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, stateSn, nil, nil, nil, log.New(), true, true, true, false, nil)
	a.statePruneBoundaryFn = func(string) uint64 { return 50 }

	a.statePruneDisabled = true
	a.pruneFrozenStateTables(ctx, math.MaxUint64)
	require.Equal(t, slotRange(0, 100), tableSlots(t, db, kv.BlockRoot))
	require.Zero(t, pruneMarker(t, db, kv.BlockRoot))

	// an already-expired timeout override must stop the pass before any delete
	a.statePruneDisabled = false
	a.statePruneTimeout = time.Nanosecond
	a.pruneFrozenStateTables(ctx, math.MaxUint64)
	require.Equal(t, slotRange(0, 100), tableSlots(t, db, kv.BlockRoot))
	require.Zero(t, pruneMarker(t, db, kv.BlockRoot))

	a.statePruneTimeout = 0
	a.pruneFrozenStateTables(ctx, math.MaxUint64)
	require.Equal(t, slotRange(50, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(50), pruneMarker(t, db, kv.BlockRoot))
}

func TestPruneFrozenStateTablesCapsBoundaryToFlushed(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.BlockRoot, slotRange(0, 100))
	ctx := context.Background()
	stateSn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), snapshotsync.MakeCaplinStateSnapshotsTypes(db), log.New())
	a := NewAntiquary(ctx, nil, nil, nil, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, stateSn, nil, nil, nil, log.New(), true, true, true, false, nil)
	a.statePruneBoundaryFn = func(string) uint64 { return 80 }

	// Coverage says 80, but a resumed reconstruction has only re-flushed rows
	// below 30 so far. The pass must stop at 30 and leave [30,80) for later
	// passes rather than jump the marker to 80 and strand the rows the
	// reconstruction still writes below coverage.
	a.pruneFrozenStateTables(ctx, 30)
	require.Equal(t, slotRange(30, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(30), pruneMarker(t, db, kv.BlockRoot))

	// Once the rest of the below-coverage window is flushed, the next pass
	// resumes from 30 and drains up to coverage.
	a.pruneFrozenStateTables(ctx, 80)
	require.Equal(t, slotRange(80, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, uint64(80), pruneMarker(t, db, kv.BlockRoot))
}

func TestStatePruneWiredIntoAntiquaryCycle(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()
	to := blocks[len(blocks)-1].Block.Slot + 33
	// the fixture chain sits far above this boundary, so the cycle never
	// re-flushes rows below it and the seeded rows are the only prunable ones
	const boundary = uint64(10)

	run := func(disabled bool) (int64, []uint64) {
		db := memdb.NewTestDB(t, dbcfg.ChainDB)
		reader := tests.LoadChain(blocks, postState, db, t)
		seedStateSlots(t, db, kv.BlockRoot, slotRange(0, boundary))
		sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
		sn.OnHeadState(postState)
		ctx := context.Background()
		vt := state_accessors.NewStaticValidatorTable()
		stateSn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{}, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), snapshotsync.MakeCaplinStateSnapshotsTypes(db), log.New())
		a := NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, stateSn, nil, reader, sn, log.New(), true, true, true, false, nil)
		a.maxSlotsPerCommit = 8
		a.statePruneDisabled = disabled
		calls := &atomic.Int64{}
		a.statePruneBoundaryFn = func(table string) uint64 {
			calls.Add(1)
			if table == kv.BlockRoot {
				return boundary
			}
			return 0
		}
		require.NoError(t, a.IncrementBeaconState(ctx, to))
		var belowBoundary []uint64
		for _, s := range tableSlots(t, db, kv.BlockRoot) {
			if s < boundary {
				belowBoundary = append(belowBoundary, s)
			}
		}
		return calls.Load(), belowBoundary
	}

	calls, below := run(true)
	require.Zero(t, calls)
	require.Equal(t, slotRange(0, boundary), below)

	calls, below = run(false)
	require.Positive(t, calls)
	require.Empty(t, below)
}

func TestPruneStateTablesRotationAndZeroBoundary(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.BlockRoot, slotRange(0, 100))
	seedStateSlots(t, db, kv.StateRoot, slotRange(0, 100))
	seedStateSlots(t, db, kv.EpochData, slotRange(0, 10))
	boundaries := map[string]uint64{kv.BlockRoot: 50, kv.StateRoot: 70, kv.EpochData: 0}
	boundaryFn := func(table string) uint64 { return boundaries[table] }
	tables := []string{kv.BlockRoot, kv.StateRoot, kv.EpochData}

	next, err := pruneStateTables(t.Context(), db, tables, boundaryFn, 1, 1000, log.New())
	require.NoError(t, err)
	require.Equal(t, 1, next)
	require.Equal(t, slotRange(50, 100), tableSlots(t, db, kv.BlockRoot))
	require.Equal(t, slotRange(70, 100), tableSlots(t, db, kv.StateRoot))
	require.Equal(t, slotRange(0, 10), tableSlots(t, db, kv.EpochData))
	require.Zero(t, pruneMarker(t, db, kv.EpochData))
}
