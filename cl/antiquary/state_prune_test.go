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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
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
	require.GreaterOrEqual(t, commits.Load(), int64(10))
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
