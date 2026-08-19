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

package stagedsync

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// TestNoPruneSkipsAllPruneStages verifies that when --exec.no-prune is set
// (dbg.NoPrune() == true), each staged-sync prune entrypoint is a no-op
// against the MDBX tables it would otherwise delete rows from.
func TestNoPruneSkipsAllPruneStages(t *testing.T) {
	orig := dbg.NoPrune()
	t.Cleanup(func() { dbg.SetNoPrune(orig) })
	dbg.SetNoPrune(true)

	ctx := context.Background()
	logger := log.New()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Seed rows in every table any of the prune stages would target.
	type seedRow struct{ table, key, value string }
	seeds := []seedRow{
		{kv.ChangeSets3, "k1", "v1"},
		{kv.ChangeSets3, "k2", "v2"},
		{kv.BlockAccessList, "b1", "ba1"},
		{kv.BlockAccessList, "b2", "ba2"},
		{kv.TxLookup, "t1", "tl1"},
		{kv.BorWitnesses, "w1", "wit1"},
	}
	for _, s := range seeds {
		require.NoError(t, tx.Put(s.table, []byte(s.key), []byte(s.value)))
	}
	countRows := func(t *testing.T, table string) int {
		c, err := tx.Cursor(table)
		require.NoError(t, err)
		defer c.Close()
		n := 0
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			require.NoError(t, err)
			n++
		}
		return n
	}
	tracked := []string{kv.ChangeSets3, kv.BlockAccessList, kv.TxLookup, kv.BorWitnesses}
	pre := map[string]int{}
	for _, table := range tracked {
		pre[table] = countRows(t, table)
		require.Greater(t, pre[table], 0, "expected seeded rows in %s", table)
	}

	// ForwardProgress is well past MaxReorgDepth so the inner rawdb.PruneTable /
	// PruneSmallBatches calls would normally fire. Each prune function
	// early-returns on dbg.NoPrune() before reading any cfg field, so zero-value
	// cfgs are safe.
	const forward uint64 = 10_000
	require.NoError(t, PruneExecutionStage(ctx, &PruneState{ID: stages.Execution, ForwardProgress: forward}, tx, ExecuteBlockCfg{}, 0, logger))
	require.NoError(t, PruneTxLookup(&PruneState{ID: stages.TxLookup, ForwardProgress: forward}, tx, TxLookupCfg{}, ctx, logger))
	require.NoError(t, PruneWitnessProcessingStage(&PruneState{ID: stages.WitnessProcessing, ForwardProgress: forward}, tx, WitnessProcessingCfg{}, ctx, logger))
	require.NoError(t, SnapshotsPrune(&PruneState{ID: stages.Snapshots, ForwardProgress: forward}, SnapshotsCfg{}, ctx, tx, logger))

	for _, table := range tracked {
		require.Equal(t, pre[table], countRows(t, table), "table %s lost rows under --exec.no-prune", table)
	}
}

// TestNoPruneFlagBookkeeping confirms each prune stage still records its
// PruneProgress when skipping work — otherwise the staged-sync state machine
// would re-enter the prune step on every cycle.
func TestNoPruneFlagBookkeeping(t *testing.T) {
	orig := dbg.NoPrune()
	t.Cleanup(func() { dbg.SetNoPrune(orig) })
	dbg.SetNoPrune(true)

	ctx := context.Background()
	logger := log.New()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const forward uint64 = 12_345
	require.NoError(t, PruneExecutionStage(ctx, &PruneState{ID: stages.Execution, ForwardProgress: forward}, tx, ExecuteBlockCfg{}, 0, logger))
	require.NoError(t, PruneTxLookup(&PruneState{ID: stages.TxLookup, ForwardProgress: forward}, tx, TxLookupCfg{}, ctx, logger))
	require.NoError(t, PruneWitnessProcessingStage(&PruneState{ID: stages.WitnessProcessing, ForwardProgress: forward}, tx, WitnessProcessingCfg{}, ctx, logger))

	for _, id := range []stages.SyncStage{stages.Execution, stages.TxLookup, stages.WitnessProcessing} {
		got, err := stages.GetStagePruneProgress(tx, id)
		require.NoError(t, err)
		require.Equal(t, forward, got, "stage %s did not record PruneProgress under --exec.no-prune", id)
	}
}

// seedTxLookupPruneFixture builds the minimal state PruneTxLookup reads: a
// txNum index for every block, TxLookup rows carrying blockNum||txNum, and a
// Headers table whose oldest surviving entry is firstHeader — which is what a
// node that started from downloaded snapshots looks like, since PruneAncientBlocks
// deletes headers up to the same frontier this stage prunes to.
func seedTxLookupPruneFixture(t *testing.T, tx kv.RwTx, lastBlock, firstHeader uint64) {
	t.Helper()
	txNumReader := freezeblocks.NewBlockReader(nil, nil).TxnumReader()
	for blockNum := uint64(0); blockNum <= lastBlock; blockNum++ {
		require.NoError(t, txNumReader.Append(tx, blockNum, blockNum*2+1))
	}
	for blockNum := uint64(1); blockNum <= lastBlock; blockNum++ {
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[:], blockNum)
		val := make([]byte, 16)
		binary.BigEndian.PutUint64(val[:8], blockNum)
		binary.BigEndian.PutUint64(val[8:], blockNum*2)
		require.NoError(t, tx.Put(kv.TxLookup, hash[:], val))
		if blockNum >= firstHeader {
			require.NoError(t, tx.Put(kv.Headers, dbutils.HeaderKey(blockNum, hash), []byte{1}))
		}
	}
	var genesis common.Hash
	require.NoError(t, tx.Put(kv.Headers, dbutils.HeaderKey(0, genesis), []byte{1}))
}

func txLookupRowCount(t *testing.T, tx kv.Tx) int {
	t.Helper()
	n, err := tx.Count(kv.TxLookup)
	require.NoError(t, err)
	return int(n)
}

// TestPruneTxLookupFloorNotTiedToHeaders pins the prune range floor. The floor
// must not come from kv.Headers: another stage deletes headers up to this
// stage's own blockTo, so reading it back yields an empty range forever and
// TxLookup never shrinks.
func TestPruneTxLookupFloorNotTiedToHeaders(t *testing.T) {
	ctx := context.Background()
	logger := log.New()

	const lastBlock, forward, distance = 2_000, uint64(2_000), uint64(100)

	for _, tc := range []struct {
		name        string
		firstHeader uint64
	}{
		{name: "headers_from_genesis", firstHeader: 1},
		{name: "headers_pruned_to_frontier", firstHeader: forward - distance},
		{name: "headers_pruned_past_frontier", firstHeader: forward - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			tx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			seedTxLookupPruneFixture(t, tx, lastBlock, tc.firstHeader)
			before := txLookupRowCount(t, tx)
			require.Positive(t, before)

			cfg := StageTxLookupCfg(prune.Mode{Initialised: true, History: prune.Distance(distance)},
				t.TempDir(), freezeblocks.NewBlockReader(nil, nil))
			s := &PruneState{ID: stages.TxLookup, ForwardProgress: forward,
				CurrentSyncCycle: CurrentSyncCycleInfo{IsInitialCycle: true}}
			require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))

			require.Less(t, txLookupRowCount(t, tx), before,
				"no rows pruned: the range floor tracked kv.Headers instead of the stage's own progress")
		})
	}
}

// TestPruneTxLookupForwardAheadOfExecution covers the stage ordering where
// TxLookup has run further than Execution/Senders: the prune range is driven by
// TxLookup's own ForwardProgress, so it must still prune and must not walk past
// its retention distance.
func TestPruneTxLookupForwardAheadOfExecution(t *testing.T) {
	ctx := context.Background()
	logger := log.New()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const lastBlock, forward, distance = 2_000, uint64(2_000), uint64(100)
	seedTxLookupPruneFixture(t, tx, lastBlock, 1)
	// Execution/Senders lag behind TxLookup.
	require.NoError(t, stages.SaveStageProgress(tx, stages.Senders, 500))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 500))

	cfg := StageTxLookupCfg(prune.Mode{Initialised: true, History: prune.Distance(distance)},
		t.TempDir(), freezeblocks.NewBlockReader(nil, nil))
	s := &PruneState{ID: stages.TxLookup, ForwardProgress: forward,
		CurrentSyncCycle: CurrentSyncCycleInfo{IsInitialCycle: true}}
	require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))

	// Rows at or above the retention frontier must survive.
	var keep common.Hash
	binary.BigEndian.PutUint64(keep[:], forward-1)
	v, err := tx.GetOne(kv.TxLookup, keep[:])
	require.NoError(t, err)
	require.NotNil(t, v, "pruned past the retention distance")
}
