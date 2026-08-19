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

const (
	txlBlocks   = uint64(2_000)
	txlDistance = uint64(100)
)

func txLookupFixture(t *testing.T, firstHeader, pruneProgress, senders uint64) (kv.TemporalRwTx, TxLookupCfg, *PruneState) {
	t.Helper()
	dir := t.TempDir()
	db := temporaltest.NewTestDB(t, datadir.New(dir))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	txNums := freezeblocks.NewBlockReader(nil, nil).TxnumReader()
	require.NoError(t, tx.Put(kv.Headers, dbutils.HeaderKey(0, common.Hash{}), []byte{1}))
	for b := uint64(0); b <= txlBlocks; b++ {
		require.NoError(t, txNums.Append(tx, b, b*2+1))
	}
	for b := uint64(1); b <= txlBlocks; b++ {
		var h common.Hash
		binary.BigEndian.PutUint64(h[:], b)
		val := make([]byte, 16)
		binary.BigEndian.PutUint64(val[:8], b)
		binary.BigEndian.PutUint64(val[8:], b*2)
		require.NoError(t, tx.Put(kv.TxLookup, h[:], val))
		if b >= firstHeader {
			require.NoError(t, tx.Put(kv.Headers, dbutils.HeaderKey(b, h), []byte{1}))
		}
	}
	if pruneProgress > 0 {
		require.NoError(t, stages.SaveStagePruneProgress(tx, stages.TxLookup, pruneProgress))
	}
	if senders > 0 {
		require.NoError(t, stages.SaveStageProgress(tx, stages.Senders, senders))
		require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, senders))
	}

	cfg := StageTxLookupCfg(prune.Mode{Initialised: true, History: prune.Distance(txlDistance)},
		dir, freezeblocks.NewBlockReader(nil, nil))
	s := &PruneState{ID: stages.TxLookup, ForwardProgress: txlBlocks, PruneProgress: pruneProgress,
		CurrentSyncCycle: CurrentSyncCycleInfo{IsInitialCycle: true}}
	return tx, cfg, s
}

func txLookupCount(t *testing.T, tx kv.Tx) int {
	t.Helper()
	n, err := tx.Count(kv.TxLookup)
	require.NoError(t, err)
	return int(n)
}

func txLookupRow(t *testing.T, tx kv.Tx, block uint64) []byte {
	t.Helper()
	var h common.Hash
	binary.BigEndian.PutUint64(h[:], block)
	v, err := tx.GetOne(kv.TxLookup, h[:])
	require.NoError(t, err)
	return v
}

// The floor must not track blockTo: kv.Headers is deleted to the same frontier,
// and SpawnTxLookup sets PruneProgress to FrozenBlocks().
func TestPruneTxLookupFloor(t *testing.T) {
	for _, tc := range []struct {
		name                                string
		firstHeader, pruneProgress, senders uint64
	}{
		{name: "headers_from_genesis", firstHeader: 1},
		{name: "headers_pruned_to_frontier", firstHeader: txlBlocks - txlDistance},
		{name: "headers_pruned_past_frontier", firstHeader: txlBlocks - 1},
		{name: "forward_stage_watermark", firstHeader: 1, pruneProgress: txlBlocks - txlDistance},
		// guards that blockTo stays derived from this stage, not from Senders
		{name: "ahead_of_execution", firstHeader: 1, senders: 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, cfg, s := txLookupFixture(t, tc.firstHeader, tc.pruneProgress, tc.senders)
			before := txLookupCount(t, tx)
			require.NoError(t, PruneTxLookup(s, tx, cfg, context.Background(), log.New()))
			require.Less(t, txLookupCount(t, tx), before)

			blockTo := txlBlocks - txlDistance // exclusive end of the range
			require.Nil(t, txLookupRow(t, tx, blockTo-1), "left a row below blockTo")
			require.NotNil(t, txLookupRow(t, tx, blockTo), "pruned blockTo itself")
		})
	}
}
