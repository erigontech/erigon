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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
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

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Seed rows in every table any of the prune stages would target.
	type seedRow struct{ table, key, value string }
	seeds := []seedRow{
		{kv.ChangeSets3, "k1", "v1"},
		{kv.ChangeSets3, "k2", "v2"},
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
	tracked := []string{kv.ChangeSets3, kv.TxLookup, kv.BorWitnesses}
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

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(ctx)
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
