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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
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

const (
	txlBlocks     = uint64(3_000)
	txlTxPerBlock = uint64(3)
	txlFrozen     = uint64(1_500) // blocks in snapshots
)

// txlBlockReader serves the two methods PruneTxLookup uses; anything else is a
// nil-panic, which is the point.
type txlBlockReader struct {
	services.FullBlockReader
	frozen uint64
}

func (r txlBlockReader) CanPruneTo(cur uint64) uint64 {
	return freezeblocks.CanDeleteTo(cur, r.frozen)
}
func (r txlBlockReader) TxnumReader() rawdbv3.TxNumsReader {
	return freezeblocks.NewBlockReader(nil, nil).TxnumReader()
}

// txlTxHash mimics production keys: unrelated to block order, so the table is
// walked in hash order like the real one.
func txlTxHash(block, i uint64) common.Hash {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], block)
	binary.BigEndian.PutUint64(b[8:], i)
	return common.Hash(sha256.Sum256(b[:]))
}

// A block occupies [Min(b) system][Min(b)+1 .. Min(b)+n txns][Max(b) system],
// and txnLookupTransform writes a row only for the txns.
func txlMinTxNum(block uint64) uint64 {
	if block == 0 {
		return 0 // genesis: [Min system][Max system], no txns
	}
	return 2 + (block-1)*(txlTxPerBlock+2)
}
func txlMaxTxNum(block uint64) uint64 {
	if block == 0 {
		return 1
	}
	return txlMinTxNum(block) + txlTxPerBlock + 1
}

func txLookupFixture(t *testing.T, firstHeader, pruneProgress, senders, staleFloor, resumeFrom uint64) (kv.RwTx, TxLookupCfg, *PruneState) {
	t.Helper()
	dir := t.TempDir()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	txNums := freezeblocks.NewBlockReader(nil, nil).TxnumReader()
	require.NoError(t, tx.Put(kv.Headers, dbutils.HeaderKey(0, common.Hash{}), []byte{1}))
	for b := uint64(0); b <= txlBlocks; b++ {
		require.NoError(t, txNums.Append(tx, b, txlMaxTxNum(b)))
	}
	for b := uint64(1); b <= txlBlocks; b++ {
		for i := range txlTxPerBlock {
			val := make([]byte, 16)
			binary.BigEndian.PutUint64(val[:8], b)
			binary.BigEndian.PutUint64(val[8:], txlMinTxNum(b)+i+1)
			h := txlTxHash(b, i)
			require.NoError(t, tx.Put(kv.TxLookup, h[:], val))
		}
		if b >= firstHeader {
			var hh common.Hash
			binary.BigEndian.PutUint64(hh[:], b)
			require.NoError(t, tx.Put(kv.Headers, dbutils.HeaderKey(b, hh), []byte{1}))
		}
	}
	if pruneProgress > 0 {
		require.NoError(t, stages.SaveStagePruneProgress(tx, stages.TxLookup, pruneProgress))
	}
	if senders > 0 {
		require.NoError(t, stages.SaveStageProgress(tx, stages.Senders, senders))
		require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, senders))
	}
	if resumeFrom > 0 {
		// an interrupted rotation under the current floor: must be resumed, not restarted
		h := txlTxHash(resumeFrom, 0)
		require.NoError(t, state.SavePruneValProgress(tx, kv.TxLookup, &prune.Stat{
			TxFrom: 0, TxTo: 1, ValueProgress: prune.InProgress, LastPrunedValue: h[:],
		}))
	}
	if staleFloor > 0 {
		// what a pre-fix binary left: non-zero floor, TxTo from Max(blockTo), rotation Done
		h := txlTxHash(txlBlocks/2, 0)
		require.NoError(t, state.SavePruneValProgress(tx, kv.TxLookup, &prune.Stat{
			TxFrom: staleFloor, TxTo: txlMaxTxNum(txlBlockTo()),
			ValueProgress: prune.Done, LastPrunedValue: h[:],
		}))
	}

	cfg := StageTxLookupCfg(prune.Mode{Initialised: true, History: prune.Distance(config3.DefaultPruneDistance)},
		dir, txlBlockReader{frozen: txlFrozen})
	s := &PruneState{ID: stages.TxLookup, ForwardProgress: txlBlocks, PruneProgress: pruneProgress,
		CurrentSyncCycle: CurrentSyncCycleInfo{IsInitialCycle: true}}
	return tx, cfg, s
}

func txlBlockRows(t *testing.T, tx kv.Tx, block uint64) int {
	t.Helper()
	n := 0
	for i := range txlTxPerBlock {
		h := txlTxHash(block, i)
		v, err := tx.GetOne(kv.TxLookup, h[:])
		require.NoError(t, err)
		if v != nil {
			n++
		}
	}
	return n
}

// The bound is the snapshot frontier: rows below it duplicate the .idx files.
func txlBlockTo() uint64 { return freezeblocks.CanDeleteTo(txlBlocks, txlFrozen) }

// The floor must not track the bound: kv.Headers is deleted to the same frontier,
// and SpawnTxLookup sets PruneProgress to FrozenBlocks().
func TestPruneTxLookupFloor(t *testing.T) {
	for _, tc := range []struct {
		name                                                        string
		firstHeader, pruneProgress, senders, staleFloor, resumeFrom uint64
	}{
		{name: "headers_from_genesis", firstHeader: 1},
		{name: "headers_pruned_to_frontier", firstHeader: txlBlockTo()},
		{name: "headers_pruned_past_frontier", firstHeader: txlBlocks - 1},
		{name: "forward_stage_watermark", firstHeader: 1, pruneProgress: txlFrozen},
		{name: "senders_fallback", firstHeader: txlBlocks + 1, senders: 500},
		{name: "stale_rotation", firstHeader: 1, staleFloor: txlMinTxNum(txlBlocks / 2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, cfg, s := txLookupFixture(t, tc.firstHeader, tc.pruneProgress, tc.senders, tc.staleFloor, tc.resumeFrom)
			require.NoError(t, PruneTxLookup(s, tx, cfg, context.Background(), log.New()))

			to := txlBlockTo()
			require.Positive(t, to)
			// every txn below the bound is gone, and the bound itself is untouched
			require.Zero(t, txlBlockRows(t, tx, 1), "left an early block: floor above 0")
			require.Zero(t, txlBlockRows(t, tx, to-1), "left the block below the bound")
			require.EqualValues(t, txlTxPerBlock, txlBlockRows(t, tx, to), "pruned the exclusive bound")
			require.EqualValues(t, txlTxPerBlock, txlBlockRows(t, tx, txlBlocks), "pruned the tip")
		})
	}
}

// An interrupted rotation under the current floor must be resumed, not restarted:
// at the tip the budget cannot finish a pass over the whole table, so restarting
// every cycle would mean no rotation ever completes.
func TestPruneTxLookupResumesInterruptedRotation(t *testing.T) {
	const resumeAt = txlBlocks / 2
	tx, cfg, s := txLookupFixture(t, 1, 0, 0, 0, resumeAt)
	require.NoError(t, PruneTxLookup(s, tx, cfg, context.Background(), log.New()))

	// rows before the saved cursor are not visited this pass — proof it resumed
	require.EqualValues(t, txlTxPerBlock, txlBlockRows(t, tx, 1),
		"restarted from First instead of resuming the saved cursor")

	st, err := state.GetPruneValProgress(tx, []byte(kv.TxLookup))
	require.NoError(t, err)
	require.Equal(t, prune.Done, st.ValueProgress)
}

// A rotation that spans a bound advance resumes past rows the widened range now
// covers, so finishing it must not record the wider bound — that would claim
// coverage it never achieved and short-circuit the pass that would catch them.
func TestPruneTxLookupRotationRecordsItsStartBound(t *testing.T) {
	ctx, logger := context.Background(), log.New()
	tx, cfg, s := txLookupFixture(t, 1, 0, 0, 0, txlBlocks/2)

	// the interrupted rotation started at a lower bound than the current one
	started, err := state.GetPruneValProgress(tx, []byte(kv.TxLookup))
	require.NoError(t, err)
	require.Equal(t, prune.InProgress, started.ValueProgress)
	require.Less(t, started.TxTo, txlMinTxNum(txlBlockTo()))

	require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))

	done, err := state.GetPruneValProgress(tx, []byte(kv.TxLookup))
	require.NoError(t, err)
	require.Equal(t, prune.Done, done.ValueProgress)
	require.Equal(t, started.TxTo, done.TxTo,
		"recorded the widened bound, claiming coverage the resumed rotation skipped")
}

// A completed rotation must survive the tip advancing, or the whole table is
// rescanned once per payload to collect a single block of rows.
func TestPruneTxLookupSkipsRescanAfterRotation(t *testing.T) {
	ctx, logger := context.Background(), log.New()
	tx, cfg, s := txLookupFixture(t, 1, 0, 0, 0, 0)

	require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))
	first, err := state.GetPruneValProgress(tx, []byte(kv.TxLookup))
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, prune.Done, first.ValueProgress)

	// plant a row the bound already covers; only a rescan would remove it
	planted := txlTxHash(txlBlocks+1, 0)
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[8:], first.TxTo-1)
	require.NoError(t, tx.Put(kv.TxLookup, planted[:], val))

	s.ForwardProgress++
	require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))

	got, err := tx.GetOne(kv.TxLookup, planted[:])
	require.NoError(t, err)
	require.NotNil(t, got, "rescanned the whole table though the bound had not moved")
}

// Drives one table through the whole prune lifecycle, asserting the state machine
// at each step rather than each step in isolation.
func TestPruneTxLookupLifecycle(t *testing.T) {
	ctx, logger := context.Background(), log.New()
	tx, cfg, s := txLookupFixture(t, 1, 0, 0, 0, 0)
	progress := func() *prune.Stat {
		st, err := state.GetPruneValProgress(tx, []byte(kv.TxLookup))
		require.NoError(t, err)
		return st
	}
	to := txlBlockTo()

	// 1. fresh node: one rotation clears everything below the bound
	require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))
	require.Equal(t, prune.Done, progress().ValueProgress)
	require.Zero(t, txlBlockRows(t, tx, to-1), "left rows below the bound")
	require.EqualValues(t, txlTxPerBlock, txlBlockRows(t, tx, to), "pruned the exclusive bound")
	afterFirst := progress().TxTo

	// 2. bound unchanged: short-circuits, so a row planted below it survives
	planted := txlTxHash(txlBlocks+1, 0)
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[8:], afterFirst-1)
	require.NoError(t, tx.Put(kv.TxLookup, planted[:], val))
	s.ForwardProgress++
	require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))
	got, err := tx.GetOne(kv.TxLookup, planted[:])
	require.NoError(t, err)
	require.NotNil(t, got, "rescanned though the bound had not moved")
	require.Equal(t, afterFirst, progress().TxTo, "short-circuit moved the recorded bound")

	// 3. an interrupted rotation is resumed, not restarted. The table is hash-ordered,
	// so the probe has to sort before the saved cursor to be skipped by a resume.
	cur := txlTxHash(txlBlocks/2, 0)
	var before common.Hash
	for i := uint64(0); ; i++ {
		if h := txlTxHash(txlBlocks+2, i); bytes.Compare(h[:], cur[:]) < 0 {
			before = h
			break
		}
	}
	require.NoError(t, tx.Put(kv.TxLookup, before[:], val))
	require.NoError(t, state.SavePruneValProgress(tx, kv.TxLookup, &prune.Stat{
		TxFrom: 0, TxTo: afterFirst, ValueProgress: prune.InProgress, LastPrunedValue: cur[:],
	}))
	require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))
	require.Equal(t, prune.Done, progress().ValueProgress)
	require.NotNil(t, mustGet(t, tx, before[:]), "restarted from First instead of resuming")

	// 4. a pre-fix record bypasses the short-circuit and restarts
	require.NoError(t, state.SavePruneValProgress(tx, kv.TxLookup, &prune.Stat{
		TxFrom: 42, TxTo: afterFirst + 1_000_000, ValueProgress: prune.Done, LastPrunedValue: cur[:],
	}))
	require.NoError(t, PruneTxLookup(s, tx, cfg, ctx, logger))
	require.Nil(t, mustGet(t, tx, before[:]), "stale record still short-circuited")
	require.Zero(t, progress().TxFrom, "stale floor was not cleared")
}

func mustGet(t *testing.T, tx kv.Tx, k []byte) []byte {
	t.Helper()
	v, err := tx.GetOne(kv.TxLookup, k)
	require.NoError(t, err)
	return v
}
