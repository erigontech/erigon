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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	mdbx2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

const (
	txlBlocks     = uint64(3_000)
	txlTxPerBlock = uint64(3)
	txlFrozen     = uint64(1_500) // blocks in snapshots
)

// txlBlockReader serves the two methods PruneTxLookup uses; anything else is a
// nil-panic, which is the point.
type txlBlockReader struct {
	dbservices.FullBlockReader
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

func txLookupFixture(t *testing.T, firstHeader, pruneProgress, senders, staleFloor, resumeFrom uint64) (kv.TemporalRwTx, TxLookupCfg, *PruneState) {
	t.Helper()
	dir := t.TempDir()
	db := temporaltest.NewTestDB(t, datadir.New(dir))
	tx, err := db.BeginTemporalRw(context.Background())
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
	// Both describe the single kv.TxLookup prune record, so at most one may be set.
	require.False(t, resumeFrom > 0 && staleFloor > 0, "resumeFrom and staleFloor write the same record")
	switch {
	case resumeFrom > 0:
		// an interrupted rotation under the current floor: must be resumed, not restarted
		h := txlTxHash(resumeFrom, 0)
		require.NoError(t, state.SavePruneValProgress(tx, kv.TxLookup, &prune.Stat{
			TxFrom: 0, TxTo: 1, ValueProgress: prune.InProgress, LastPrunedValue: h[:],
		}))
	case staleFloor > 0:
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
		name                                            string
		firstHeader, pruneProgress, senders, staleFloor uint64
	}{
		{name: "headers_from_genesis", firstHeader: 1},
		{name: "headers_pruned_to_frontier", firstHeader: txlBlockTo()},
		{name: "headers_pruned_past_frontier", firstHeader: txlBlocks - 1},
		{name: "forward_stage_watermark", firstHeader: 1, pruneProgress: txlFrozen},
		{name: "senders_fallback", firstHeader: txlBlocks + 1, senders: 500},
		{name: "stale_rotation", firstHeader: 1, staleFloor: txlMinTxNum(txlBlocks / 2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, cfg, s := txLookupFixture(t, tc.firstHeader, tc.pruneProgress, tc.senders, tc.staleFloor, 0)
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

// ---------------------------------------------------------------------------
// A/B: hash-ordered table scan (PruneTxLookup) vs the pre-#19179 block walk
// (pruneTxLookupByBlocks). Both are driven through PruneTxLookup so the switch,
// the bounds and the budget are the production ones.
// ---------------------------------------------------------------------------

// benchTxn is deterministic per (block, i) and its Hash() is a real txn hash, so
// the block walk can rediscover the same keys the fixture wrote.
func benchTxn(block, i uint64) types.Transaction {
	var to common.Address
	binary.BigEndian.PutUint64(to[:8], block)
	return &types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: i, GasLimit: 21_000, To: &to, Value: *uint256.NewInt(block)},
		GasPrice: *uint256.NewInt(1),
	}
}

type txlBenchReader struct {
	dbservices.FullBlockReader
	frozen      *uint64
	txPerBlock  uint64
	bodyMissing func(blockNum uint64) bool
}

func (r txlBenchReader) CanPruneTo(cur uint64) uint64 {
	return freezeblocks.CanDeleteTo(cur, *r.frozen)
}
func (r txlBenchReader) FrozenBlocks() uint64 { return *r.frozen }
func (r txlBenchReader) TxnumReader() rawdbv3.TxNumsReader {
	return freezeblocks.NewBlockReader(nil, nil).TxnumReader()
}
func (r txlBenchReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Body, error) {
	if r.bodyMissing != nil && r.bodyMissing(blockNum) {
		return nil, nil
	}
	txs := make([]types.Transaction, r.txPerBlock)
	for i := range txs {
		txs[i] = benchTxn(blockNum, uint64(i))
	}
	return &types.Body{Transactions: txs}, nil
}

type txlBenchCfg struct {
	blocks, txPerBlock, frozen uint64
}

func (c txlBenchCfg) minTxNum(block uint64) uint64 {
	if block == 0 {
		return 0
	}
	return 2 + (block-1)*(c.txPerBlock+2)
}
func (c txlBenchCfg) maxTxNum(block uint64) uint64 {
	if block == 0 {
		return 1
	}
	return c.minTxNum(block) + c.txPerBlock + 1
}

func txlBenchFixture(tb testing.TB, c txlBenchCfg) (kv.TemporalRwDB, TxLookupCfg, *uint64) {
	tb.Helper()
	dir := tb.TempDir()
	db := temporaltest.NewTestDB(tb, datadir.New(dir))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(tb, err)
	defer tx.Rollback()

	txNums := freezeblocks.NewBlockReader(nil, nil).TxnumReader()
	for b := uint64(0); b <= c.blocks; b++ {
		require.NoError(tb, txNums.Append(tx, b, c.maxTxNum(b)))
	}
	val := make([]byte, 16)
	for b := uint64(1); b <= c.blocks; b++ {
		var hh common.Hash
		binary.BigEndian.PutUint64(hh[:], b)
		require.NoError(tb, tx.Put(kv.HeaderCanonical, hexutil.EncodeTs(b), hh[:]))
		binary.BigEndian.PutUint64(val[:8], b)
		for i := range c.txPerBlock {
			binary.BigEndian.PutUint64(val[8:], c.minTxNum(b)+i+1)
			h := benchTxn(b, i).Hash()
			require.NoError(tb, tx.Put(kv.TxLookup, h[:], val))
		}
	}
	require.NoError(tb, tx.Commit())

	frozen := new(uint64)
	*frozen = c.frozen
	cfg := StageTxLookupCfg(prune.Mode{Initialised: true, History: prune.Distance(config3.DefaultPruneDistance)},
		dir, txlBenchReader{frozen: frozen, txPerBlock: c.txPerBlock})
	return db, cfg, frozen
}

// txlPageOps reports MDBX write-amplification counters — the quantity #23199 is
// about, and the one wall-clock hides when the whole table is in page cache.
func txlPageOps(tb testing.TB, db kv.TemporalRwDB) (cow, split, wops uint64) {
	tb.Helper()
	inner := db.(interface{ InternalDB() kv.RwDB }).InternalDB()
	info, err := inner.(*mdbx2.MdbxKV).Env().Info(nil)
	require.NoError(tb, err)
	return info.PageOps.Cow, info.PageOps.Split, info.PageOps.Wops
}

func txlPruneOnce(tb testing.TB, db kv.TemporalRwDB, cfg TxLookupCfg, forward uint64, byBlocks bool) (pruned, left uint64) {
	tb.Helper()
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(tb, err)
	defer tx.Rollback()

	before, err := tx.Count(kv.TxLookup)
	require.NoError(tb, err)

	txLookupPruneByBlocks = byBlocks
	s := &PruneState{ID: stages.TxLookup, ForwardProgress: forward,
		CurrentSyncCycle: CurrentSyncCycleInfo{IsInitialCycle: true}}
	pp, err := stages.GetStagePruneProgress(tx, stages.TxLookup)
	require.NoError(tb, err)
	s.PruneProgress = pp
	require.NoError(tb, PruneTxLookup(s, tx, cfg, context.Background(), log.New()))
	txLookupPruneByBlocks = false

	left, err = tx.Count(kv.TxLookup)
	require.NoError(tb, err)
	require.NoError(tb, tx.Commit())
	return before - left, left
}

func txlBenchSizes() txlBenchCfg {
	return txlBenchCfg{
		blocks:     uint64(dbg.EnvInt("TXL_BENCH_BLOCKS", 3_000)),
		txPerBlock: uint64(dbg.EnvInt("TXL_BENCH_TXS", 150)),
		frozen:     uint64(dbg.EnvInt("TXL_BENCH_FROZEN", 1_000)),
	}
}

func txlBenchDelta() uint64 { return uint64(dbg.EnvInt("TXL_BENCH_DELTA", 1_000)) }

// One rotation over a table that has never been pruned: the backlog case an
// upgraded node meets once.
func BenchmarkPruneTxLookupBacklog(b *testing.B) {
	c := txlBenchSizes()
	for _, byBlocks := range []bool{false, true} {
		b.Run(map[bool]string{false: "scan", true: "byblocks"}[byBlocks], func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				db, cfg, _ := txlBenchFixture(b, c)
				cow0, split0, wops0 := txlPageOps(b, db)
				b.StartTimer()

				pruned, left := txlPruneOnce(b, db, cfg, c.blocks, byBlocks)

				b.StopTimer()
				cow1, split1, wops1 := txlPageOps(b, db)
				b.ReportMetric(float64(pruned), "rows_pruned")
				b.ReportMetric(float64(left), "rows_left")
				b.ReportMetric(float64(cow1-cow0), "cow_pages")
				b.ReportMetric(float64(split1-split0), "splits")
				b.ReportMetric(float64(wops1-wops0), "wops")
				db.Close()
				b.StartTimer()
			}
		})
	}
}

// The steady chain-tip cycle: the table is already pruned to the frontier, then
// the frontier advances by TXL_BENCH_DELTA blocks. The scan rewalks the whole
// remaining table to collect that delta; the block walk visits only the delta.
func BenchmarkPruneTxLookupTip(b *testing.B) {
	c, delta := txlBenchSizes(), txlBenchDelta()
	for _, byBlocks := range []bool{false, true} {
		b.Run(map[bool]string{false: "scan", true: "byblocks"}[byBlocks], func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				db, cfg, frozen := txlBenchFixture(b, c)
				txlPruneOnce(b, db, cfg, c.blocks, byBlocks)
				*frozen += delta
				cow0, split0, wops0 := txlPageOps(b, db)
				b.StartTimer()

				pruned, left := txlPruneOnce(b, db, cfg, c.blocks+delta, byBlocks)

				b.StopTimer()
				cow1, split1, wops1 := txlPageOps(b, db)
				b.ReportMetric(float64(pruned), "rows_pruned")
				b.ReportMetric(float64(left), "rows_left")
				b.ReportMetric(float64(cow1-cow0), "cow_pages")
				b.ReportMetric(float64(split1-split0), "splits")
				b.ReportMetric(float64(wops1-wops0), "wops")
				db.Close()
				b.StartTimer()
			}
		})
	}
}

// Both implementations must leave the same table.
func TestPruneTxLookupImplsAgree(t *testing.T) {
	c := txlBenchCfg{blocks: 4_000, txPerBlock: 3, frozen: 2_000}
	survivors := func(byBlocks bool) map[string]struct{} {
		db, cfg, _ := txlBenchFixture(t, c)
		defer db.Close()
		txlPruneOnce(t, db, cfg, c.blocks, byBlocks)
		tx, err := db.BeginTemporalRo(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		out := map[string]struct{}{}
		require.NoError(t, tx.ForEach(kv.TxLookup, nil, func(k, _ []byte) error {
			out[string(k)] = struct{}{}
			return nil
		}))
		return out
	}
	scan, byBlocks := survivors(false), survivors(true)
	require.NotEmpty(t, scan)
	require.Equal(t, scan, byBlocks)
}

// A block walk can only delete keys it can rediscover, so anything it cannot
// read is retained while the watermark moves past it. The scan does not read
// blocks at all.
func TestPruneTxLookupRetainsWhatItCannotRead(t *testing.T) {
	c := txlBenchCfg{blocks: 4_000, txPerBlock: 3, frozen: 2_000}
	const blind = 500 // blocks whose body/header the walk cannot see

	for _, tc := range []struct {
		name string
		hide func(tb testing.TB, db kv.TemporalRwDB, cfg *TxLookupCfg)
	}{
		{name: "body_gone", hide: func(tb testing.TB, _ kv.TemporalRwDB, cfg *TxLookupCfg) {
			r := cfg.blockReader.(txlBenchReader)
			r.bodyMissing = func(n uint64) bool { return n < blind }
			cfg.blockReader = r
		}},
		{name: "canonical_marker_gone", hide: func(tb testing.TB, db kv.TemporalRwDB, _ *TxLookupCfg) {
			tx, err := db.BeginTemporalRw(context.Background())
			require.NoError(tb, err)
			defer tx.Rollback()
			for b := uint64(1); b < blind; b++ {
				require.NoError(tb, tx.Delete(kv.HeaderCanonical, hexutil.EncodeTs(b)))
			}
			require.NoError(tb, tx.Commit())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, byBlocks := range []bool{false, true} {
				db, cfg, _ := txlBenchFixture(t, c)
				tc.hide(t, db, &cfg)
				txlPruneOnce(t, db, cfg, c.blocks, byBlocks)

				tx, err := db.BeginTemporalRo(context.Background())
				require.NoError(t, err)
				left := 0
				for b := uint64(1); b < blind; b++ {
					for i := range c.txPerBlock {
						h := benchTxn(b, i).Hash()
						v, err := tx.GetOne(kv.TxLookup, h[:])
						require.NoError(t, err)
						if v != nil {
							left++
						}
					}
				}
				tx.Rollback()
				db.Close()
				if byBlocks {
					require.Equal(t, int(blind-1)*int(c.txPerBlock), left, "block walk deleted rows it could not read")
				} else {
					require.Zero(t, left, "scan left rows behind")
				}
			}
		})
	}
}

// Rows whose block is not canonical any more — what an unwind leaves when the
// canonical marker is gone before UnwindTxLookup runs. The walk never visits
// such a key; the scan tests every key it passes.
func TestPruneTxLookupOrphanRows(t *testing.T) {
	c := txlBenchCfg{blocks: 4_000, txPerBlock: 3, frozen: 2_000}
	orphan := benchTxn(1_000_000, 0).Hash() // no canonical header carries it
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[:8], 1)
	binary.BigEndian.PutUint64(val[8:], c.minTxNum(1)+1) // well below the bound

	for _, byBlocks := range []bool{false, true} {
		db, cfg, _ := txlBenchFixture(t, c)
		tx, err := db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		require.NoError(t, tx.Put(kv.TxLookup, orphan[:], val))
		require.NoError(t, tx.Commit())

		txlPruneOnce(t, db, cfg, c.blocks, byBlocks)

		ro, err := db.BeginTemporalRo(context.Background())
		require.NoError(t, err)
		got, err := ro.GetOne(kv.TxLookup, orphan[:])
		require.NoError(t, err)
		ro.Rollback()
		db.Close()
		if byBlocks {
			require.NotNil(t, got, "block walk removed an orphan it cannot see")
		} else {
			require.Nil(t, got, "scan left an orphan below the bound")
		}
	}
}

// The block walk resumes from the stage's prune watermark. Older binaries wrote
// FrozenBlocks() into that same key from the forward stage, so an upgraded node
// starts above its own backlog and never comes back down for it. The scan has
// no watermark to poison.
func TestPruneTxLookupPoisonedWatermark(t *testing.T) {
	c := txlBenchCfg{blocks: 4_000, txPerBlock: 3, frozen: 2_000}
	for _, byBlocks := range []bool{false, true} {
		db, cfg, _ := txlBenchFixture(t, c)
		tx, err := db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		require.NoError(t, stages.SaveStagePruneProgress(tx, stages.TxLookup, c.frozen))
		require.NoError(t, tx.Commit())

		txlPruneOnce(t, db, cfg, c.blocks, byBlocks)

		ro, err := db.BeginTemporalRo(context.Background())
		require.NoError(t, err)
		backlog := 0
		for b := uint64(1); b < c.frozen; b++ {
			for i := range c.txPerBlock {
				h := benchTxn(b, i).Hash()
				v, err := ro.GetOne(kv.TxLookup, h[:])
				require.NoError(t, err)
				if v != nil {
					backlog++
				}
			}
		}
		ro.Rollback()
		db.Close()
		if byBlocks {
			require.Equal(t, int(c.frozen-1)*int(c.txPerBlock), backlog,
				"block walk cleared a backlog its watermark had skipped")
		} else {
			require.Zero(t, backlog, "scan was blocked by the stale watermark")
		}
	}
}
