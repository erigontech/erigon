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

package prune_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	mdbx2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/prune"
)

const testTxLookupTable = "TestTxLookup"
const testDupSortTable = "TestDupSort"

func openTestDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	return mdbx2.New(dbcfg.ChainDB, log.New()).
		InMem(tb, tb.TempDir()).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
			return kv.TableCfg{testTxLookupTable: {}}
		}).MustOpen()
}

func openTestDupSortDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	return mdbx2.New(dbcfg.ChainDB, log.New()).
		InMem(tb, tb.TempDir()).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
			return kv.TableCfg{testDupSortTable: {Flags: kv.DupSort}}
		}).MustOpen()
}

// makeTxHash builds a 32-byte key from index i. Keys are ordered by i in the
// B-tree so we can reason deterministically about cursor positions.
func makeTxHash(i uint64) []byte {
	k := make([]byte, 32)
	binary.BigEndian.PutUint64(k[24:], i)
	return k
}

// makeTxLookupValue encodes blockNum||txNum as TxLookup does.
func makeTxLookupValue(blockNum, txNum uint64) []byte {
	v := make([]byte, 16)
	binary.BigEndian.PutUint64(v[0:8], blockNum)
	binary.BigEndian.PutUint64(v[8:16], txNum)
	return v
}

func insertEntries(tb testing.TB, tx kv.RwTx, count int, txNumBase uint64) {
	tb.Helper()
	for i := uint64(0); i < uint64(count); i++ {
		require.NoError(tb, tx.Put(testTxLookupTable, makeTxHash(i), makeTxLookupValue(i, txNumBase+i)))
	}
}

func countTable(tb testing.TB, tx kv.Tx) int {
	tb.Helper()
	c, err := tx.Cursor(testTxLookupTable)
	require.NoError(tb, err)
	defer c.Close()
	n := 0
	for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
		require.NoError(tb, err)
		n++
	}
	return n
}

func openPseudoCursor(tb testing.TB, tx kv.RwTx) kv.PseudoDupSortRwCursor {
	tb.Helper()
	raw, err := tx.RwCursor(testTxLookupTable) //nolint:gocritic // caller owns the returned cursor and calls Close() on it
	require.NoError(tb, err)
	c, ok := raw.(*mdbx2.MdbxCursor)
	require.True(tb, ok)
	return &mdbx2.MdbxCursorPseudoDupSort{MdbxCursor: c}
}

// TestTableScanningPrune_Basic: single pass deletes exactly txNums in [txFrom, txTo).
func TestTableScanningPrune_Basic(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	insertEntries(t, tx, 20, 0) // txNums 0..19

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	cur := openPseudoCursor(t, tx)
	defer cur.Close()

	stat, err := prune.TableScanningPrune(
		t.Context(), "test", "txlookup",
		5, 15, 0, 1, logEvery, log.New(),
		nil, cur, false, &prune.Stat{}, prune.ValueOffset8StorageMode,
	)
	require.NoError(t, err)
	require.Equal(t, prune.Done, stat.ValueProgress)
	require.EqualValues(t, 10, stat.PruneCountValues)
	require.Equal(t, 10, countTable(t, tx))
}

// TestTableScanningPrune_RollingCursor verifies that the rolling-cursor
// optimisation works correctly:
//
//  1. A scan interrupted mid-table saves LastPrunedValue (cursor position).
//  2. Resuming with an ADVANCED txTo (TxFrom/TxTo pre-set to match) continues
//     from the saved position rather than restarting from First().  Entries in
//     the new prune range that lie BEFORE the cursor are intentionally skipped
//     this rotation — they will be caught in the next rotation.
//  3. After the rotation completes (ValueProgress==Done) the caller resets to
//     First and a new rotation picks up the previously-skipped entries.
func TestTableScanningPrune_RollingCursor(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	// 10 entries: makeTxHash(0)..makeTxHash(9) → txNums 0..9.
	// Keys are ordered by index (big-endian suffix), so the B-tree order
	// matches i=0,1,...,9.
	insertEntries(t, tx, 10, 0)

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	// ── Pass 1: simulate a mid-rotation interrupt by injecting a prevStat
	// with LastPrunedValue pointing at makeTxHash(5).  The scan should
	// resume from hash(5) and only visit/delete entries at indices 5..9.
	midCursorPos := makeTxHash(5) // cursor will seek here
	prevStat := &prune.Stat{
		ValueProgress:   prune.InProgress,
		LastPrunedValue: midCursorPos,
		TxFrom:          0,
		TxTo:            8, // prune txNums [0,8)
	}

	cur := openPseudoCursor(t, tx)
	stat1, err := prune.TableScanningPrune(
		t.Context(), "test", "txlookup",
		0, 8, 0, 1, logEvery, log.New(),
		nil, cur, false, prevStat, prune.ValueOffset8StorageMode,
	)
	cur.Close()
	require.NoError(t, err)
	require.Equal(t, prune.Done, stat1.ValueProgress, "pass1 must finish the rotation")
	// Only entries at hashes 5,6,7 match txNum in [0,8) AND are past cursor.
	// Hash 8,9 have txNum 8,9 which are >= txTo=8, so not deleted.
	require.EqualValues(t, 3, stat1.PruneCountValues, "hashes 5,6,7 deleted (txNums 5,6,7 < 8)")
	require.Equal(t, 7, countTable(t, tx), "hashes 0-4 and 8-9 remain")

	// ── Pass 2 (new rotation): caller resets to First because previous Done.
	// txTo advances to 10.  A full scan from First should now delete:
	//   - hashes 0..4: txNums 0..4 < 10, were before the cursor in pass 1
	//   - hash 8,9:    txNums 8,9 < 10, were past txTo=8 in pass 1
	newRotStat := &prune.Stat{
		ValueProgress: prune.First,
		TxFrom:        0,
		TxTo:          10,
	}
	cur = openPseudoCursor(t, tx)
	stat2, err := prune.TableScanningPrune(
		t.Context(), "test", "txlookup",
		0, 10, 0, 1, logEvery, log.New(),
		nil, cur, false, newRotStat, prune.ValueOffset8StorageMode,
	)
	cur.Close()
	require.NoError(t, err)
	require.Equal(t, prune.Done, stat2.ValueProgress)
	require.EqualValues(t, 7, stat2.PruneCountValues, "remaining 7 entries deleted")
	require.Equal(t, 0, countTable(t, tx), "table empty")
}

// TestTableScanningPrune_CtxCancelOnOutOfRange verifies that context
// cancellation is respected while scanning out-of-range entries (txNum >= txTo).
//
// Regression test: PR #19898 introduced an early-skip `continue` for
// out-of-range entries that bypassed ctx.Done() checks, causing the loop
// to scan the entire remaining table without honouring timeouts.
func TestTableScanningPrune_CtxCancelOnOutOfRange(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	// 20 entries with txNums 100..119 — ALL out of range for txTo=5.
	// The scan visits every entry, each hitting "txNum >= txTo → continue".
	insertEntries(t, tx, 20, 100)

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	// Pre-cancel the context so ctx.Done() is immediately readable.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	cur := openPseudoCursor(t, tx)
	defer cur.Close()

	stat, err := prune.TableScanningPrune(
		ctx, "test", "txlookup",
		0, 5, 0, 1, logEvery, log.New(),
		nil, cur, false, &prune.Stat{}, prune.ValueOffset8StorageMode,
	)
	require.NoError(t, err)

	// With the fix: the first out-of-range entry checks ctx.Done(),
	// sees cancellation, returns InProgress immediately.
	// Without the fix: the `continue` skips ctx.Done(), the loop
	// scans all 20 entries, returns Done with 0 deletions.
	require.Equal(t, prune.InProgress, stat.ValueProgress,
		"scan must be interrupted by context cancellation on out-of-range entries")
	require.NotNil(t, stat.LastPrunedValue,
		"interrupted scan must save cursor position for resume")
	require.EqualValues(t, 0, stat.PruneCountValues,
		"no entries should be deleted — all are out of range")
}

// --- DupSort domain prune tests (mirrors commitmentvals: StepValueStorageMode) ---

// encodeStepVal builds the value bytes a domain writes for a (step,value) pair:
//
//	value = ^step (BE u64) || actualValue
//
// This is the layout for non-largeValues domains like commitmentvals (see
// domain.go:addValue and StepValueStorageMode decoder).
func encodeStepVal(step uint64, val []byte) []byte {
	buf := make([]byte, 8+len(val))
	binary.BigEndian.PutUint64(buf[:8], ^step)
	copy(buf[8:], val)
	return buf
}

// userKey returns a 20-byte address-shaped key for test determinism.
func userKey(i uint64) []byte {
	k := make([]byte, 20)
	binary.BigEndian.PutUint64(k[12:], i)
	return k
}

func openDupSortPseudoCursor(tb testing.TB, tx kv.RwTx) kv.PseudoDupSortRwCursor {
	tb.Helper()
	raw, err := tx.RwCursorDupSort(testDupSortTable) //nolint:gocritic // caller owns the returned cursor and calls Close() on it
	require.NoError(tb, err)
	c, ok := raw.(kv.PseudoDupSortRwCursor)
	require.True(tb, ok)
	return c
}

func countDupSortTable(tb testing.TB, tx kv.Tx) (keys, dups int) {
	tb.Helper()
	c, err := tx.Cursor(testDupSortTable)
	require.NoError(tb, err)
	defer c.Close()
	for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
		require.NoError(tb, err)
		dups++
	}
	c2, err := tx.CursorDupSort(testDupSortTable)
	require.NoError(tb, err)
	defer c2.Close()
	for k, _, err := c2.First(); k != nil; k, _, err = c2.NextNoDup() {
		require.NoError(tb, err)
		keys++
	}
	return keys, dups
}

// TestDupSortPrune_SingleDupAllInRange: every user-key has exactly one dup,
// every step is < txTo. After one scan, table must be empty.
// This is the simplest case: no cursor-state shenanigans from AllDups delete.
func TestDupSortPrune_SingleDupAllInRange(t *testing.T) {
	db := openTestDupSortDB(t)
	defer db.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	const N = 50
	const stepSize uint64 = 16
	// 50 user keys; each gets a single dup at step=1 (txNum in [16,32)).
	for i := uint64(0); i < N; i++ {
		require.NoError(t, tx.Put(testDupSortTable, userKey(i), encodeStepVal(1, []byte{byte(i)})))
	}

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	cur := openDupSortPseudoCursor(t, tx)
	defer cur.Close()

	// txTo = 64 → prune steps 0,1,2,3.
	stat, err := prune.TableScanningPrune(
		t.Context(), "test", "dup",
		0, 64, 0, stepSize, logEvery, log.New(),
		nil, cur, false, &prune.Stat{}, prune.StepValueStorageMode,
	)
	require.NoError(t, err)
	require.Equal(t, prune.Done, stat.ValueProgress)
	require.EqualValues(t, N, stat.PruneCountValues, "all N entries deleted")

	keys, dups := countDupSortTable(t, tx)
	require.Equal(t, 0, keys, "no keys remain")
	require.Equal(t, 0, dups, "no dups remain")
}

// TestDupSortPrune_MultipleDupsAllInRange: every user-key has 3 dups at
// different steps, ALL in range. Exercises the bulk-AllDups delete path
// followed by NextNoDup — the suspected production bug site.
func TestDupSortPrune_MultipleDupsAllInRange(t *testing.T) {
	db := openTestDupSortDB(t)
	defer db.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	const N = 50
	const stepSize uint64 = 16
	// Three dups per key, at steps 1,2,3 (txNums 16,32,48 — all < txTo=64).
	for i := uint64(0); i < N; i++ {
		k := userKey(i)
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(1, []byte{0xa})))
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(2, []byte{0xb})))
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(3, []byte{0xc})))
	}

	keysBefore, dupsBefore := countDupSortTable(t, tx)
	require.Equal(t, N, keysBefore)
	require.Equal(t, 3*N, dupsBefore)

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	cur := openDupSortPseudoCursor(t, tx)
	defer cur.Close()

	stat, err := prune.TableScanningPrune(
		t.Context(), "test", "dup",
		0, 64, 0, stepSize, logEvery, log.New(),
		nil, cur, false, &prune.Stat{}, prune.StepValueStorageMode,
	)
	require.NoError(t, err)
	require.Equal(t, prune.Done, stat.ValueProgress)
	require.EqualValues(t, 3*N, stat.PruneCountValues, "all 3N dups deleted")

	keys, dups := countDupSortTable(t, tx)
	require.Equal(t, 0, keys, "no keys remain after scan claimed Done")
	require.Equal(t, 0, dups, "no dups remain after scan claimed Done")
}

// TestDupSortPrune_MixedDupsPartialRange: each user-key has dups at multiple
// steps, some in range and some beyond. Exercises the selective-deletion path
// (line ~378 in prune.go).
func TestDupSortPrune_MixedDupsPartialRange(t *testing.T) {
	db := openTestDupSortDB(t)
	defer db.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	const N = 30
	const stepSize uint64 = 16
	// Two dups per key:
	//   - step 1 (txNum 16) — in range
	//   - step 5 (txNum 80) — out of range when txTo=64
	for i := uint64(0); i < N; i++ {
		k := userKey(i)
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(1, []byte{0xa})))
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(5, []byte{0xe})))
	}

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	cur := openDupSortPseudoCursor(t, tx)
	defer cur.Close()

	stat, err := prune.TableScanningPrune(
		t.Context(), "test", "dup",
		0, 64, 0, stepSize, logEvery, log.New(),
		nil, cur, false, &prune.Stat{}, prune.StepValueStorageMode,
	)
	require.NoError(t, err)
	require.Equal(t, prune.Done, stat.ValueProgress)
	require.EqualValues(t, N, stat.PruneCountValues, "exactly N step-1 dups deleted")

	keys, dups := countDupSortTable(t, tx)
	require.Equal(t, N, keys, "all keys still present (step-5 dup remains)")
	require.Equal(t, N, dups, "exactly N dups remain (step-5)")
}

// TestDupSortPrune_ProductionLike mirrors the commitmentvals residue pattern.
// Build a table where some keys are only present in OLD steps, others have
// their newest write in the ACTIVE (out-of-range) step. Prune with txTo at the
// active-step boundary. Every dup whose txNum < txTo must be gone.
func TestDupSortPrune_ProductionLike(t *testing.T) {
	db := openTestDupSortDB(t)
	defer db.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	const stepSize uint64 = 16
	const activeStep uint64 = 10 // tip
	// txTo = start of activeStep — everything in steps [0, activeStep) prunable.
	txTo := activeStep * stepSize

	// Group A: 20 keys with dups only at old step 2 — should be fully deleted.
	for i := uint64(0); i < 20; i++ {
		k := userKey(i)
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(2, []byte{0x1})))
	}
	// Group B: 20 keys (offset by 100) with dups at steps 2 AND activeStep (out of range).
	// Step-2 dups must still be deleted; step-activeStep dups must remain.
	for i := uint64(100); i < 120; i++ {
		k := userKey(i)
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(2, []byte{0x1})))
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(activeStep, []byte{0xa})))
	}
	// Group C: 20 keys (offset 200) with dups across many old steps (5,6,7,8) AND activeStep.
	// All old-step dups must be deleted; activeStep dup must remain.
	for i := uint64(200); i < 220; i++ {
		k := userKey(i)
		for s := uint64(5); s <= 8; s++ {
			require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(s, []byte{byte(s)})))
		}
		require.NoError(t, tx.Put(testDupSortTable, k, encodeStepVal(activeStep, []byte{0xa})))
	}

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	cur := openDupSortPseudoCursor(t, tx)
	defer cur.Close()

	_, err = prune.TableScanningPrune(
		t.Context(), "test", "dup",
		0, txTo, 0, stepSize, logEvery, log.New(),
		nil, cur, false, &prune.Stat{}, prune.StepValueStorageMode,
	)
	require.NoError(t, err)

	// Verify table state: walk each remaining row and check its step.
	c, err := tx.CursorDupSort(testDupSortTable)
	require.NoError(t, err)
	defer c.Close()
	type remaining struct{ keyIdx, step uint64 }
	got := make([]remaining, 0)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		require.NoError(t, err)
		idx := binary.BigEndian.Uint64(k[12:])
		step := ^binary.BigEndian.Uint64(v[:8])
		got = append(got, remaining{idx, step})
	}
	// Allowed survivors: Group B (idx 100..119) and Group C (idx 200..219) at activeStep only.
	for _, r := range got {
		if r.step*stepSize >= txTo {
			continue // out of range → allowed
		}
		t.Errorf("BUG: in-range row survived: key=%d step=%d (txTo=%d)", r.keyIdx, r.step, txTo)
	}
	t.Logf("survivors=%d", len(got))
}

func BenchmarkTableScanningPrune(b *testing.B) {
	db := openTestDB(b)
	defer db.Close()

	const N = 10_000
	tx, err := db.BeginRw(b.Context())
	require.NoError(b, err)
	defer tx.Rollback()
	insertEntries(b, tx, N, 0) // txNums 0..N-1; prune [0, N/2)

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()
	logger := log.New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur := openPseudoCursor(b, tx)
		prune.TableScanningPrune( //nolint:errcheck
			b.Context(), "bench", "txlookup",
			0, N/2, 0, 1, logEvery, logger,
			nil, cur, false, &prune.Stat{}, prune.ValueOffset8StorageMode,
		)
		cur.Close()
	}
}
