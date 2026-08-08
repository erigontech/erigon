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

package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/seg"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
)

// v4EmitFixture is a controlled aggregator + temporal DB with a small
// step size and a deterministic write plan the test can query.
type v4EmitFixture struct {
	t        *testing.T
	dirs     datadir.Dirs
	db       kv.TemporalRwDB
	agg      *dbstate.Aggregator
	stepSize uint64
}

// setupV4EmitFixture builds a temporal DB + aggregator with the given
// step size. Caller owns the returned fixture; teardown is handled by
// t.Cleanup registered on the underlying db + agg.
func setupV4EmitFixture(t *testing.T, stepSize uint64) *v4EmitFixture {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)

	agg := dbstate.NewTest(dirs).StepSize(stepSize).Logger(logger).MustOpen(t.Context(), db)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder())

	tdb, err := temporal.New(db, agg, nil)
	require.NoError(t, err)
	t.Cleanup(tdb.Close)

	return &v4EmitFixture{t: t, dirs: dirs, db: tdb, agg: agg, stepSize: stepSize}
}

// v4Write applies (key, value) at the given txN via SharedDomains.
// del=true issues DomainDel instead of DomainPut (accounts-domain
// tombstone semantics — SELFDESTRUCT / zero-out).
type v4Write struct {
	txN uint64
	key []byte
	val []byte
	del bool
}

// applyWrites executes the plan and commits the tx.
func (f *v4EmitFixture) applyWrites(writes []v4Write) {
	f.t.Helper()
	rwTx, err := f.db.BeginTemporalRw(f.t.Context())
	require.NoError(f.t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(f.t.Context(), rwTx, log.New())
	require.NoError(f.t, err)
	defer domains.Close()

	for _, w := range writes {
		prev, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, w.key)
		require.NoError(f.t, err)
		if w.del {
			require.NoError(f.t, domains.DomainDel(kv.AccountsDomain, rwTx, w.key, w.txN, prev))
		} else {
			require.NoError(f.t, domains.DomainPut(kv.AccountsDomain, rwTx, w.key, w.val, w.txN, prev))
		}
	}
	require.NoError(f.t, domains.Flush(f.t.Context(), rwTx))
	require.NoError(f.t, rwTx.Commit())
}

// buildFilesUpTo retires + merges MDBX history into files up to txN.
func (f *v4EmitFixture) buildFilesUpTo(txN uint64) {
	f.t.Helper()
	require.NoError(f.t, f.agg.BuildFiles(txN))
}

// TestV4EmitCoversInWindowTouches drives the production wiring of
// WriteStateBoundaryFileV4 with historyKeyWalker + tx.GetAsOf against
// a REAL aggregator + temporal DB, at the isolation level a mode-B
// unwind would exercise. Regression test for the failure class in
// memory/mode-c-v4-emit-nondeterministic-2026-08-06.md — an address
// touched in the (fromTxN, targetTxN] window must appear in the
// emitted v4 .kv with its as-of-target value.
//
// Setup: stepSize=16. Writes span steps 0-1. Target = txN 24 (mid-
// step 1). Straddler window = (16, 25]. The test asserts the walker
// yields every in-window key AND the emitted file has each key with
// its as-of-24 value.
func TestV4EmitCoversInWindowTouches(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const stepSize uint64 = 16
	f := setupV4EmitFixture(t, stepSize)

	// Address keys are 20-byte account addresses.
	addrA := bytes.Repeat([]byte{0xAA}, 20)
	addrB := bytes.Repeat([]byte{0xBB}, 20)
	addrC := bytes.Repeat([]byte{0xCC}, 20)
	addrD := bytes.Repeat([]byte{0xDD}, 20)

	// Fake serialized accounts — this test is about the emit path,
	// not account decoding, so opaque bytes are fine. The key
	// property is that reading back the emit yields the SAME bytes.
	valA5 := []byte("A-at-5")
	valA20 := []byte("A-at-20")
	valB10 := []byte("B-at-10")
	valC22 := []byte("C-first-at-22")
	valD30 := []byte("D-at-30-past-target")

	writes := []v4Write{
		// step 0 (txN 0-15)
		{txN: 5, key: addrA, val: valA5},
		{txN: 10, key: addrB, val: valB10},
		// step 1 (txN 16-31) — IN the walk window (16, 25]
		{txN: 20, key: addrA, val: valA20}, // update
		{txN: 22, key: addrC, val: valC22}, // first-write in window
		// past target (txN > 24) — OUTSIDE the walk window
		{txN: 30, key: addrD, val: valD30},
	}
	f.applyWrites(writes)
	// Retire up through step 1's end so both .0-1 and .1-2 exist.
	f.buildFilesUpTo(stepSize * 2)

	// Simulate mode-B unwind at target txN = 24 (mid-step 1).
	targetTxN := uint64(24)
	// Straddler = .1-2 (fromStep=1). Walker range = (16, 25].
	fromTxN := stepSize * 1

	roTx, err := f.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// (1) Walker independently — pin its yield set.
	walker := historyKeyWalker(roTx, kv.AccountsDomain, fromTxN, targetTxN)
	var yielded [][]byte
	require.NoError(t, walker(func(k []byte) bool {
		yielded = append(yielded, append([]byte(nil), k...))
		return true
	}))
	require.ElementsMatch(t,
		[][]byte{addrA, addrC},
		yielded,
		"walker must yield every key touched in (16, 24]: addrA (updated at 20) and addrC (first-write at 22)")

	// (2) End-to-end via WriteStateBoundaryFileV4 with real wiring.
	lookup := func(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
		return roTx.GetAsOf(domain, key, ts+1)
	}
	outPath := filepath.Join(f.dirs.SnapDomain, "v4.0-accounts.16-25.kv")
	require.NoError(t, WriteStateBoundaryFileV4(
		ctx,
		kv.AccountsDomain,
		historyKeyWalker(roTx, kv.AccountsDomain, fromTxN, targetTxN),
		lookup,
		targetTxN,
		outPath,
		f.dirs.Tmp,
		seg.CompressNone,
		&fakeAccessorBuilder{},
		log.New(),
	))

	got := readKV(t, outPath)
	// Sort into a comparable map for stable assertion regardless of walker order.
	gotMap := make(map[string][]byte, len(got))
	for _, kv := range got {
		gotMap[string(kv[0])] = kv[1]
	}
	require.Contains(t, gotMap, string(addrA), "emit must include addrA (touched in-window)")
	require.Equal(t, valA20, gotMap[string(addrA)], "addrA value must be its as-of-target (txN=20) write")
	require.Contains(t, gotMap, string(addrC), "emit must include addrC (touched in-window)")
	require.Equal(t, valC22, gotMap[string(addrC)], "addrC value must be its as-of-target (txN=22) write")
	require.NotContains(t, gotMap, string(addrB), "addrB was NOT touched in-window; emit should not include it")
	require.NotContains(t, gotMap, string(addrD), "addrD was touched AFTER target; emit should not include it")
}

// TestV4EmitPreWindowKeyFallsThroughToBaseline pins the fallthrough
// invariant behind the observed state-restoration failures — an
// address touched only pre-window (never in-window) must still be
// readable at target via GetLatestFromFiles falling through to the
// baseline .kv. The v4 emit doesn't include it (correctly, since
// the walker doesn't yield it), so state reads MUST resolve it via
// the wider baseline.
//
// This test mirrors the specific failure signature of address
// 0x86c38852... in postfix-run1 (2026-08-08): touched only in the
// distant past, absent from every file according to state-lookup-at.
// If the test passes, the bug is not in the file layering; if it
// fails here, we've reproduced the missing-key class in isolation.
func TestV4EmitPreWindowKeyFallsThroughToBaseline(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const stepSize uint64 = 16
	f := setupV4EmitFixture(t, stepSize)

	// addrEarly touched ONLY at txN=5 (step 0). Never appears again.
	// This is the shape state-lookup-at showed for the failing
	// address on the frozen datadir — one funding event long past,
	// no other touches, absent from post-unwind state.
	addrEarly := bytes.Repeat([]byte{0xEE}, 20)
	valEarly := []byte("Early-balance-1000")

	// A distractor address touched in-window so v4 emit produces
	// a non-empty file (matching production shape).
	addrInWindow := bytes.Repeat([]byte{0x11}, 20)
	valInWindow := []byte("Distractor-at-20")

	f.applyWrites([]v4Write{
		{txN: 5, key: addrEarly, val: valEarly},
		{txN: 20, key: addrInWindow, val: valInWindow},
	})
	f.buildFilesUpTo(stepSize * 2)

	// Merge steps 0-1 + 1-2 into a wider baseline (production has
	// .288-304 merged files as the baseline for a mid-step-304
	// unwind target).
	f.agg.MergeLoop(ctx)
	require.NoError(t, f.agg.OpenFolder())

	targetTxN := uint64(24)
	fromTxN := stepSize * 1

	roTx, err := f.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// Walker should yield only addrInWindow — addrEarly was
	// touched pre-window.
	var yielded [][]byte
	walker := historyKeyWalker(roTx, kv.AccountsDomain, fromTxN, targetTxN)
	require.NoError(t, walker(func(k []byte) bool {
		yielded = append(yielded, append([]byte(nil), k...))
		return true
	}))
	require.ElementsMatch(t,
		[][]byte{addrInWindow},
		yielded,
		"walker must yield only in-window touches; addrEarly (touched pre-window) is out of scope")

	// GetAsOf at target for addrEarly MUST resolve to its pre-window
	// value via baseline .kv fallthrough. Failure here reproduces
	// the observed state-restoration bug in isolation.
	val, _, err := roTx.GetAsOf(kv.AccountsDomain, addrEarly, targetTxN+1)
	require.NoError(t, err)
	require.Equal(t, valEarly, val,
		"addrEarly's pre-window write MUST be resolvable at target via baseline .kv fallthrough — this is the invariant the observed failure violates")

	// Same via GetLatestFromFiles capped at target — the exact call
	// the runtime read path uses.
	debugTx := roTx.Debug()
	filesVal, filesFound, fileStart, fileEnd, err := debugTx.GetLatestFromFiles(kv.AccountsDomain, addrEarly, targetTxN+1)
	require.NoError(t, err)
	require.True(t, filesFound, "GetLatestFromFiles capped at target MUST find addrEarly in baseline file")
	require.Equal(t, valEarly, filesVal,
		"GetLatestFromFiles capped at target MUST return addrEarly's pre-window value (file range [%d, %d))", fileStart, fileEnd)
}

// deletePrunedHistoryFiles removes every .v/.ef/.efi/.vi file
// under the datadir's snapshot subdirs, simulating the effect of
// FilterPreverifiedByPruneMode at bootstrap under
// --prune.mode=minimal (history + idx + accessor dirs' history-side
// entries are filtered out; only domain .kv survives).
// Returns the list of removed basenames for the assertion.
func deletePrunedHistoryFiles(t *testing.T, dirs datadir.Dirs) []string {
	t.Helper()
	var removed []string
	for _, d := range []string{dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		entries, err := os.ReadDir(d)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read %s: %v", d, err)
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !(strings.HasSuffix(name, ".v") ||
				strings.HasSuffix(name, ".ef") ||
				strings.HasSuffix(name, ".efi") ||
				strings.HasSuffix(name, ".vi")) {
				continue
			}
			require.NoError(t, os.Remove(filepath.Join(d, name)))
			removed = append(removed, name)
		}
	}
	return removed
}

// TestV4EmitAfterPruneSimulation reproduces the postfix-run1 iter 3
// scenario more faithfully: after retiring, DELETE the .v/.ef/.efi/.vi
// files (simulating what FilterPreverifiedByPruneMode does at a
// --prune.mode=minimal fresh sync). Only .kv files survive. Then
// walk history for the mid-step window and emit v4.
//
// If the walker returns EMPTY when the .v files are absent (instead
// of erroring or reading MDBX), then EVERY key touched in-window
// is missed by v4 emit. State reads at target fall through to
// baseline .kv, but any key that was UPDATED in-window shows the
// stale pre-window value — the exact class of stale-state failure
// the memo describes.
//
// Expected outcomes:
//   - Walker for in-window range → yields addrUpdated (pass) OR empty (repro).
//   - Reads at target for addrUpdated → return valInWindow (pass) OR
//     stale valPreWindow (repro).
func TestV4EmitAfterPruneSimulation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const stepSize uint64 = 16
	f := setupV4EmitFixture(t, stepSize)

	// addrUpdated: touched pre-window (txN=5) AND in-window (txN=20).
	// If walker returns empty after prune, in-window write is lost →
	// v4 emit doesn't include it → reads at target return pre-window value.
	addrUpdated := bytes.Repeat([]byte{0xEE}, 20)
	valPreWindow := []byte("balance-at-5-should-be-stale")
	valInWindow := []byte("balance-at-20-should-win-at-target")

	f.applyWrites([]v4Write{
		{txN: 5, key: addrUpdated, val: valPreWindow},
		{txN: 20, key: addrUpdated, val: valInWindow},
	})
	f.buildFilesUpTo(stepSize * 2)
	f.agg.MergeLoop(ctx)

	// Delete .v/.ef/.efi/.vi files (prune simulation). Then reload.
	pruned := deletePrunedHistoryFiles(t, f.dirs)
	t.Logf("prune-simulation removed %d history/idx/accessor files: %v", len(pruned), pruned)
	require.NoError(t, f.agg.OpenFolder())

	targetTxN := uint64(24)
	fromTxN := stepSize * 1

	roTx, err := f.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// Walker output after prune. If it's empty, we've reproduced.
	var yielded [][]byte
	walker := historyKeyWalker(roTx, kv.AccountsDomain, fromTxN, targetTxN)
	require.NoError(t, walker(func(k []byte) bool {
		yielded = append(yielded, append([]byte(nil), k...))
		return true
	}))
	t.Logf("post-prune walker yielded %d keys for range (%d, %d]", len(yielded), fromTxN, targetTxN+1)

	// GetAsOf at target. If this returns valPreWindow (stale) instead of
	// valInWindow, we've reproduced the state-restoration failure.
	got, _, err := roTx.GetAsOf(kv.AccountsDomain, addrUpdated, targetTxN+1)
	require.NoError(t, err)
	t.Logf("post-prune GetAsOf(addrUpdated, %d) = %q (expected in-window: %q, stale would be: %q)",
		targetTxN+1, got, valInWindow, valPreWindow)

	// Behavioural assertion — walker must yield addrUpdated for a
	// correct emit, and GetAsOf must return the in-window value.
	// If the assertion fails, the failure mechanism is reproduced.
	require.ElementsMatch(t, [][]byte{addrUpdated}, yielded,
		"walker must yield addrUpdated even under prune-simulation "+
			"(history absent). If empty, .kv-only reads can't reconstruct in-window touches → v4 emit will be blind")
	require.Equal(t, valInWindow, got,
		"GetAsOf must return the in-window value even under prune-simulation. "+
			"If it returns %q (pre-window), state at target is stale — exact repro of the failure signature", valPreWindow)
}

// TestV4EmitTombstoneRecreateAcrossSteps reproduces the exact data
// shape state-lookup-at showed on the frozen datadir for the failing
// address 0x86c38852...: an address tombstoned in an early step,
// then RECREATED in a later step's history. If retire/merge or the
// walker loses the re-create event, the address appears tombstoned
// at target — matching the observed failure.
func TestV4EmitTombstoneRecreateAcrossSteps(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const stepSize uint64 = 16
	f := setupV4EmitFixture(t, stepSize)

	addr := bytes.Repeat([]byte{0x86}, 20)
	valFund := []byte("initial-funding")
	valRecreate := []byte("recreated-with-balance")

	// txN 5 (step 0): funded
	// txN 10 (step 0): deleted (tombstone)
	// txN 22 (step 1): recreated with new balance
	// target: txN 24 (mid-step 1)
	// Expected at target: valRecreate
	f.applyWrites([]v4Write{
		{txN: 5, key: addr, val: valFund},
		{txN: 10, key: addr, del: true}, // tombstone
		{txN: 22, key: addr, val: valRecreate},
	})
	f.buildFilesUpTo(stepSize * 2)
	f.agg.MergeLoop(ctx)
	require.NoError(t, f.agg.OpenFolder())

	targetTxN := uint64(24)
	fromTxN := stepSize * 1

	roTx, err := f.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// Walker must yield addr (touched in-window at txN=22).
	var yielded [][]byte
	walker := historyKeyWalker(roTx, kv.AccountsDomain, fromTxN, targetTxN)
	require.NoError(t, walker(func(k []byte) bool {
		yielded = append(yielded, append([]byte(nil), k...))
		return true
	}))
	require.ElementsMatch(t, [][]byte{addr}, yielded,
		"walker must yield the recreated address — in-window touch at txN=22")

	// GetAsOf must return the recreated value, not tombstone.
	got, ok, err := roTx.GetAsOf(kv.AccountsDomain, addr, targetTxN+1)
	require.NoError(t, err)
	require.True(t, ok, "GetAsOf must find the recreated address at target — "+
		"if this returns not-found, we've reproduced the failing address's post-emit state")
	require.Equal(t, valRecreate, got, "GetAsOf must return the recreated value, not tombstone")
}

// TestV4EmitTombstoneRecreateAfterPrune combines both: address is
// tombstoned then recreated in-window, THEN the .v/.ef files are
// pruned (minimal-mode simulation). This is the closest match to
// the postfix-run1 datadir's shape: address absent from state,
// history .v/.ef files present but potentially not covering the
// recreation event.
func TestV4EmitTombstoneRecreateAfterPrune(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const stepSize uint64 = 16
	f := setupV4EmitFixture(t, stepSize)

	addr := bytes.Repeat([]byte{0x86}, 20)
	valFund := []byte("initial-funding")
	valRecreate := []byte("recreated-with-balance")

	f.applyWrites([]v4Write{
		{txN: 5, key: addr, val: valFund},
		{txN: 10, key: addr, del: true}, // tombstone in step 0
		{txN: 22, key: addr, val: valRecreate}, // recreate in step 1 (in-window at target=24)
	})
	f.buildFilesUpTo(stepSize * 2)
	f.agg.MergeLoop(ctx)

	pruned := deletePrunedHistoryFiles(t, f.dirs)
	t.Logf("prune-simulation removed %d files: %v", len(pruned), pruned)
	require.NoError(t, f.agg.OpenFolder())

	targetTxN := uint64(24)
	fromTxN := stepSize * 1

	roTx, err := f.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	var yielded [][]byte
	walker := historyKeyWalker(roTx, kv.AccountsDomain, fromTxN, targetTxN)
	require.NoError(t, walker(func(k []byte) bool {
		yielded = append(yielded, append([]byte(nil), k...))
		return true
	}))
	t.Logf("post-prune+recreate walker yielded %d keys: %v", len(yielded), yielded)

	got, ok, err := roTx.GetAsOf(kv.AccountsDomain, addr, targetTxN+1)
	require.NoError(t, err)
	t.Logf("post-prune+recreate GetAsOf(addr, %d) = found=%v value=%q",
		targetTxN+1, ok, got)

	// After prune of .v/.ef, the recreation event is gone → walker
	// can't yield the recreated key → v4 emit misses it → state at
	// target reads baseline .kv, which has the TOMBSTONE from step 0.
	// If this assertion fails, we've reproduced the mechanism.
	require.ElementsMatch(t, [][]byte{addr}, yielded,
		"walker must yield addr even under prune — the recreation event MUST be visible")
	require.True(t, ok, "GetAsOf must find addr at target with the recreated value")
	require.Equal(t, valRecreate, got, "GetAsOf must return recreated value, not tombstone")
}
