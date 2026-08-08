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
	"path/filepath"
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

// write applies (key, value) at the given txN via SharedDomains.
// values with length zero are recorded as tombstones (empty value —
// the accounts domain's "deleted" marker).
type v4Write struct {
	txN uint64
	key []byte
	val []byte
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
		require.NoError(f.t, domains.DomainPut(kv.AccountsDomain, rwTx, w.key, w.val, w.txN, prev))
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
