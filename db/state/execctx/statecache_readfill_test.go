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

package execctx_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func encAccount(nonce uint64) []byte {
	a := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(nonce * 1000)}
	return accounts.SerialiseV3(&a)
}

// twoStepRows commits two versions of one account key so MDBX holds rows at
// step 0 (txNum 5, v1) and step 1 (txNum 20, v2), and returns a delete-only
// unwind diff for the step-1 row — the legacy-changeset shape that makes the
// mem overlay publish a per-key maxStep bound while MDBX still holds the
// dying row.
func twoStepRows(t *testing.T, db kv.TemporalRwDB, sc *cache.StateCache) (key, v1, v2 []byte, diffs [kv.DomainLen][]kv.DomainEntryDiff) {
	t.Helper()
	ctx := t.Context()
	key = make([]byte, 20)
	key[0] = 0xaa
	v1, v2 = encAccount(1), encAccount(2)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetStateCacheForTest(sc)

	sd.SetTxNum(5)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, key, v1, 5, nil))
	sd.SetTxNum(20)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, key, v2, 20, v1))
	require.NoError(t, sd.Commit(ctx, rwTx))

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^uint64(1))
	diffs[kv.AccountsDomain] = []kv.DomainEntryDiff{{Key: string(key) + string(stepBytes), Value: nil}}
	return key, v1, v2, diffs
}

func newSmallStateCache() *cache.StateCache {
	b := 1 * datasize.MB
	return cache.NewStateCache(b, b, b, b)
}

func frontierAt(end uint64) cache.Frontier {
	return cache.FrontierFunc(func(kv.Domain) (uint64, bool) { return end, true })
}

// seed places an entry with an exact txNum stamp through the public fill API
// without moving the applied frontier. A positive passes admission at any
// applied end; a negative is stamped frontier-1 by the fill path, so it must
// be seeded while the applied end is at most txNum+1.
func seed(sc *cache.StateCache, domain kv.Domain, k, v []byte, txNum uint64) {
	end := uint64(math.MaxUint64)
	if len(v) == 0 {
		end = txNum + 1
	}
	sc.View(frontierAt(end)).Fill(domain, k, v, txNum)
}

type visibleEndCountingDebugTx struct {
	kv.TemporalDebugTx
	calls uint64
	last  uint64
}

func (tx *visibleEndCountingDebugTx) DomainVisibleEnd(domain kv.Domain) (uint64, bool) {
	tx.calls++
	end, ok := tx.TemporalDebugTx.DomainVisibleEnd(domain)
	tx.last = end
	return end, ok
}

type visibleEndCountingRwTx struct {
	kv.TemporalRwTx
	debug *visibleEndCountingDebugTx
}

func (tx *visibleEndCountingRwTx) Debug() kv.TemporalDebugTx {
	return tx.debug
}

func TestReadFill_MemoizesWritableVisibleEndUntilFlush(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)

	baseTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer baseTx.Rollback()
	debug := &visibleEndCountingDebugTx{TemporalDebugTx: baseTx.Debug()}
	rwTx := &visibleEndCountingRwTx{TemporalRwTx: baseTx, debug: debug}
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	domains.SetStateCacheForTest(stateCache)

	for i := byte(2); i <= 3; i++ {
		missing := make([]byte, 20)
		missing[0] = i
		value, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, missing)
		require.NoError(t, err)
		require.Empty(t, value)
	}
	require.Equal(t, uint64(1), debug.calls)
	initialEnd := debug.last

	written := make([]byte, 20)
	written[0] = 4
	domains.SetTxNum(20)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, written, encAccount(2), 20, nil))

	// The memo must re-derive inside Commit's validate window (after the
	// internal flush, before the tx commits): reads here already see the
	// advanced frontier.
	require.NoError(t, domains.Commit(ctx, rwTx, func(kv.RwTx) error {
		missing := make([]byte, 20)
		missing[0] = 5
		value, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, missing)
		require.NoError(t, err)
		require.Empty(t, value)
		return nil
	}))
	require.Equal(t, uint64(2), debug.calls)
	require.Greater(t, debug.last, initialEnd)
}

// An SD with a state cache must route every flush through Commit: a plain
// Flush neither applies nor invalidates, so the cache would keep serving
// pre-flush values for the flushed keys forever.
func TestFlushRejectsCacheAttachedSD(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	require.NoError(t, domains.Flush(ctx, rwTx), "cache-less SDs may flush and commit themselves")

	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	domains.SetStateCacheForTest(stateCache)
	require.Panics(t, func() { _ = domains.Flush(ctx, rwTx) },
		"a wiring bug must fail loudly, like the SetStateCache assert — an error can be swallowed")
}

// The incoherence the Flush rejection prevents, end to end: after v1 is
// committed (the cache holds it), flushing v2 through another cache-attached
// SD and committing the tx would leave the cache serving v1 while MDBX holds
// v2. The rejection fires at exactly that step; routing through Commit keeps
// the cache coherent.
func TestFlushRejectionPreventsStaleCachedReads(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)

	slot := make([]byte, 52)
	slot[0] = 1
	v1, v2 := []byte{1}, []byte{2}

	tx1, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx1.Rollback()
	sd1, err := execctx.NewSharedDomains(ctx, tx1, log.New())
	require.NoError(t, err)
	defer sd1.Close()
	sd1.SetStateCacheForTest(stateCache)
	sd1.SetTxNum(10)
	require.NoError(t, sd1.DomainPut(kv.StorageDomain, tx1, slot, v1, 10, nil))
	require.NoError(t, sd1.Commit(ctx, tx1))
	sd1.Close()

	got, ok := stateCache.View(nil).Get(kv.StorageDomain, slot)
	require.True(t, ok)
	require.Equal(t, v1, got)

	tx2, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx2.Rollback()
	sd2, err := execctx.NewSharedDomains(ctx, tx2, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.SetStateCacheForTest(stateCache)
	sd2.SetTxNum(20)
	require.NoError(t, sd2.DomainPut(kv.StorageDomain, tx2, slot, v2, 20, nil))
	require.Panics(t, func() { _ = sd2.Flush(ctx, tx2) },
		"the step that would split the cache (v1) from MDBX (v2) must be rejected")

	require.NoError(t, sd2.Commit(ctx, tx2))
	got, ok = stateCache.View(nil).Get(kv.StorageDomain, slot)
	require.True(t, ok)
	require.Equal(t, v2, got, "Commit keeps the cache coherent with MDBX")
}

// During an in-flight unwind the mem overlay bounds reads of an affected key
// by maxStep while MDBX still holds the not-yet-deleted dying row inside that
// bound. A cache hit legitimately below the unwind floor then diverges from
// the maxStep-bounded DB read, and the ASSERT_STATE_CACHE comparison must not
// blame the cache for it.
func TestAssertStateCache_NoFalsePanicDuringInFlightUnwind(t *testing.T) {
	// Mutates dbg.AssertStateCache — must not run in parallel with tests that
	// read it on the SD read path.

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()
	key, v1, _, diffs := twoStepRows(t, db, sc)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetStateCacheForTest(sc)

	sd.Unwind(10, &diffs) // in-flight: mem publishes maxStep=1; MDBX still holds the step-1 row
	// A live cache entry below the unwind floor: the restored (correct) value,
	// as a post-unwind fill would insert it.
	seed(sc, kv.AccountsDomain, key, v1, 5)

	old := dbg.AssertStateCache
	dbg.AssertStateCache = true
	t.Cleanup(func() { dbg.AssertStateCache = old })

	var v []byte
	require.NotPanics(t, func() {
		v, _, err = sd.GetLatest(kv.AccountsDomain, roTx, key)
	}, "assert must not fire on a legitimately-bounded cache hit during an in-flight unwind")
	require.NoError(t, err)
	require.Equal(t, v1, v, "the cache serves the restored value")
}

// Same invariant with the unwound key bound at step 0 — a young chain's whole
// state lives there. A step-0 bound must not read as "no bound": the assert
// must stay silenced while MDBX still holds the dying step-0 row, and the
// cache serves the correct negative (the key was created inside the unwound
// range, so the delete-shape diff restores nothing).
func TestAssertStateCache_NoFalsePanicDuringInFlightUnwindStepZero(t *testing.T) {
	// Mutates dbg.AssertStateCache — must not run in parallel with tests that
	// read it on the SD read path.

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()

	key := make([]byte, 20)
	key[0] = 0xbb
	v1 := encAccount(1)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetTxNum(5)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, key, v1, 5, nil))
	require.NoError(t, sd.Commit(ctx, rwTx))

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^uint64(0))
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.AccountsDomain] = []kv.DomainEntryDiff{{Key: string(key) + string(stepBytes), Value: nil}}

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd2, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.SetStateCacheForTest(sc)

	sd2.Unwind(3, &diffs)
	seed(sc, kv.AccountsDomain, key, nil, 2)

	old := dbg.AssertStateCache
	dbg.AssertStateCache = true
	t.Cleanup(func() { dbg.AssertStateCache = old })

	var v []byte
	require.NotPanics(t, func() {
		v, _, err = sd2.GetLatest(kv.AccountsDomain, roTx, key)
	}, "assert must not fire when the in-flight unwind bound is at step 0")
	require.NoError(t, err)
	require.Empty(t, v, "the cache serves the correct negative")
}

// The read-fill after a fall-through read must not replace a live cache
// entry: it never carries newer information than a post-commit apply, and during an
// in-flight unwind the bounded DB read can even return the not-yet-deleted
// dying row.
func TestReadFill_DoesNotClobberLiveEntry(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()
	key, _, v2, diffs := twoStepRows(t, db, sc)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetStateCacheForTest(sc)

	sd.Unwind(10, &diffs)
	// A live (current-epoch) entry above the read bound: the maxStep gate turns
	// the hit into a miss, so the read falls through to the bounded DB read.
	v3 := encAccount(3)
	seed(sc, kv.AccountsDomain, key, v3, 40)

	v, _, err := sd.GetLatest(kv.AccountsDomain, roTx, key)
	require.NoError(t, err)
	require.Equal(t, v2, v, "fall-through read serves the maxStep-bounded DB row")

	got, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, v3, got, "read-fill must not clobber the live entry")
}

// A negative reflects transactions below the read view's exclusive frontier,
// so its unwind stamp is the last included txNum.
func TestReadFill_NegativeUsesLastVisibleTxNum(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetStateCacheForTest(sc)

	written := make([]byte, 20)
	written[0] = 0x01
	sd.SetTxNum(100)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, written, encAccount(7), 100, nil))
	require.NoError(t, sd.Commit(ctx, rwTx))

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	visibleEnd, ok := roTx.Debug().DomainVisibleEnd(kv.AccountsDomain)
	require.True(t, ok)
	require.NotZero(t, visibleEnd)
	sd2, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.SetStateCacheForTest(sc)

	missing := make([]byte, 20)
	missing[0] = 0x02
	v, _, err := sd2.GetLatest(kv.AccountsDomain, roTx, missing)
	require.NoError(t, err)
	require.Empty(t, v)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, missing)
	require.True(t, ok, "the negative result must be cached")

	sc.Applier().Unwind(visibleEnd)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, missing)
	require.True(t, ok, "an unwind starting after the read view must preserve the negative")

	sc.Applier().Unwind(visibleEnd - 1)
	_, ok = sc.View(nil).Get(kv.AccountsDomain, missing)
	require.False(t, ok, "an unwind of the view's last included txNum must invalidate the negative")
}

type fakeForbidder struct{ called bool }

func (f *fakeForbidder) ForbidVisibilityLowering() { f.called = true }

// fakeTemporalDB satisfies kv.TemporalRwDB by embedding (the interface now
// carries Agg, so a DB shape without it no longer compiles); only Agg is
// implemented — the guard must not touch anything else.
type fakeTemporalDB struct {
	kv.TemporalRwDB
	agg any
}

func (d fakeTemporalDB) Agg() any { return d.agg }

// The binding is load-bearing: for a fill-enabled cache it must either bind
// the invariant or fail loudly — never silently drop it. A nil or apply-only
// cache needs no binding at all.
func TestBindAggregator(t *testing.T) {
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)

	f := &fakeForbidder{}
	sc.BindAggregator(fakeTemporalDB{agg: f})
	require.True(t, f.called)
	require.True(t, sc.AggregatorBound())

	var nilCache *cache.StateCache
	require.NotPanics(t, func() { nilCache.BindAggregator(fakeTemporalDB{}) },
		"no cache, no invariant to bind — the aggregator is never consulted")
	sc2 := newSmallStateCache()
	t.Cleanup(sc2.Close)
	require.Panics(t, func() { sc2.BindAggregator(fakeTemporalDB{agg: struct{}{}}) },
		"an aggregator without ForbidVisibilityLowering must fail loudly, not drop the binding")
	require.PanicsWithValue(t,
		"assert: fill-enabled StateCache bound to a DB without an aggregator — the visibility-lowering guard would be silently dropped",
		func() { sc2.BindAggregator(fakeTemporalDB{}) },
		"a DB without an aggregator must name that case, not report a nil type mismatch")
}

type nilDebugRwTx struct {
	kv.TemporalRwTx
}

func (nilDebugRwTx) Debug() kv.TemporalDebugTx { return nil }

// A tx without a debug backend (MemoryMutation over a nil db) has no exact
// frontier: reads must still work and simply never fill.
func TestReadFill_NilDebugTxSkipsFills(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)

	baseTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer baseTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, baseTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	domains.SetStateCacheForTest(stateCache)

	missing := make([]byte, 20)
	missing[0] = 7
	value, _, err := domains.GetLatest(kv.AccountsDomain, nilDebugRwTx{TemporalRwTx: baseTx}, missing)
	require.NoError(t, err)
	require.Empty(t, value)

	_, ok := stateCache.View(nil).Get(kv.AccountsDomain, missing)
	require.False(t, ok, "no exact frontier means no fill")
}

// The binding is asserted at the real wiring point, so no future call site
// can wire a fill-enabled cache while forgetting the aggregator guard.
func TestSetStateCacheRequiresBoundAggregator(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	unbound := newSmallStateCache()
	t.Cleanup(unbound.Close)
	require.Panics(t, func() { domains.SetStateCache(unbound) },
		"wiring a fill-enabled cache without a bound aggregator must fail loudly")

	bound := newSmallStateCache()
	t.Cleanup(bound.Close)
	f := &fakeForbidder{}
	bound.BindAggregator(fakeTemporalDB{agg: f})
	require.True(t, f.called)
	require.NotPanics(t, func() { domains.SetStateCache(bound) })
}

// Apply-only mode must not pay for fills it will never make: the plain miss
// path used to box a frontier only for the fill to no-op. Asserted as a
// difference against the cache-less read, so unrelated allocations elsewhere
// in the read path cannot fail this test.
func TestApplyOnlyMissPathBindsNoFrontier(t *testing.T) {
	t.Setenv("STATE_CACHE_FILLS", "false")

	ctx := t.Context()
	db := newTestDb(t, 16)

	missAllocs := func(withCache bool) float64 {
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		if withCache {
			stateCache := newSmallStateCache()
			t.Cleanup(stateCache.Close)
			domains.SetStateCacheForTest(stateCache)
		}
		missing := make([]byte, 20)
		missing[0] = 7
		return testing.AllocsPerRun(100, func() {
			v, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, missing)
			if err != nil || len(v) != 0 {
				t.Fatalf("expected a clean negative read, got %x %v", v, err)
			}
		})
	}

	require.Equal(t, missAllocs(false), missAllocs(true),
		"an apply-only cache must add no allocations to the miss path")
}

// An apply-only cache (STATE_CACHE_FILLS=false) has no fills for a lowered
// frontier to poison, so the binding must not constrain the aggregator.
func TestBindAggregator_ApplyOnlySkips(t *testing.T) {
	t.Setenv("STATE_CACHE_FILLS", "false")
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)

	f := &fakeForbidder{}
	sc.BindAggregator(fakeTemporalDB{agg: f})
	require.False(t, f.called)
}
