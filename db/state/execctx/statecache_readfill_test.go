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
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
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
	sd.SetCanonicalCachesForTest(sc)

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

func currentStateCacheGeneration(t *testing.T, db kv.TemporalRoDB) cache.Generation {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	stateVersion, err := rawdb.GetStateVersion(tx)
	require.NoError(t, err)
	debug := tx.Debug()
	return cache.StateGeneration(
		stateVersion,
		debug.TxNumsInFiles(kv.AccountsDomain),
		debug.TxNumsInFiles(kv.StorageDomain),
		debug.TxNumsInFiles(kv.CodeDomain),
	)
}

func currentStateCacheView(t *testing.T, db kv.TemporalRoDB, stateCache *cache.StateCache) cache.ReadView {
	t.Helper()
	return stateCache.View(currentStateCacheGeneration(t, db))
}

func TestStateCacheReadViewSupportsHistoryDisabledLatestState(t *testing.T) {
	t.Parallel()

	db := newTestDbWithoutHistory(t, 16)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	debug := tx.Debug()
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		require.True(t, debug.HasCacheableLatestView(domain))
		_, exact := debug.DomainVisibleEnd(domain)
		require.False(t, exact)
	}

	stateVersion, err := rawdb.GetStateVersion(tx)
	require.NoError(t, err)
	generation := cache.StateGeneration(
		stateVersion,
		debug.TxNumsInFiles(kv.AccountsDomain),
		debug.TxNumsInFiles(kv.StorageDomain),
		debug.TxNumsInFiles(kv.CodeDomain),
	)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	publisher := stateCache.Publisher()
	publisher.Initialize(generation)
	publication := publisher.Begin()
	key, value := []byte("account"), []byte("value")
	publication.Publish(generation, 1, []cache.Update{{
		Domain: kv.AccountsDomain,
		Key:    key,
		Value:  value,
	}}, false)

	view, identityKnown := execctx.StateCacheReadView(tx, stateCache)
	require.True(t, identityKnown)
	got, ok := view.Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, value, got)
}

// During an in-flight unwind this SharedDomains is detached from StateCache,
// so the assertion compares the bounded database read without observing the
// cache generation that still serves readers of the durable state.
func TestAssertStateCache_NoFalsePanicDuringInFlightUnwind(t *testing.T) {
	// Mutates dbg.AssertStateCache — must not run in parallel with tests that
	// read it on the SD read path.

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	key, _, v2, diffs := twoStepRows(t, db, sc)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetCanonicalCachesForTest(sc)

	sd.Unwind(10, &diffs)

	old := dbg.AssertStateCache
	dbg.AssertStateCache = true
	t.Cleanup(func() { dbg.AssertStateCache = old })

	var v []byte
	require.NotPanics(t, func() {
		v, _, err = sd.GetLatest(kv.AccountsDomain, roTx, key)
	}, "assert must not fire on a legitimately-bounded cache hit during an in-flight unwind")
	require.NoError(t, err)
	require.Equal(t, v2, v, "the inactive cache must fall through to the bounded database read")
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
	sd.SetCanonicalCachesForTest(sc)
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
	sd2.SetCanonicalCachesForTest(sc)

	sd2.Unwind(3, &diffs)

	old := dbg.AssertStateCache
	dbg.AssertStateCache = true
	t.Cleanup(func() { dbg.AssertStateCache = old })

	var v []byte
	require.NotPanics(t, func() {
		v, _, err = sd2.GetLatest(kv.AccountsDomain, roTx, key)
	}, "assert must not fire when the in-flight unwind bound is at step 0")
	require.NoError(t, err)
	require.Equal(t, v1, v, "the inactive cache must fall through to the bounded database read")
}

func TestReadFill_UnwindDetachesWithoutRevokingStateCache(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()
	key, _, v2, diffs := twoStepRows(t, db, sc)
	generation := currentStateCacheGeneration(t, db)
	sc.Publisher().Begin().Publish(generation, 0, nil, true)
	durableView := sc.View(generation)
	sentinelKey := make([]byte, 20)
	sentinelKey[0] = 0xdd
	durableView.Fill(kv.AccountsDomain, sentinelKey, []byte("durable"), 0)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetCanonicalCachesForTest(sc)
	sd.Unwind(10, &diffs)

	got, _, err := sd.GetLatest(kv.AccountsDomain, roTx, key)
	require.NoError(t, err)
	require.Equal(t, v2, got)

	_, ok := durableView.Get(kv.AccountsDomain, sentinelKey)
	require.True(t, ok, "an uncommitted unwind must not revoke the durable cache")
	_, ok = durableView.Get(kv.AccountsDomain, key)
	require.False(t, ok, "the detached SharedDomains must not fill from its rewound database view")
}

func TestBoundedReadDoesNotFillStateCache(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	key := make([]byte, 20)
	key[0] = 0xaa
	value := encAccount(1)
	generation := currentStateCacheGeneration(t, db)
	stateCache.Publisher().Initialize(generation)
	view := stateCache.View(generation)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	agg := &boundedLatestAgg{domain: kv.AccountsDomain, key: key, value: value, step: 1}
	tx := &temporalTxWithAgg{TemporalTx: roTx, agg: agg}
	parent, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer parent.Close()
	child, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer child.Close()
	child.SetStateCacheReaderForTest(stateCache)
	child.SetParent(parent)

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^uint64(1))
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.AccountsDomain] = []kv.DomainEntryDiff{{Key: string(key) + string(stepBytes)}}
	parent.Unwind(0, &diffs)

	got, step, err := child.GetLatest(kv.AccountsDomain, tx, key)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, kv.Step(1), step)
	require.Equal(t, kv.Step(1), agg.maxStep)
	_, ok := view.Get(kv.AccountsDomain, key)
	require.False(t, ok, "a bounded historical value must not enter the latest-state cache")
}

func TestCodeHashFill_UnwindDetachesWithoutRevokingStateCache(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()

	key := make([]byte, 20)
	key[0] = 0xcc
	var codeHash common.Hash
	codeHash[0] = 0xdd
	value := accounts.SerialiseV3(&accounts.Account{
		Nonce:    1,
		CodeHash: accounts.InternCodeHash(codeHash),
	})

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetCanonicalCachesForTest(sc)
	sd.SetTxNum(20)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, key, value, 20, nil))
	require.NoError(t, sd.Commit(ctx, rwTx))
	generation := currentStateCacheGeneration(t, db)
	sc.Publisher().Begin().Publish(generation, 0, nil, true)
	durableView := sc.View(generation)
	sentinelKey := make([]byte, 20)
	sentinelKey[0] = 0xee
	durableView.Fill(kv.AccountsDomain, sentinelKey, []byte("durable"), 0)

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^uint64(1))
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.AccountsDomain] = []kv.DomainEntryDiff{{Key: string(key) + string(stepBytes)}}

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd2, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.SetCanonicalCachesForTest(sc)
	sd2.Unwind(10, &diffs)

	got := sd2.CodeHashForAddr(roTx, key, 20)
	require.Equal(t, codeHash[:], got)

	_, ok := durableView.Get(kv.AccountsDomain, sentinelKey)
	require.True(t, ok, "an uncommitted unwind must not revoke the durable cache")
	_, ok = durableView.Get(kv.AccountsDomain, key)
	require.False(t, ok, "code-hash lookup through the rewound view must not fill the durable cache")
}

func TestSpeculativeUnwindDoesNotPublishStateCache(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	key, _, v2, diffs := twoStepRows(t, db, sc)

	view := currentStateCacheView(t, db, sc)
	got, ok := view.Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, v2, got)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetStateCacheReaderForTest(sc)
	sd.Unwind(10, &diffs)

	got, ok = view.Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, v2, got)
}

type fakeCacheBinder struct{ called bool }

func (f *fakeCacheBinder) BindStateCache(*cache.StateCache) { f.called = true }

type fakeHasAgg struct{ f *fakeCacheBinder }

func (h fakeHasAgg) Agg() any { return h.f }

type fakeHasBadAgg struct{}

func (fakeHasBadAgg) Agg() any { return struct{}{} }

// The guard is load-bearing for every StateCache and must fail loudly when the
// DB cannot enforce the visibility invariant. A nil cache needs no guard.
func TestBindStateCacheToAggregator(t *testing.T) {
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)

	f := &fakeCacheBinder{}
	execctx.BindStateCacheToAggregator(fakeHasAgg{f}, sc)
	require.True(t, f.called)

	require.NotPanics(t, func() { execctx.BindStateCacheToAggregator(struct{}{}, nil) },
		"no cache, no invariant to bind — shape is irrelevant")
	require.Panics(t, func() { execctx.BindStateCacheToAggregator(struct{}{}, sc) },
		"a db that cannot produce its aggregator must fail loudly, not drop the guard")
	require.Panics(t, func() { execctx.BindStateCacheToAggregator(fakeHasBadAgg{}, sc) },
		"an aggregator without BindStateCache must fail loudly, not drop the binding")
}

func TestBindStateCacheToAggregator_FillsDisabledStillBinds(t *testing.T) {
	t.Setenv("STATE_CACHE_FILLS", "false")
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)

	f := &fakeCacheBinder{}
	execctx.BindStateCacheToAggregator(fakeHasAgg{f}, sc)
	require.True(t, f.called)
}
