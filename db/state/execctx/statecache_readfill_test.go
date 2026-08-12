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
	"errors"
	"math"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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

func frontierAtStateVersion(t *testing.T, tx kv.Tx, frontier cache.Frontier) cache.Frontier {
	t.Helper()
	stateVersion, err := rawdb.GetStateVersion(tx)
	require.NoError(t, err)
	return cache.FrontierWithStateVersion(frontier, stateVersion)
}

// seed places an entry with an exact txNum stamp through the public fill API
// without moving the applied frontier. A positive passes admission at any
// applied end; a negative is stamped frontier-1 by the fill path, so it must
// be seeded while the applied end is at most txNum+1.
func seed(t *testing.T, sc *cache.StateCache, tx kv.Tx, domain kv.Domain, k, v []byte, txNum uint64) {
	t.Helper()
	end := uint64(math.MaxUint64)
	if len(v) == 0 {
		end = txNum + 1
	}
	sc.View(frontierAtStateVersion(t, tx, frontierAt(end))).Fill(domain, k, v, txNum)
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

type failStateVersionOnceRwTx struct {
	kv.TemporalRwTx
	stateVersionReads int
}

func (tx *failStateVersionOnceRwTx) ReadSequence(table string) (uint64, error) {
	if table == string(kv.PlainStateVersion) {
		tx.stateVersionReads++
		if tx.stateVersionReads == 1 {
			return 0, errors.New("temporary state-version read failure")
		}
	}
	return tx.TemporalRwTx.ReadSequence(table)
}

func TestNewSharedDomains_StateVersionReadErrorFailsConstruction(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)
	baseTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer baseTx.Rollback()
	tx := &failStateVersionOnceRwTx{TemporalRwTx: baseTx}

	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	if domains != nil {
		defer domains.Close()
	}
	require.ErrorContains(t, err, "read base state version")
	require.Nil(t, domains)
	require.Equal(t, 1, tx.stateVersionReads)
}

func TestStateCache_MergedUnwindPublishesInvalidation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	parent, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer parent.Close()
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	parent.SetStateCacheForTest(stateCache)

	key := make([]byte, 20)
	key[0] = 0x01
	seed(t, stateCache, tx, kv.AccountsDomain, key, encAccount(1), 12)
	_, ok := stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok)
	var parentDiffs [kv.DomainLen][]kv.DomainEntryDiff
	parent.Unwind(15, &parentDiffs)
	_, ok = stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "an entry below the parent's unwind boundary must remain live")

	child, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	var childDiffs [kv.DomainLen][]kv.DomainEntryDiff
	child.Unwind(10, &childDiffs)
	require.NoError(t, parent.Merge(ctx, 0, child, 0))
	_, ok = stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "the merged unwind must invalidate the cache before the parent serves reads")

	seed(t, stateCache, tx, kv.AccountsDomain, key, encAccount(1), 12)
	_, ok = stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "a fill admitted after staging exercises commit-time invalidation")
	require.NoError(t, parent.Commit(ctx, tx))

	_, ok = stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "the merged unwind must invalidate entries from the discarded range")
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
	require.NoError(t, domains.Flush(ctx, rwTx))

	missing := make([]byte, 20)
	missing[0] = 5
	value, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, missing)
	require.NoError(t, err)
	require.Empty(t, value)
	require.Equal(t, uint64(2), debug.calls)
	require.Greater(t, debug.last, initialEnd)
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
	t.Cleanup(sc.Close)
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
	seed(t, sc, roTx, kv.AccountsDomain, key, v1, 5)

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
	t.Cleanup(sc.Close)

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
	seed(t, sc, roTx, kv.AccountsDomain, key, nil, 2)

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
	t.Cleanup(sc.Close)
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
	seed(t, sc, roTx, kv.AccountsDomain, key, v3, 40)

	v, _, err := sd.GetLatest(kv.AccountsDomain, roTx, key)
	require.NoError(t, err)
	require.Equal(t, v2, v, "fall-through read serves the maxStep-bounded DB row")

	got, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, v3, got, "read-fill must not clobber the live entry")
}

func TestReadFill_SkipsInFlightUnwindRow(t *testing.T) {
	t.Parallel()

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
	sd.SetStateCacheForTest(sc)
	sd.Unwind(10, &diffs)

	got, _, err := sd.GetLatest(kv.AccountsDomain, roTx, key)
	require.NoError(t, err)
	require.Equal(t, v2, got)

	_, ok := sc.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "a bounded in-flight unwind read must not populate the shared cache")
}

func TestCodeHashFill_SkipsInFlightUnwindRow(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)

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
	sd.SetStateCacheForTest(sc)
	sd.SetTxNum(20)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, key, value, 20, nil))
	require.NoError(t, sd.Commit(ctx, rwTx))

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
	sd2.SetStateCacheForTest(sc)
	sd2.Unwind(10, &diffs)

	got := sd2.CodeHashForAddr(roTx, key, 20)
	require.Equal(t, codeHash[:], got)

	_, ok := sc.View(nil).GetAddrCodeHash(key)
	require.False(t, ok, "a bounded in-flight unwind read must not seed a code-hash mapping")
}

func TestGetCode_RespectsStagedUnwindBound(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	codeStore := cache.NewCodeStore(1<<20, 1<<20)

	addr := make([]byte, 20)
	addr[0] = 0xdd
	code := []byte{0x60, 0x01, 0x60, 0x00, 0x55}
	account := accounts.SerialiseV3(&accounts.Account{
		Nonce:    1,
		CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code)),
	})

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedDomains, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	defer seedDomains.Close()
	seedDomains.SetStateCacheForTest(stateCache)
	seedDomains.SetCodeStore(codeStore)
	seedDomains.SetTxNum(20)
	require.NoError(t, seedDomains.DomainPut(kv.AccountsDomain, seedTx, addr, account, 20, nil))
	require.NoError(t, seedDomains.DomainPut(kv.CodeDomain, seedTx, addr, code, 20, nil))
	require.NoError(t, seedDomains.Commit(ctx, seedTx))

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^uint64(1))
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.AccountsDomain] = []kv.DomainEntryDiff{{Key: string(addr) + string(stepBytes)}}
	diffs[kv.CodeDomain] = []kv.DomainEntryDiff{{Key: string(addr) + string(stepBytes)}}

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	unwindDomains, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer unwindDomains.Close()
	unwindDomains.SetStateCacheForTest(stateCache)
	unwindDomains.SetCodeStore(codeStore)
	unwindDomains.Unwind(10, &diffs)

	futureAddr := make([]byte, 20)
	futureAddr[0] = 0xee
	futureCode := []byte{0x60, 0x02, 0x60, 0x00, 0x55}
	futureHash := crypto.Keccak256Hash(futureCode)
	futureView := stateCache.View(frontierAtStateVersion(t, roTx, frontierAt(math.MaxUint64)))
	futureView.Fill(kv.CodeDomain, futureAddr, futureCode, 40)
	futureView.SeedAddrCodeHash(addr, [32]byte(futureHash), 40)

	got, ok, err := unwindDomains.GetCode(roTx, addr, 20)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, code, got,
		"the code-hash fast path must ignore cache entries above the staged unwind bound")
}

// A negative reflects transactions below the read view's exclusive frontier,
// so its unwind stamp is the last included txNum.
func TestReadFill_NegativeUsesLastVisibleTxNum(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)

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

type fakeHasAgg struct{ f *fakeForbidder }

func (h fakeHasAgg) Agg() any { return h.f }

type fakeHasBadAgg struct{}

func (fakeHasBadAgg) Agg() any { return struct{}{} }

// The guard is load-bearing: for a fill-enabled cache it must either bind the
// invariant or fail loudly — never silently drop it on a DB shape mismatch.
// A nil or apply-only cache needs no guard at all.
func TestGuardAggregatorForCache(t *testing.T) {
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)

	f := &fakeForbidder{}
	execctx.GuardAggregatorForCache(fakeHasAgg{f}, sc)
	require.True(t, f.called)

	require.NotPanics(t, func() { execctx.GuardAggregatorForCache(struct{}{}, nil) },
		"no cache, no invariant to bind — shape is irrelevant")
	require.Panics(t, func() { execctx.GuardAggregatorForCache(struct{}{}, sc) },
		"a db that cannot produce its aggregator must fail loudly, not drop the guard")
	require.Panics(t, func() { execctx.GuardAggregatorForCache(fakeHasBadAgg{}, sc) },
		"an aggregator without ForbidVisibilityLowering must fail loudly, not drop the guard")
}

// An apply-only cache (STATE_CACHE_FILLS=false) has no fills for a lowered
// frontier to poison, so the guard must not constrain the aggregator.
func TestGuardAggregatorForCache_ApplyOnlySkips(t *testing.T) {
	t.Setenv("STATE_CACHE_FILLS", "false")
	sc := newSmallStateCache()
	t.Cleanup(sc.Close)

	f := &fakeForbidder{}
	execctx.GuardAggregatorForCache(fakeHasAgg{f}, sc)
	require.False(t, f.called)
}
