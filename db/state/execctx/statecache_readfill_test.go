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

// During an in-flight unwind the mem overlay bounds reads of an affected key
// by maxStep while MDBX still holds the not-yet-deleted dying row inside that
// bound. A cache hit legitimately below the unwind floor then diverges from
// the maxStep-bounded DB read, and the ASSERT_STATE_CACHE comparison must not
// blame the cache for it.
func TestAssertStateCache_NoFalsePanicDuringInFlightUnwind(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
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

	// A live cache entry below the unwind floor: the restored (correct) value.
	sc.Put(kv.AccountsDomain, key, v1, 5)
	sd.Unwind(10, &diffs) // in-flight: mem publishes maxStep=1; MDBX still holds the step-1 row

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

// The read-fill after a fall-through read must not replace a live cache
// entry: it never carries newer information than a flush-apply, and during an
// in-flight unwind the bounded DB read can even return the not-yet-deleted
// dying row.
func TestReadFill_DoesNotClobberLiveEntry(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
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
	sc.Put(kv.AccountsDomain, key, v3, 40)

	v, _, err := sd.GetLatest(kv.AccountsDomain, roTx, key)
	require.NoError(t, err)
	require.Equal(t, v2, v, "fall-through read serves the maxStep-bounded DB row")

	got, ok := sc.Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, v3, got, "read-fill must not clobber the live entry")
}

// Negative results (missing account) must be stamped with the domain's
// progress at observation time, not a synthetic step-0 bound that survives
// every unwind.
func TestReadFill_NegativeStampedWithProgress(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
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
	sd2, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.SetStateCacheForTest(sc)

	missing := make([]byte, 20)
	missing[0] = 0x02
	v, _, err := sd2.GetLatest(kv.AccountsDomain, roTx, missing)
	require.NoError(t, err)
	require.Empty(t, v)
	_, ok := sc.Get(kv.AccountsDomain, missing)
	require.True(t, ok, "the negative result must be cached")

	// The domain's progress is 100 (the committed write), so any unwind at or
	// below it must drop the negative instead of letting it outlive the fact.
	sc.Unwind(50)
	_, ok = sc.Get(kv.AccountsDomain, missing)
	require.False(t, ok, "a negative observed at progress 100 must not survive an unwind to 50")
}

// A flush-apply for a deletion must leave a live tombstone, not remove the
// entry: with the key absent, a read-fill from a straddling pre-delete
// snapshot re-inserts the deleted value as live (PutIfAbsent only defers to
// live entries), and the resurrected account is served as canonical.
func TestReadFill_DoesNotResurrectDeletedKey(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()

	key := make([]byte, 20)
	key[0] = 0xbb
	v1 := encAccount(1)

	rwTx1, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx1.Rollback()
	sd1, err := execctx.NewSharedDomains(ctx, rwTx1, log.New())
	require.NoError(t, err)
	defer sd1.Close()
	sd1.SetStateCacheForTest(sc)
	sd1.SetTxNum(10)
	require.NoError(t, sd1.DomainPut(kv.AccountsDomain, rwTx1, key, v1, 10, nil))
	require.NoError(t, sd1.Commit(ctx, rwTx1))

	// A reader snapshot from before the deletion.
	roTxOld, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTxOld.Rollback()

	rwTx2, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx2.Rollback()
	sd2, err := execctx.NewSharedDomains(ctx, rwTx2, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.SetStateCacheForTest(sc)
	sd2.SetTxNum(20)
	require.NoError(t, sd2.DomainDel(kv.AccountsDomain, rwTx2, key, 20, v1))
	require.NoError(t, sd2.Commit(ctx, rwTx2))

	// The straddling reader: any fill it makes must not resurrect the account.
	sdOld, err := execctx.NewSharedDomains(ctx, roTxOld, log.New())
	require.NoError(t, err)
	defer sdOld.Close()
	sdOld.SetStateCacheForTest(sc)
	_, _, err = sdOld.GetLatest(kv.AccountsDomain, roTxOld, key)
	require.NoError(t, err)

	roTxNew, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTxNew.Rollback()
	sdNew, err := execctx.NewSharedDomains(ctx, roTxNew, log.New())
	require.NoError(t, err)
	defer sdNew.Close()
	sdNew.SetStateCacheForTest(sc)
	v, _, err := sdNew.GetLatest(kv.AccountsDomain, roTxNew, key)
	require.NoError(t, err)
	require.Empty(t, v, "the straddling read-fill must not resurrect the deleted account")

	// The tombstone is stamped with the delete's txNum, so an unwind at or
	// below it drops the negative instead of letting it outlive the deletion.
	_, cTxNum, ok := sc.GetWithTxNum(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, uint64(20), cTxNum)
}

// The code-domain equivalent of the tombstone test: a code deletion must
// leave a live no-code marker on the addr binding, or a read-fill from a
// straddling pre-delete snapshot re-binds the deleted code as a live hit.
func TestReadFill_DoesNotResurrectDeletedCode(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	sc := newSmallStateCache()

	addr := make([]byte, 20)
	addr[0] = 0xcc
	code := []byte{0xaa, 1, 2, 3}

	rwTx1, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx1.Rollback()
	sd1, err := execctx.NewSharedDomains(ctx, rwTx1, log.New())
	require.NoError(t, err)
	defer sd1.Close()
	sd1.SetStateCacheForTest(sc)
	sd1.SetTxNum(10)
	require.NoError(t, sd1.DomainPut(kv.CodeDomain, rwTx1, addr, code, 10, nil))
	require.NoError(t, sd1.Commit(ctx, rwTx1))

	// A reader snapshot from before the deletion.
	roTxOld, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTxOld.Rollback()

	rwTx2, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx2.Rollback()
	sd2, err := execctx.NewSharedDomains(ctx, rwTx2, log.New())
	require.NoError(t, err)
	defer sd2.Close()
	sd2.SetStateCacheForTest(sc)
	sd2.SetTxNum(20)
	require.NoError(t, sd2.DomainDel(kv.CodeDomain, rwTx2, addr, 20, code))
	require.NoError(t, sd2.Commit(ctx, rwTx2))

	// The straddling reader: any fill it makes must not resurrect the code.
	sdOld, err := execctx.NewSharedDomains(ctx, roTxOld, log.New())
	require.NoError(t, err)
	defer sdOld.Close()
	sdOld.SetStateCacheForTest(sc)
	_, _, err = sdOld.GetLatest(kv.CodeDomain, roTxOld, addr)
	require.NoError(t, err)

	roTxNew, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTxNew.Rollback()
	sdNew, err := execctx.NewSharedDomains(ctx, roTxNew, log.New())
	require.NoError(t, err)
	defer sdNew.Close()
	sdNew.SetStateCacheForTest(sc)
	v, _, err := sdNew.GetLatest(kv.CodeDomain, roTxNew, addr)
	require.NoError(t, err)
	require.Empty(t, v, "the straddling read-fill must not resurrect the deleted code")

	// The marker is stamped with the delete's txNum, so an unwind at or below
	// it drops the negative instead of letting it outlive the deletion.
	_, cTxNum, ok := sc.GetWithTxNum(kv.CodeDomain, addr)
	require.True(t, ok, "the deletion must be cached as a live no-code marker")
	require.Equal(t, uint64(20), cTxNum)
}
