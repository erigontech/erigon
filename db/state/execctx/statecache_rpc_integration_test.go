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
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/shards"
)

func TestEmbeddedRPCCacheViewDoesNotResurrectDeletedAccount(t *testing.T) {
	testEmbeddedRPCCacheViewDoesNotResurrectDeletedValue(t, kv.AccountsDomain)
}

func TestEmbeddedRPCCacheViewDoesNotResurrectDeletedStorage(t *testing.T) {
	testEmbeddedRPCCacheViewDoesNotResurrectDeletedValue(t, kv.StorageDomain)
}

func TestEmbeddedRPCCacheViewDoesNotResurrectDeletedCode(t *testing.T) {
	testEmbeddedRPCCacheViewDoesNotResurrectDeletedValue(t, kv.CodeDomain)
}

func TestEmbeddedRPCCacheViewDoesNotRefillUnwoundAccount(t *testing.T) {
	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	key, v1, v2, diffs := twoStepRows(t, db, stateCache)

	rpcTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer rpcTx.Rollback()

	unwindTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer unwindTx.Rollback()
	unwindDomains, err := execctx.NewSharedDomains(ctx, unwindTx, log.New())
	require.NoError(t, err)
	defer unwindDomains.Close()
	unwindDomains.SetStateCacheForTest(stateCache)

	events := shards.NewEvents()
	events.PublishOverlay(unwindDomains)
	rpcCache := &execmodule.Cache{}
	rpcCache.SetPublishedSD(events.LatestSD)
	rpcView, err := rpcCache.View(ctx, rpcTx)
	require.NoError(t, err)

	unwindDomains.Unwind(10, &diffs)
	require.NoError(t, unwindDomains.Commit(ctx, unwindTx))
	events.PublishOverlay(nil)

	got, err := rpcView.Get(key)
	require.NoError(t, err)
	require.Equal(t, v2, got, "the pre-reorg RPC view still sees the discarded fork")

	_, ok := stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "the pre-reorg RPC view must not refill the discarded fork")

	freshTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer freshTx.Rollback()
	freshDomains, err := execctx.NewSharedDomains(ctx, freshTx, log.New())
	require.NoError(t, err)
	defer freshDomains.Close()
	freshDomains.SetStateCacheForTest(stateCache)

	got, _, err = freshDomains.GetLatest(kv.AccountsDomain, freshTx, key)
	require.NoError(t, err)
	require.Equal(t, v1, got)
}

func TestEmbeddedRPCTxBoundAfterUnwindDoesNotRefillUnwoundAccount(t *testing.T) {
	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	key, v1, v2, diffs := twoStepRows(t, db, stateCache)

	rpcTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer rpcTx.Rollback()

	unwindTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer unwindTx.Rollback()
	unwindDomains, err := execctx.NewSharedDomains(ctx, unwindTx, log.New())
	require.NoError(t, err)
	unwindDomains.SetStateCacheForTest(stateCache)
	unwindDomains.Unwind(10, &diffs)
	require.NoError(t, unwindDomains.Commit(ctx, unwindTx))
	unwindDomains.Close()

	freshTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer freshTx.Rollback()
	freshDomains, err := execctx.NewSharedDomains(ctx, freshTx, log.New())
	require.NoError(t, err)
	defer freshDomains.Close()
	freshDomains.SetStateCacheForTest(stateCache)

	events := shards.NewEvents()
	events.PublishOverlay(freshDomains)
	rpcCache := &execmodule.Cache{}
	rpcCache.SetPublishedSD(events.LatestSD)
	rpcView, err := rpcCache.View(ctx, rpcTx)
	require.NoError(t, err)

	got, err := rpcView.Get(key)
	require.NoError(t, err)
	require.Equal(t, v2, got, "the old RPC transaction still sees the discarded fork")

	_, ok := stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "binding an old RPC transaction after unwind must not refill the discarded fork")

	got, _, err = freshDomains.GetLatest(kv.AccountsDomain, freshTx, key)
	require.NoError(t, err)
	require.Equal(t, v1, got)
}

func TestSharedDomainsOldTxBoundAfterUnwindDoesNotRefillUnwoundAccount(t *testing.T) {
	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	key, v1, v2, diffs := twoStepRows(t, db, stateCache)

	oldTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer oldTx.Rollback()

	unwindTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer unwindTx.Rollback()
	unwindDomains, err := execctx.NewSharedDomains(ctx, unwindTx, log.New())
	require.NoError(t, err)
	unwindDomains.SetStateCacheForTest(stateCache)
	unwindDomains.Unwind(10, &diffs)
	require.NoError(t, unwindDomains.Commit(ctx, unwindTx))
	unwindDomains.Close()

	freshTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer freshTx.Rollback()
	freshDomains, err := execctx.NewSharedDomains(ctx, freshTx, log.New())
	require.NoError(t, err)
	defer freshDomains.Close()
	freshDomains.SetStateCacheForTest(stateCache)

	got, _, err := freshDomains.GetLatest(kv.AccountsDomain, oldTx, key)
	require.NoError(t, err)
	require.Equal(t, v2, got, "the old transaction still sees the discarded fork")

	_, ok := stateCache.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "binding an old transaction on a cache miss must not refill the discarded fork")

	got, _, err = freshDomains.GetLatest(kv.AccountsDomain, freshTx, key)
	require.NoError(t, err)
	require.Equal(t, v1, got)
}

func TestAccountOnlyDeleteDoesNotBlockUnrelatedCodeFill(t *testing.T) {
	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)

	contractAddr := make([]byte, 20)
	contractAddr[0] = 0xaa
	deletedAddr := make([]byte, 20)
	deletedAddr[0] = 0xbb
	code := []byte{0xcc, 1, 2, 3}
	account := accounts.SerialiseV3(&accounts.Account{
		Nonce:    1,
		Balance:  *uint256.NewInt(1),
		CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code)),
	})
	codelessAccount := accounts.SerialiseV3(&accounts.Account{
		Nonce:   1,
		Balance: *uint256.NewInt(1),
	})

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedDomains, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	defer seedDomains.Close()
	seedDomains.SetTxNum(10)
	require.NoError(t, seedDomains.DomainPut(kv.AccountsDomain, seedTx, contractAddr, account, 10, nil))
	require.NoError(t, seedDomains.DomainPut(kv.CodeDomain, seedTx, contractAddr, code, 10, nil))
	require.NoError(t, seedDomains.DomainPut(kv.AccountsDomain, seedTx, deletedAddr, codelessAccount, 10, nil))
	require.NoError(t, seedDomains.Commit(ctx, seedTx))
	seedDomains.Close()

	budget := 1 * datasize.MB
	stateCache := cache.NewStateCache(budget, budget, budget, budget)
	t.Cleanup(stateCache.Close)

	deleteTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer deleteTx.Rollback()
	deleteDomains, err := execctx.NewSharedDomains(ctx, deleteTx, log.New())
	require.NoError(t, err)
	defer deleteDomains.Close()
	deleteDomains.SetStateCacheForTest(stateCache)
	deleteDomains.SetTxNum(20)
	require.NoError(t, deleteDomains.DomainDel(kv.AccountsDomain, deleteTx, deletedAddr, 20, nil))
	require.NoError(t, deleteDomains.Commit(ctx, deleteTx))
	deleteDomains.Close()

	freshTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer freshTx.Rollback()
	codeEnd, ok := freshTx.Debug().DomainVisibleEnd(kv.CodeDomain)
	require.True(t, ok)
	accountsEnd, ok := freshTx.Debug().DomainVisibleEnd(kv.AccountsDomain)
	require.True(t, ok)
	require.Less(t, codeEnd, accountsEnd)

	freshDomains, err := execctx.NewSharedDomains(ctx, freshTx, log.New())
	require.NoError(t, err)
	defer freshDomains.Close()
	freshDomains.SetStateCacheForTest(stateCache)
	got, _, err := freshDomains.GetLatest(kv.CodeDomain, freshTx, contractAddr)
	require.NoError(t, err)
	require.Equal(t, code, got)

	cached, ok := stateCache.View(nil).Get(kv.CodeDomain, contractAddr)
	require.True(t, ok, "an account-only deletion must not block unrelated code fills")
	require.Equal(t, code, cached)
}

func TestSharedDomainsNegativeCacheEntryUsesLastVisibleTxNum(t *testing.T) {
	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)

	presentKey := make([]byte, 20)
	presentKey[0] = 0xaa
	missingKey := make([]byte, 20)
	missingKey[0] = 0xbb
	account := accounts.SerialiseV3(&accounts.Account{
		Nonce:   1,
		Balance: *uint256.NewInt(1),
	})

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedDomains, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	defer seedDomains.Close()
	seedDomains.SetTxNum(10)
	require.NoError(t, seedDomains.DomainPut(kv.AccountsDomain, seedTx, presentKey, account, 10, nil))
	require.NoError(t, seedDomains.Commit(ctx, seedTx))
	seedDomains.Close()

	budget := 1 * datasize.MB
	stateCache := cache.NewStateCache(budget, budget, budget, budget)
	t.Cleanup(stateCache.Close)

	readTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer readTx.Rollback()
	visibleEnd, ok := readTx.Debug().DomainVisibleEnd(kv.AccountsDomain)
	require.True(t, ok)
	require.NotZero(t, visibleEnd)

	readDomains, err := execctx.NewSharedDomains(ctx, readTx, log.New())
	require.NoError(t, err)
	defer readDomains.Close()
	readDomains.SetStateCacheForTest(stateCache)
	got, _, err := readDomains.GetLatest(kv.AccountsDomain, readTx, missingKey)
	require.NoError(t, err)
	require.Empty(t, got)

	cached, ok := stateCache.View(nil).Get(kv.AccountsDomain, missingKey)
	require.True(t, ok)
	require.Empty(t, cached)

	stateCache.Applier().Unwind(visibleEnd)
	_, ok = stateCache.View(nil).Get(kv.AccountsDomain, missingKey)
	require.True(t, ok, "a negative observed before the unwind floor must remain cached")

	stateCache.Applier().Unwind(visibleEnd - 1)
	_, ok = stateCache.View(nil).Get(kv.AccountsDomain, missingKey)
	require.False(t, ok, "a negative observed at the unwind floor must be invalidated")
}

func testEmbeddedRPCCacheViewDoesNotResurrectDeletedValue(t *testing.T, domain kv.Domain) {
	t.Helper()
	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)

	budget := 1 * datasize.MB
	stateCache := cache.NewStateCache(budget, budget, budget, budget)
	t.Cleanup(stateCache.Close)

	keyLen := 20
	if domain == kv.StorageDomain {
		keyLen = 52
	}
	key := make([]byte, keyLen)
	key[0] = 0xab
	value := accounts.SerialiseV3(&accounts.Account{
		Nonce:   1,
		Balance: *uint256.NewInt(1),
	})
	switch domain {
	case kv.StorageDomain:
		value = []byte{0x01}
	case kv.CodeDomain:
		value = []byte{0xaa, 1, 2, 3}
	}

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedDomains, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	defer seedDomains.Close()
	seedDomains.SetStateCacheForTest(stateCache)
	seedDomains.SetTxNum(10)
	require.NoError(t, seedDomains.DomainPut(domain, seedTx, key, value, 10, nil))
	require.NoError(t, seedDomains.Commit(ctx, seedTx))
	seedDomains.Close()

	rpcTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer rpcTx.Rollback()

	deleteTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer deleteTx.Rollback()
	deleteDomains, err := execctx.NewSharedDomains(ctx, deleteTx, log.New())
	require.NoError(t, err)
	defer deleteDomains.Close()
	deleteDomains.SetStateCacheForTest(stateCache)
	deleteDomains.SetTxNum(20)
	require.NoError(t, deleteDomains.DomainDel(domain, deleteTx, key, 20, value))

	events := shards.NewEvents()
	events.PublishOverlay(deleteDomains)
	rpcCache := &execmodule.Cache{}
	rpcCache.SetPublishedSD(events.LatestSD)
	rpcView, err := rpcCache.View(ctx, rpcTx)
	require.NoError(t, err)

	require.NoError(t, deleteDomains.Commit(ctx, deleteTx))
	events.PublishOverlay(nil)
	// The view outlives the overlay teardown on purpose: a production RPC view
	// built during a background commit keeps reading after PublishOverlay(nil)
	// and the SD's Close.
	deleteDomains.Close()

	oldValue, _, err := rpcTx.GetLatest(domain, key)
	require.NoError(t, err)
	require.Equal(t, value, oldValue)

	freshTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer freshTx.Rollback()
	freshValue, _, err := freshTx.GetLatest(domain, key)
	require.NoError(t, err)
	require.Empty(t, freshValue)

	if domain == kv.CodeDomain {
		_, err = rpcView.GetCode(key)
	} else {
		_, err = rpcView.Get(key)
	}
	require.NoError(t, err)

	freshDomains, err := execctx.NewSharedDomains(ctx, freshTx, log.New())
	require.NoError(t, err)
	defer freshDomains.Close()
	freshDomains.SetStateCacheForTest(stateCache)
	got, _, err := freshDomains.GetLatest(domain, freshTx, key)
	require.NoError(t, err)
	require.Empty(t, got, "the old RPC read view must not repopulate the shared cache after the deletion")
}

// The account-deletion mirror of TestEmbeddedRPCCacheViewDoesNotResurrectDeletedCode.
// DomainDel(AccountsDomain) cascades a code-domain delete at the SD layer, so the
// commit applies it and the code frontier advances past every pre-deletion view —
// and the cache-level code-fill admission also checks the accounts frontier. This
// pins both layers: losing either must not let a pre-deletion RPC view refill the
// deleted account's code.
func TestEmbeddedRPCCacheViewDoesNotRefillCodeOfDeletedAccount(t *testing.T) {
	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)

	budget := 1 * datasize.MB
	stateCache := cache.NewStateCache(budget, budget, budget, budget)
	t.Cleanup(stateCache.Close)

	addr := make([]byte, 20)
	addr[0] = 0xab
	code := []byte{0xaa, 1, 2, 3}
	account := accounts.SerialiseV3(&accounts.Account{
		Nonce:    1,
		Balance:  *uint256.NewInt(1),
		CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code)),
	})

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedDomains, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	defer seedDomains.Close()
	seedDomains.SetStateCacheForTest(stateCache)
	seedDomains.SetTxNum(10)
	require.NoError(t, seedDomains.DomainPut(kv.AccountsDomain, seedTx, addr, account, 10, nil))
	require.NoError(t, seedDomains.DomainPut(kv.CodeDomain, seedTx, addr, code, 10, nil))
	require.NoError(t, seedDomains.Commit(ctx, seedTx))
	seedDomains.Close()

	rpcTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer rpcTx.Rollback()

	deleteTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer deleteTx.Rollback()
	deleteDomains, err := execctx.NewSharedDomains(ctx, deleteTx, log.New())
	require.NoError(t, err)
	defer deleteDomains.Close()
	deleteDomains.SetStateCacheForTest(stateCache)
	deleteDomains.SetTxNum(20)
	require.NoError(t, deleteDomains.DomainDel(kv.AccountsDomain, deleteTx, addr, 20, account))

	events := shards.NewEvents()
	events.PublishOverlay(deleteDomains)
	rpcCache := &execmodule.Cache{}
	rpcCache.SetPublishedSD(events.LatestSD)
	rpcView, err := rpcCache.View(ctx, rpcTx)
	require.NoError(t, err)

	require.NoError(t, deleteDomains.Commit(ctx, deleteTx))
	events.PublishOverlay(nil)
	// The view outlives the overlay teardown on purpose, as above.
	deleteDomains.Close()

	_, ok := stateCache.View(nil).Get(kv.CodeDomain, addr)
	require.False(t, ok, "the account deletion must drop the cached code entry")

	got, err := rpcView.GetCode(addr)
	require.NoError(t, err)
	require.Empty(t, got, "the view keeps serving the published SD's state after teardown, so the deletion stays visible")

	_, ok = stateCache.View(nil).Get(kv.CodeDomain, addr)
	require.False(t, ok, "a pre-deletion RPC view must not refill the deleted account's code")
}
