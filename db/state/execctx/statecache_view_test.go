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
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

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
	defer unwindDomains.Close()
	unwindDomains.BindStateCache(stateCache)
	unwindDomains.Unwind(10, &diffs)
	require.NoError(t, unwindDomains.Commit(ctx, unwindTx))
	unwindDomains.Close()

	freshTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer freshTx.Rollback()
	freshDomains, err := execctx.NewSharedDomains(ctx, freshTx, log.New())
	require.NoError(t, err)
	defer freshDomains.Close()
	freshDomains.BindStateCache(stateCache)

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
	deleteDomains.BindStateCache(stateCache)
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
	freshDomains.BindStateCache(stateCache)
	got, _, err := freshDomains.GetLatest(kv.CodeDomain, freshTx, contractAddr)
	require.NoError(t, err)
	require.Equal(t, code, got)

	cached, ok := stateCache.View(nil).Get(kv.CodeDomain, contractAddr)
	require.True(t, ok, "an account-only deletion must not block unrelated code fills")
	require.Equal(t, code, cached)
}

func TestGetCodeSizeColdReadDoesNotCacheCode(t *testing.T) {
	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)
	addr := make([]byte, 20)
	addr[0] = 0xaa
	code := []byte{0xcc, 1, 2, 3}
	codeHash := crypto.Keccak256Hash(code)
	account := accounts.SerialiseV3(&accounts.Account{
		Nonce:    1,
		Balance:  *uint256.NewInt(1),
		CodeHash: accounts.InternCodeHash(codeHash),
	})

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedDomains, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	seedDomains.SetTxNum(10)
	require.NoError(t, seedDomains.DomainPut(kv.AccountsDomain, seedTx, addr, account, 10, nil))
	require.NoError(t, seedDomains.DomainPut(kv.CodeDomain, seedTx, addr, code, 10, nil))
	require.NoError(t, seedDomains.Commit(ctx, seedTx))
	seedDomains.Close()

	budget := 1 * datasize.MB
	stateCache := cache.NewStateCache(budget, budget, budget, budget)
	t.Cleanup(stateCache.Close)
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	domains.BindStateCache(stateCache)

	size, found, err := domains.GetCodeSize(roTx, addr, 20)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, len(code), size)
	_, found = stateCache.View(nil).Get(kv.CodeDomain, addr)
	require.False(t, found)
	cachedSize, found := stateCache.View(nil).GetCodeSizeByHash(codeHash[:])
	require.True(t, found)
	require.Equal(t, len(code), cachedSize)
}
