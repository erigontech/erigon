package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Pins that an in-batch account write overrides a stale addr→codeHash LRU entry.
// The LRU caches committed state and is invalidated when the account update is
// published.
func TestCodeHashForAddr_InBatchAccountWinsOverStaleLRU(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	sc := cache.NewDefaultStateCache()
	sd.SetStateCacheForTest(sc) // force-enable regardless of USE_STATE_CACHE

	var addr common.Address
	addr[0] = 0xab

	// Seed the committed-state LRU with a non-empty (stale) codeHash, as if the
	// account were a committed 7702 designator / contract the batch is overwriting.
	var stale common.Hash
	for i := range stale {
		stale[i] = 0x11
	}
	var staleArr [32]byte
	copy(staleArr[:], stale[:])
	currentStateCacheView(t, db, sc).SeedAddrCodeHash(addr[:], staleArr)

	t.Run("empty in-batch account wins (codeHash-no-code repro)", func(t *testing.T) {
		acc := accounts.Account{Nonce: 7, CodeHash: accounts.EmptyCodeHash}
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr[:], accounts.SerialiseV3(&acc), 0, nil))

		got := sd.CodeHashForAddr(rwTx, addr[:], 0)
		require.Nil(t, got, "in-batch empty-code account must override the stale non-empty LRU entry")
	})

	t.Run("non-empty in-batch account wins", func(t *testing.T) {
		var freshHash common.Hash
		for i := range freshHash {
			freshHash[i] = 0x22
		}
		acc := accounts.Account{Nonce: 8, CodeHash: accounts.InternCodeHash(freshHash)}
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr[:], accounts.SerialiseV3(&acc), 1, nil))

		got := sd.CodeHashForAddr(rwTx, addr[:], 0)
		require.Equal(t, freshHash[:], got, "in-batch account's codeHash must override the stale LRU entry")
		require.NotEqual(t, stale[:], got)
	})
}

// A generation-bound account-cache hit can safely seed the derived mapping:
// publication revokes the view before changing either cache layer.
func TestCodeHashForAddr_CacheSourcedRecordSeedsMapping(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)
	sc := cache.NewDefaultStateCache()
	t.Cleanup(sc.Close)

	var addr common.Address
	addr[0] = 0xab
	var codeHash common.Hash
	for i := range codeHash {
		codeHash[i] = 0x11
	}
	acc := accounts.Account{Nonce: 7, CodeHash: accounts.InternCodeHash(codeHash)}

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedSD, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	defer seedSD.Close()
	seedSD.SetStateCacheForTest(sc)
	seedSD.SetTxNum(10)
	require.NoError(t, seedSD.DomainPut(kv.AccountsDomain, seedTx, addr[:], accounts.SerialiseV3(&acc), 10, nil))
	require.NoError(t, seedSD.Commit(ctx, seedTx))
	seedSD.Close()

	_, ok := currentStateCacheView(t, db, sc).Get(kv.AccountsDomain, addr[:])
	require.True(t, ok, "the committed record must be served by the accounts cache")
	_, ok = currentStateCacheView(t, db, sc).GetAddrCodeHash(addr[:])
	require.False(t, ok, "the post-commit apply must leave the derived mapping empty")

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetStateCacheForTest(sc)

	got := sd.CodeHashForAddr(roTx, addr[:], 20)
	require.Equal(t, codeHash[:], got)
	h, ok := currentStateCacheView(t, db, sc).GetAddrCodeHash(addr[:])
	require.True(t, ok)
	require.Equal(t, [32]byte(codeHash), h)
}

// A record read from the transaction's exact generation can safely seed the
// derived addr→codeHash mapping.
func TestCodeHashForAddr_ViewSourcedRecordSeedsMapping(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)

	var addr common.Address
	addr[0] = 0xcd
	var codeHash common.Hash
	for i := range codeHash {
		codeHash[i] = 0x22
	}
	acc := accounts.Account{Nonce: 3, CodeHash: accounts.InternCodeHash(codeHash)}

	// Seed without a state cache so the record lands in the DB only.
	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedSD, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	defer seedSD.Close()
	seedSD.SetTxNum(10)
	require.NoError(t, seedSD.DomainPut(kv.AccountsDomain, seedTx, addr[:], accounts.SerialiseV3(&acc), 10, nil))
	require.NoError(t, seedSD.Commit(ctx, seedTx))
	seedSD.Close()

	sc := cache.NewDefaultStateCache()
	t.Cleanup(sc.Close)

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetStateCacheForTest(sc)

	got := sd.CodeHashForAddr(roTx, addr[:], 20)
	require.Equal(t, codeHash[:], got)
	h, ok := currentStateCacheView(t, db, sc).GetAddrCodeHash(addr[:])
	require.True(t, ok, "a view-sourced record must seed the mapping")
	require.Equal(t, [32]byte(codeHash), h)
}
