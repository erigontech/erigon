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

// Pins that an in-batch account write overrides a stale addr→codeHash LRU entry
// (the LRU caches committed state and is invalidated only at flush).
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
	sc.PutAddrCodeHashIfFresh(addr[:], staleArr, 0, 0)

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

// The addr→codeHash admission gate vouches for the tx snapshot's frontier, but
// resolve() may serve the account record from the shared accounts cache, which
// lags a just-committed flush until the apply loop reaches the key. A
// cache-sourced record must therefore never seed the mapping — an apply
// interleaved between the read and the fill would leave a mapping derived from
// the pre-apply record.
func TestCodeHashForAddr_CacheSourcedRecordDoesNotSeedMapping(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
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

	_, ok := sc.Get(kv.AccountsDomain, addr[:])
	require.True(t, ok, "the committed record must be served by the accounts cache")
	_, ok = sc.GetAddrCodeHash(addr[:])
	require.False(t, ok, "the flush apply must leave the derived mapping empty")

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetStateCacheForTest(sc)

	got := sd.CodeHashForAddr(roTx, addr[:], 20)
	require.Equal(t, codeHash[:], got)
	_, ok = sc.GetAddrCodeHash(addr[:])
	require.False(t, ok, "a cache-sourced account record must not seed the addr→codeHash mapping")
}

// A record read from the tx snapshot (accounts-cache miss) is exactly what the
// admission gate vouches for, so it still seeds the mapping.
func TestCodeHashForAddr_SnapshotSourcedRecordSeedsMapping(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
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
	h, ok := sc.GetAddrCodeHash(addr[:])
	require.True(t, ok, "a snapshot-sourced record must seed the mapping")
	require.Equal(t, [32]byte(codeHash), h)
}
