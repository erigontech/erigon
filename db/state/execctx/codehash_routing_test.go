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

// TestCodeHashForAddr_InBatchAccountWinsOverStaleLRU is the deterministic
// regression for the codeHash-no-code corruption: the addr→codeHash LRU caches
// committed state and is invalidated only at flush, so an in-batch account
// write (a 7702 set/clear, a fresh EOA) it has not yet seen must override it.
// Before the fix, codeHashForAddr consulted the LRU before sd.mem and returned
// a codeHash stale relative to the uncommitted write — a non-empty codeHash
// beside an empty (mem-routed) code read, which surfaces on re-exec as EIP-3607
// "sender not an eoa".
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
	sd.SetStateCache(sc)
	if !sd.HasStateCache() {
		t.Skip("state cache disabled (USE_STATE_CACHE=false); routing test needs it on")
	}

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
	sc.PutAddrCodeHash(addr[:], staleArr, 0)

	t.Run("empty in-batch account wins (codeHash-no-code repro)", func(t *testing.T) {
		acc := accounts.Account{Nonce: 7, CodeHash: accounts.EmptyCodeHash}
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr[:], accounts.SerialiseV3(&acc), 0, nil))

		got := sd.CodeHashForAddr(rwTx, addr[:])
		require.Nil(t, got, "in-batch empty-code account must override the stale non-empty LRU entry")
	})

	t.Run("non-empty in-batch account wins", func(t *testing.T) {
		var freshHash common.Hash
		for i := range freshHash {
			freshHash[i] = 0x22
		}
		acc := accounts.Account{Nonce: 8, CodeHash: accounts.InternCodeHash(freshHash)}
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr[:], accounts.SerialiseV3(&acc), 1, nil))

		got := sd.CodeHashForAddr(rwTx, addr[:])
		require.Equal(t, freshHash[:], got, "in-batch account's codeHash must override the stale LRU entry")
		require.NotEqual(t, stale[:], got)
	})
}
