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
