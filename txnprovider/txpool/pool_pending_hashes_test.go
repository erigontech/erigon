package txpool

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestPendingHashesListsEachSendersTransactionsInNonceOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	pool, err := New(ctx, make(chan Announcements, 100), mdbxtest.NewTestPoolDB(t), coreDB, txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig), chain.AllProtocolChanges, nil, nil, func() {}, nil, nil, log.New())
	require.NoError(t, err)
	pool.started.Store(true)

	senders := [][20]byte{{1}, {2}, {3}}
	change := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 200000,
		BlockGasLimit:       10000000,
		ChangeBatch:         []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: gointerfaces.ConvertHashToH256([32]byte{})}},
	}
	for _, addr := range senders {
		acc := accounts.Account{Nonce: 2, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash, Incarnation: 1}
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
			Action: remoteproto.Action_UPSERT, Address: gointerfaces.ConvertAddressToH160(addr), Data: accounts.SerialiseV3(&acc),
		})
	}
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	nonceOf := map[[32]byte]uint64{}
	senderOf := map[[32]byte]int{}
	add := func(s int, nonce, tip uint64) {
		var slots TxnSlots
		slot := newTestTxnSlot(nonce, 0, tip, 300000, 100000)
		slot.IDHash[0], slot.IDHash[1] = byte(s+1), byte(nonce)
		slots.Append(slot, senders[s][:], true)
		reasons, err := pool.AddLocalTxns(ctx, slots)
		require.NoError(t, err)
		require.Equal(t, txpoolcfg.Success, reasons[0], reasons[0].String())
		nonceOf[slot.IDHash], senderOf[slot.IDHash] = nonce, s
	}
	for _, nonce := range []uint64{2, 3, 4, 5} {
		add(0, nonce, 100000+nonce*1000)
	}
	add(1, 2, 150000)
	for _, nonce := range []uint64{2, 3, 4} {
		add(2, nonce, 300000-nonce*1000)
	}
	add(1, 3, 290000)

	hashes, ok := pool.PendingHashes(1 << 20)
	require.True(t, ok)
	require.Len(t, hashes, len(nonceOf))
	last := map[int]uint64{}
	for i, h := range hashes {
		s, n := senderOf[h], nonceOf[h]
		if prev, seen := last[s]; seen {
			require.Greater(t, n, prev, "position %d: sender %d nonce %d listed after nonce %d", i, s, n, prev)
		}
		last[s] = n
	}
}
