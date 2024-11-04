package txpool

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/temporaltest"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNonceFromAddress(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan types.Announcements, 100)
	_, coreDB, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	defer coreDB.Close()

	db := memdb.NewTestPoolDB(t)
	path := fmt.Sprintf("/tmp/db-test-%v", time.Now().UTC().Format(time.RFC3339Nano))
	txPoolDB := newTestTxPoolDB(t, path)
	defer txPoolDB.Close()
	aclsDB := newTestACLDB(t, path)
	defer aclsDB.Close()

	// Check if the dbs are created.
	require.NotNil(t, db)
	require.NotNil(t, txPoolDB)
	require.NotNil(t, aclsDB)

	cfg := txpoolcfg.DefaultConfig
	ethCfg := &ethconfig.Defaults
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, ethCfg, sendersCache, *u256.N1, nil, nil, aclsDB)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})

	// Create address for testing.
	var addr [20]byte
	addr[0] = 1

	// Fund addr with 18 Ether for sending transactions.
	v := make([]byte, types.EncodeSenderLengthForStorage(2, *uint256.NewInt(18 * common.Ether)))
	types.EncodeSender(2, *uint256.NewInt(18 * common.Ether), v)

	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, types.TxSlots{}, types.TxSlots{}, tx)
	assert.NoError(err)

	{
		var txSlots types.TxSlots
		txSlot1 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot1.IDHash[0] = 1
		txSlots.Append(txSlot1, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}

		// Add remote transactions, and check it processes.
		pool.AddRemoteTxs(ctx, txSlots)
		err = pool.processRemoteTxs(ctx)
		assert.NoError(err)

	}

	// Test sending normal transactions with expected nonces.
	{
		txSlots := types.TxSlots{}
		txSlot2 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  4,
		}
		txSlot2.IDHash[0] = 2
		txSlot3 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  6,
		}
		txSlot3.IDHash[0] = 3
		txSlots.Append(txSlot2, addr[:], true)
		txSlots.Append(txSlot3, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}

		// Test NonceFromAddress function to check if the address' nonce is being properly tracked.
		nonce, _ := pool.NonceFromAddress(addr)
		// CDK Erigon will return 0, Upstream Erigon will return latest nonce including txns in the queued pool.
		assert.Equal(uint64(0), nonce)
	}

	// Test sending transactions without having enough balance for it.
	{
		var txSlots types.TxSlots
		txSlot1 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(9 * common.Ether),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot1.IDHash[0] = 4
		txSlots.Append(txSlot1, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(InsufficientFunds, reason, reason.String())
		}
	}

	// Test sending transactions with too low nonce.
	{
		var txSlots types.TxSlots
		txSlot1 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  1,
		}
		txSlot1.IDHash[0] = 5
		txSlots.Append(txSlot1, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(NonceTooLow, reason, reason.String())
		}
	}
}
