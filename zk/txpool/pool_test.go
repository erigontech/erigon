package txpool

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"testing"
	"time"

	types2 "github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/zk/utils"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remote"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
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
	pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, nil, nil, ethCfg, aclsDB, nil)
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

func TestOnNewBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coreDB, db := memdb.NewTestDB(t), memdb.NewTestDB(t)
	ctrl := gomock.NewController(t)

	stream := remote.NewMockKV_StateChangesClient(ctrl)
	i := 0
	stream.EXPECT().
		Recv().
		DoAndReturn(func() (*remote.StateChangeBatch, error) {
			if i > 0 {
				return nil, io.EOF
			}
			i++
			return &remote.StateChangeBatch{
				StateVersionId: 1,
				ChangeBatch: []*remote.StateChange{
					{
						Txs: [][]byte{
							decodeHex(types.TxParseMainnetTests[0].PayloadStr),
							decodeHex(types.TxParseMainnetTests[1].PayloadStr),
							decodeHex(types.TxParseMainnetTests[2].PayloadStr),
						},
						BlockHeight: 1,
						BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
					},
				},
			}, nil
		}).
		AnyTimes()

	stateChanges := remote.NewMockKVClient(ctrl)
	stateChanges.
		EXPECT().
		StateChanges(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *remote.StateChangeRequest, _ ...grpc.CallOption) (remote.KV_StateChangesClient, error) {
			return stream, nil
		})

	pool := NewMockPool(ctrl)
	pool.EXPECT().
		ValidateSerializedTxn(gomock.Any()).
		DoAndReturn(func(_ []byte) error {
			return nil
		}).
		Times(3)

	var minedTxs types.TxSlots
	pool.EXPECT().
		OnNewBlock(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(
			func(
				_ context.Context,
				_ *remote.StateChangeBatch,
				_ types.TxSlots,
				minedTxsArg types.TxSlots,
				_ kv.Tx,
			) error {
				minedTxs = minedTxsArg
				return nil
			},
		).
		Times(1)

	fetch := NewFetch(ctx, nil, pool, stateChanges, coreDB, db, *u256.N1)
	err := fetch.handleStateChanges(ctx, stateChanges)
	assert.ErrorIs(t, io.EOF, err)
	assert.Equal(t, 3, len(minedTxs.Txs))
}

func TestUnmarshalPrioritySendersList(t *testing.T) {
	list, err := UnmarshalDynamicPriorityList("test-priority-list.json")
	require.NoError(t, err)
	assert.NotNil(t, list)
	assert.Equal(t, 3, len(list.Addresses()), "should have 3 addresses in the priority list")
	assert.Equal(t, list.GetPriority(common.HexToAddress("0x2581c81825C961199e84130E25442A9B3eafCB21")), Priority(3), "should have priority 3 for address 0x2581c81825C961199e84130E25442A9B3eafCB21")
	assert.Equal(t, list.GetPriority(common.HexToAddress("0xa974239DdC8cFb90eACd44470cC8889c6d997799")), Priority(2), "should have priority 2 for address 0xa974239DdC8cFb90eACd44470cC8889c6d997799")
	assert.Equal(t, list.GetPriority(common.HexToAddress("0x1a32F6e7Cf3062f1aB0D25EF0ba4Ac13593D60BA")), Priority(1), "should have priority 1 for address 0x1a32F6e7Cf3062f1aB0D25EF0ba4Ac13593D60BA")
}

func TestPriorityYieldBest(t *testing.T) {
	ctx := context.Background()
	addr1 := common.HexToAddress("0xA967E10A7f04cD54fb8419040BD400654b008777")
	addr2 := common.HexToAddress("0xa974239DdC8cFb90eACd44470cC8889c6d997799")
	addr3 := common.HexToAddress("0x784bCB8cb9AE8cdD1c1532185c02FDd25588643a")
	addr4 := common.HexToAddress("0x2581c81825C961199e84130E25442A9B3eafCB21")
	addr5 := common.HexToAddress("0xB7245c3D6D1c49F26b89Df890931495B9608ed17")
	addr6 := common.HexToAddress("0x1a32F6e7Cf3062f1aB0D25EF0ba4Ac13593D60BA")

	ch := make(chan types.Announcements, 100)
	_, coreDB, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	defer coreDB.Close()
	require.NotNil(t, coreDB)
	db := memdb.NewTestPoolDB(t)
	path := fmt.Sprintf("/tmp/db-test-%v", time.Now().UTC().Format(time.RFC3339Nano))
	txPoolDB := newTestTxPoolDB(t, path)
	defer txPoolDB.Close()
	aclDB := newTestACLDB(t, path)
	defer aclDB.Close()
	require.NotNil(t, db)
	require.NotNil(t, txPoolDB)
	require.NotNil(t, aclDB)
	priorityList, err := UnmarshalDynamicPriorityList("test-priority-list.json")
	require.NoError(t, err)
	assert.NotNil(t, priorityList)
	pool, err := New(ch, coreDB, txpoolcfg.DefaultConfig, kvcache.New(kvcache.DefaultCoherentConfig), uint256.Int(*u256.N1), nil, nil, nil, nil, nil, nil, &ethconfig.Defaults, aclDB, priorityList)
	assert.NoError(t, err)
	require.True(t, pool != nil)

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	v := make([]byte, types.EncodeSenderLengthForStorage(2, *uint256.NewInt(18 * common.Ether)))
	types.EncodeSender(2, *uint256.NewInt(18 * common.Ether), v)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})

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
		Address: gointerfaces.ConvertAddressToH160(addr1),
		Data:    v,
	})
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr2),
		Data:    v,
	})
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr3),
		Data:    v,
	})
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr4),
		Data:    v,
	})
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr5),
		Data:    v,
	})
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr6),
		Data:    v,
	})

	err = pool.OnNewBlock(ctx, change, types.TxSlots{}, types.TxSlots{}, tx)
	assert.NoError(t, err)

	parseCtx := types.NewTxParseContext(*uint256.NewInt(779)).ChainIDRequired()

	txRlps := [][]byte{
		// signed: 0xA967E10A7f04cD54fb8419040BD400654b008777 (No Priority)
		decodeHex("f86702842e3b19288252089451025f088a8e17718e03f2450a99dc495e2536c58227108082063aa0c2c6252efe24ca1b9cde5d5684812faa4f500b884fc1f89cd5898304bcd961c4a01df722403fa62a0efc63911317638bba8844571c8987eea33bdb5e8b5544661d"),
		// signed: 0xa974239DdC8cFb90eACd44470cC8889c6d997799 (Priority)
		decodeHex("f86702842e3b19288252089451025f088a8e17718e03f2450a99dc495e2536c58227108082063aa0757b0371b4e9dd8f158d776bcf1fa6d5ebb625d9a6a36f3d21aeaf9d30ca1942a05f81ce37c134c73cdf39df3988f04d3265c40cce30150ce910a1f1009920e9c7"),
		// signed: 0x784bCB8cb9AE8cdD1c1532185c02FDd25588643a (No Priority)
		decodeHex("f86702842e3b19288252089451025f088a8e17718e03f2450a99dc495e2536c582271080820639a0789dc60dc7f9bd1d082e06eb712a1b8ad7d1e1045ffae571b82c5f294926b6eba05df2304715fdf65a0e846bfe00a2133b0428b765e9c030ba3574d5db7a6ee448"),
		// signed: 0x2581c81825C961199e84130E25442A9B3eafCB21 (Priority)
		decodeHex("f86702842e3b19288252089451025f088a8e17718e03f2450a99dc495e2536c582271080820639a0385fb29260083b3ba56f3d85f64efa1200cbc106beb1ff1898cf70faee8fa76aa040e985552584672bbbdcd8da227e88cbd0f0b4627b0d2adfbf2086b568f6daea"),
		// signed: 0xB7245c3D6D1c49F26b89Df890931495B9608ed17 (No Priority)
		decodeHex("f86702842e3b19288252089451025f088a8e17718e03f2450a99dc495e2536c58227108082063aa031244c55c18ceb4d4773ceb88cf84eababd81b27d0b173d6352f3a983c445a9ba07c330b2cbfa8399a2eceab4186b2812e0ffb8cb86af4317319dc8b9af7270b59"),
		// signed: 0x1a32F6e7Cf3062f1aB0D25EF0ba4Ac13593D60BA (Priority)
		decodeHex("f86702842e3b192882520894a94f5374fce5edbc8e2a8697c15331677e6ebf0b82271080820639a04777a5be6cba3c48da288945a8de5ef836d0dd3d06f2460694df50cee733125da0763268a31fea68d85481b119bdbd587d15b0f39444ada7acd8e1bdc2cbeb2353"),
	}

	var slots types.TxSlots
	for i, rlp := range txRlps {
		txSlot := &types.TxSlot{}
		s := common.Address{}
		senderSlice := s[:]

		_, err = parseCtx.ParseTransaction(rlp, 0, txSlot, senderSlice, false, false, func(hash []byte) error {
			return nil
		})
		assert.NoError(t, err, "should parse transaction without error")

		slots.Resize(uint(i + 1))
		slots.Txs[i] = txSlot
		copy(slots.Senders.At(i), senderSlice)
		slots.IsLocal[i] = true
	}

	dReasons, err := pool.AddLocalTxs(ctx, slots, tx)
	assert.NoError(t, err)
	for _, reason := range dReasons {
		assert.Equal(t, Success, reason, reason.String())
	}

	yieldSlots := &types.TxsRlp{}
	yieldSize := uint16(4)
	allConditionsOk, _, err := pool.YieldBest(yieldSize, yieldSlots, tx, 0, utils.ForkId8BlockGasLimit, 0)
	assert.NoError(t, err)
	assert.True(t, allConditionsOk, "all conditions should be ok")
	assert.Equal(t, yieldSize, uint16(len(yieldSlots.Txs)), "should yield the expected number of transactions")

	// slot 1: addr4 (0x2581c81825C961199e84130E25442A9B3eafCB21: Priority 3)
	yield1 := yieldSlots.Txs[0]
	transaction1, err := types2.DecodeTransaction(yield1)
	assert.NoError(t, err, "should decode transaction without error")
	signer := types2.LatestSignerForChainID(big.NewInt(779))
	sdr1, err := transaction1.Sender(*signer)
	assert.NoError(t, err, "should get sender without error")
	assert.True(t, sdr1 == addr4, "should yield the transaction from the priority sender %s", addr4.Hex())

	// slot 2: addr2 (0xa974239DdC8cFb90eACd44470cC8889c6d997799: Priority 2)
	yield2 := yieldSlots.Txs[1]
	transaction2, err := types2.DecodeTransaction(yield2)
	assert.NoError(t, err, "should decode transaction without error")
	sdr2, err := transaction2.Sender(*signer)
	assert.NoError(t, err, "should get sender without error")
	assert.True(t, sdr2 == addr2, "should yield the transaction from the priority sender %s", addr2.Hex())

	// slot 3: addr6 (0x1a32F6e7Cf3062f1aB0D25EF0ba4Ac13593D60BA: Priority 1)
	yield3 := yieldSlots.Txs[2]
	transaction3, err := types2.DecodeTransaction(yield3)
	assert.NoError(t, err, "should decode transaction without error")
	sdr3, err := transaction3.Sender(*signer)
	assert.NoError(t, err, "should get sender without error")
	assert.True(t, sdr3 == addr6, "should yield the transaction from the priority sender %s", addr6.Hex())

	// slot 4: any addr (No Priority). We should still yield from the non-priority senders.
	yield4 := yieldSlots.Txs[3]
	transaction4, err := types2.DecodeTransaction(yield4)
	assert.NoError(t, err, "should decode transaction without error")
	sdr4, err := transaction4.Sender(*signer)
	assert.NoError(t, err, "should get sender without error")
	assert.True(t, sdr4 == addr1 || sdr4 == addr3 || sdr4 == addr5, "should yield the transaction from any address %s, %s or %s", addr1.Hex(), addr3.Hex(), addr5.Hex())
}
