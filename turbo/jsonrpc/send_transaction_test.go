package jsonrpc_test

import (
	"bytes"
	"crypto/ecdsa"
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/gointerfaces/sentry"
	"github.com/erigontech/erigon-lib/gointerfaces/txpool"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common/u256"

	txpool_proto "github.com/erigontech/erigon-lib/gointerfaces/txpool"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/jsonrpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/stages"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func newBaseApiForTest(m *mock.MockSentry) *jsonrpc.BaseAPI {
	agg := m.HistoryV3Components()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return jsonrpc.NewBaseApi(nil, stateCache, m.BlockReader, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs)
}

// Do 1 step to start txPool
func oneBlockStep(mockSentry *mock.MockSentry, require *require.Assertions, t *testing.T) {
	chain, err := core.GenerateChain(mockSentry.ChainConfig, mockSentry.Genesis, mockSentry.Engine, mockSentry.DB, 1 /*number of blocks:*/, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)

	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: chain.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(err)

	mockSentry.ReceiveWg.Add(1)
	for _, err = range mockSentry.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: mockSentry.PeerId}) {
		require.NoError(err)
	}
	// Send all the headers
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(err)
	mockSentry.ReceiveWg.Add(1)
	for _, err = range mockSentry.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: mockSentry.PeerId}) {
		require.NoError(err)
	}
	mockSentry.ReceiveWg.Wait() // Wait for all messages to be processed before we proceed

	initialCycle := mock.MockInsertAsInitialCycle
	if err := stages.StageLoopIteration(mockSentry.Ctx, mockSentry.DB, wrap.TxContainer{}, mockSentry.Sync, initialCycle, log.New(), mockSentry.BlockReader, nil, false); err != nil {
		t.Fatal(err)
	}
}

func TestSendRawTransaction(t *testing.T) {
	mockSentry, require := mock.MockWithTxPool(t), require.New(t)
	logger := log.New()

	oneBlockStep(mockSentry, require, t)

	expectedValue := uint64(1234)
	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectedValue), params.TxGas, uint256.NewInt(10*params.GWei), nil), *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpool.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := jsonrpc.NewEthAPI(newBaseApiForTest(mockSentry), mockSentry.DB, nil, txPool, nil, 5000000, 1e18, 100_000, false, 100_000, 128, logger)

	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)

	txsCh, id := ff.SubscribePendingTxs(1)
	defer ff.UnsubscribePendingTxs(id)

	txHash, err := api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)

	select {
	case got := <-txsCh:
		require.Equal(expectedValue, got[0].GetValue().Uint64())
	case <-time.After(20 * time.Second): // Sometimes the channel times out on github actions
		t.Log("Timeout waiting for txn from channel")
		jsonTx, err := api.GetTransactionByHash(ctx, txHash)
		require.NoError(err)
		require.Equal(expectedValue, jsonTx.Value.Uint64())
	}

	//send same tx second time and expect error
	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.NotNil(err)
	expectedErr := txpool_proto.ImportResult_name[int32(txpool_proto.ImportResult_ALREADY_EXISTS)] + ": " + txpoolcfg.AlreadyKnown.String()
	require.Equal(expectedErr, err.Error())
	mockSentry.ReceiveWg.Wait()

	//TODO: make propagation easy to test - now race
	//time.Sleep(time.Second)
	//sent := m.SentMessage(0)
	//require.Equal(eth.ToProto[m.MultiClient.Protocol()][eth.NewPooledTransactionHashesMsg], sent.Id)
}

func TestSendRawTransactionUnprotected(t *testing.T) {
	mockSentry, require := mock.MockWithTxPool(t), require.New(t)
	logger := log.New()

	oneBlockStep(mockSentry, require, t)

	expectedTxValue := uint64(4444)

	// Create a legacy signer pre-155
	unprotectedSigner := types.MakeFrontierSigner()

	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectedTxValue), params.TxGas, uint256.NewInt(10*params.GWei), nil), *unprotectedSigner, mockSentry.Key)
	require.NoError(err)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpool.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := jsonrpc.NewEthAPI(newBaseApiForTest(mockSentry), mockSentry.DB, nil, txPool, nil, 5000000, 1e18, 100_000, false, 100_000, 128, logger)

	// Enable unproteced txs flag
	api.AllowUnprotectedTxs = true

	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)

	txsCh, id := ff.SubscribePendingTxs(1)
	defer ff.UnsubscribePendingTxs(id)

	txHash, err := api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)

	select {
	case got := <-txsCh:
		require.Equal(expectedTxValue, got[0].GetValue().Uint64())
	case <-time.After(20 * time.Second): // Sometimes the channel times out on github actions
		t.Log("Timeout waiting for txn from channel")
		jsonTx, err := api.GetTransactionByHash(ctx, txHash)
		require.NoError(err)
		require.Equal(expectedTxValue, jsonTx.Value.Uint64())
	}
}

func transaction(nonce uint64, gaslimit uint64, key *ecdsa.PrivateKey) types.Transaction {
	return pricedTransaction(nonce, gaslimit, u256.Num1, key)
}

func pricedTransaction(nonce uint64, gaslimit uint64, gasprice *uint256.Int, key *ecdsa.PrivateKey) types.Transaction {
	tx, _ := types.SignTx(types.NewTransaction(nonce, common.Address{}, uint256.NewInt(100), gaslimit, gasprice, nil), *types.LatestSignerForChainID(big.NewInt(1337)), key)
	return tx
}
