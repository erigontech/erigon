package jsonrpc_test

import (
	"bytes"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common/u256"

	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/ledgerwatch/log/v3"
)

func newBaseApiForTest(m *mock.MockSentry) *jsonrpc.BaseAPI {
	agg := m.HistoryV3Components()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return jsonrpc.NewBaseApi(nil, stateCache, m.BlockReader, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs)
}

func TestSendRawTransaction(t *testing.T) {
	mockSentry, require := mock.MockWithTxPool(t), require.New(t)
	logger := log.New()

	chain, err := core.GenerateChain(mockSentry.ChainConfig, mockSentry.Genesis, mockSentry.Engine, mockSentry.DB, 1 /*number of blocks:*/, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	{ // Do 1 step to start txPool

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
		if err := stages.StageLoopIteration(mockSentry.Ctx, mockSentry.DB, nil, mockSentry.Sync, initialCycle, logger, mockSentry.BlockReader, nil, false); err != nil {
			t.Fatal(err)
		}
	}

	expectValue := uint64(1234)
	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectValue), params.TxGas, uint256.NewInt(10*params.GWei), nil), *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, nil, txPool, txpool.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := jsonrpc.NewEthAPI(newBaseApiForTest(mockSentry), mockSentry.DB, nil, txPool, nil, 5000000, 100_000, false, logger)

	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)

	txsCh, id := ff.SubscribePendingTxs(1)
	defer ff.UnsubscribePendingTxs(id)

	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)

	got := <-txsCh
	require.Equal(expectValue, got[0].GetValue().Uint64())

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
	expectedTxValue := uint64(4444)

	// Create a legacy signer pre-155
	unprotectedSigner := types.MakeFrontierSigner()

	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectedTxValue), params.TxGas, uint256.NewInt(10*params.GWei), nil), *unprotectedSigner, mockSentry.Key)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, nil, txPool, txpool.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := jsonrpc.NewEthAPI(newBaseApiForTest(mockSentry), mockSentry.DB, nil, txPool, nil, 5000000, 100_000, false, logger)

	// Enable unproteced txs flag
	api.AllowUnprotectedTxs = true

	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)

	txsCh, id := ff.SubscribePendingTxs(1)
	defer ff.UnsubscribePendingTxs(id)

	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)

	got := <-txsCh
	require.Equal(expectedTxValue, got[0].GetValue().Uint64())
}

func transaction(nonce uint64, gaslimit uint64, key *ecdsa.PrivateKey) types.Transaction {
	return pricedTransaction(nonce, gaslimit, u256.Num1, key)
}

func pricedTransaction(nonce uint64, gaslimit uint64, gasprice *uint256.Int, key *ecdsa.PrivateKey) types.Transaction {
	tx, _ := types.SignTx(types.NewTransaction(nonce, common.Address{}, uint256.NewInt(100), gaslimit, gasprice, nil), *types.LatestSignerForChainID(big.NewInt(1337)), key)
	return tx
}
