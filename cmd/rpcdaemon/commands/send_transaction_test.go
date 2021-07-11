package commands_test

import (
	"bytes"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

func TestSendRawTransaction(t *testing.T) {
	t.Skip("Flaky test")
	m, require := stages.Mock(t), require.New(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	require.NoError(err)
	{ // Do 1 step to start txPool

		// Send NewBlock message
		b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
			Block: chain.TopBlock,
			TD:    big.NewInt(1), // This is ignored anyway
		})
		require.NoError(err)
		m.ReceiveWg.Add(1)
		for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
			require.NoError(err)
		}
		// Send all the headers
		b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
			RequestId:          1,
			BlockHeadersPacket: chain.Headers,
		})
		require.NoError(err)
		m.ReceiveWg.Add(1)
		for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
			require.NoError(err)
		}
		m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

		initialCycle := true
		highestSeenHeader := chain.TopBlock.NumberU64()
		if err := stages.StageLoopStep(m.Ctx, m.DB, m.Sync, highestSeenHeader, m.Notifications, initialCycle, m.UpdateHead, nil); err != nil {
			t.Fatal(err)
		}
	}

	expectValue := uint64(1234)
	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectValue), params.TxGas, u256.Num1, nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(err)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpool.NewTxpoolClient(conn)
	ff := filters.New(ctx, nil, txPool, txpool.NewMiningClient(conn))
	api := commands.NewEthAPI(commands.NewBaseApi(ff), m.DB, nil, txPool, nil, 5000000)

	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)

	txsCh := make(chan []types.Transaction, 1)
	defer close(txsCh)
	id := ff.SubscribePendingTxs(txsCh)
	defer ff.UnsubscribePendingTxs(id)

	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)

	got := <-txsCh
	require.Equal(expectValue, got[0].GetValue().Uint64())

	//send same tx second time and expect error
	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.NotNil(err)
	require.Equal("ALREADY_EXISTS: already known", err.Error())
	m.ReceiveWg.Wait()

	//TODO: make propagation easy to test - now race
	//time.Sleep(time.Second)
	//sent := m.SentMessage(0)
	//require.Equal(eth.ToProto[m.SentryClient.Protocol()][eth.NewPooledTransactionHashesMsg], sent.Id)
}

func transaction(nonce uint64, gaslimit uint64, key *ecdsa.PrivateKey) types.Transaction {
	return pricedTransaction(nonce, gaslimit, u256.Num1, key)
}

func pricedTransaction(nonce uint64, gaslimit uint64, gasprice *uint256.Int, key *ecdsa.PrivateKey) types.Transaction {
	tx, _ := types.SignTx(types.NewTransaction(nonce, common.Address{}, uint256.NewInt(100), gaslimit, gasprice, nil), *types.LatestSignerForChainID(big.NewInt(1337)), key)
	return tx
}
