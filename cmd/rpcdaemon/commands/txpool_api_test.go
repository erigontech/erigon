package commands

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

func TestTxPoolContent(t *testing.T) {
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

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpool.NewTxpoolClient(conn)
	ff := filters.New(ctx, nil, txPool, txpool.NewMiningClient(conn))
	api := NewTxPoolAPI(NewBaseApi(ff, kvcache.New(kvcache.DefaultCoherentConfig), false), m.DB, txPool)

	expectValue := uint64(1234)
	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectValue), params.TxGas, u256.Num1, nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(err)

	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)

	_, err = txPool.Add(ctx, &txpool.AddRequest{RlpTxs: [][]byte{buf.Bytes()}})
	require.NoError(err)
	content, err := api.Content(ctx)
	require.NoError(err)

	sender := m.Address.String()
	require.Equal(1, len(content["pending"][sender]))
	require.Equal(expectValue, content["pending"][sender]["0"].Value.ToInt().Uint64())

	status, err := api.Status(ctx)
	require.NoError(err)
	require.Len(status, 3)
	require.Equal(status["pending"], hexutil.Uint(1))
	require.Equal(status["queued"], hexutil.Uint(0))
}
