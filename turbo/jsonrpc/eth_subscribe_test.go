package jsonrpc

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/wrap"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

func sendBlock(t *testing.T, require *require.Assertions, m *mock.MockSentry, chain *core.ChainPack) {
	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: chain.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestEthSubscribe(t *testing.T) {
	m, require := mock.Mock(t), require.New(t)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 7, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	require.NoError(err)

	sendBlock(t, require, m, chain)

	ctx := context.Background()
	logger := log.New()
	backendServer := privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications.Events, m.BlockReader, logger, builder.NewLatestBlockBuiltStore())
	backendClient := direct.NewEthBackendClientDirect(backendServer)
	backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
	ff := rpchelper.New(ctx, backend, nil, nil, func() {}, m.Log)

	newHeads, id := ff.SubscribeNewHeads(16)
	defer ff.UnsubscribeHeads(id)

	initialCycle := mock.MockInsertAsInitialCycle
	highestSeenHeader := chain.TopBlock.NumberU64()

	hook := stages.NewHook(m.Ctx, m.DB, m.Notifications, m.Sync, m.BlockReader, m.ChainConfig, m.Log, nil)
	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.TxContainer{}, m.Sync, initialCycle, logger, m.BlockReader, hook, false); err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= highestSeenHeader; i++ {
		header := <-newHeads
		require.Equal(i, header.Number.Uint64())
		require.Equal(chain.Blocks[i-1].Hash(), header.Hash())
	}

	// create reorg chain starting with common ancestor of 3, 4 will be first block with different coinbase
	m2 := mock.Mock(t)
	chain, err = core.GenerateChain(m2.ChainConfig, m2.Genesis, m2.Engine, m2.DB, 9, func(i int, b *core.BlockGen) {
		// i starts from 0, so this means everything under block 4 will have coinbase 1, and 4 and above will have coinbase 2
		if i < 3 {
			b.SetCoinbase(libcommon.Address{1})
		} else {
			b.SetCoinbase(libcommon.Address{2})
		}
	})
	require.NoError(err)

	sendBlock(t, require, m, chain)

	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.TxContainer{}, m.Sync, initialCycle, logger, m.BlockReader, hook, false); err != nil {
		t.Fatal(err)
	}

	highestSeenHeader = chain.TopBlock.NumberU64()

	// since common ancestor of reorg is 3 the first new header we will see should be 3
	for i := uint64(3); i <= highestSeenHeader; i++ {
		header := <-newHeads
		require.Equal(i, header.Number.Uint64())
		require.Equal(chain.Blocks[i-1].Hash(), header.Hash())
	}
}
