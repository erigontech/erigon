package jsonrpc

import (
	"context"
	"fmt"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
)

func TestEthSubscribe(t *testing.T) {
	m, require := stages.Mock(t), require.New(t)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 7, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	require.NoError(err)

	b, err := rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(err)

	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	ctx := context.Background()
	logger := log.New()
	backendServer := privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications.Events, m.BlockReader, logger, builder.NewLatestBlockBuiltStore())
	backendClient := direct.NewEthBackendClientDirect(backendServer)
	backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
	ff := rpchelper.New(ctx, backend, nil, nil, func() {}, m.Log)

	newHeads, id := ff.SubscribeNewHeads(16)
	defer ff.UnsubscribeHeads(id)

	initialCycle := stages.MockInsertAsInitialCycle
	highestSeenHeader := chain.TopBlock.NumberU64()

	hook := stages.NewHook(m.Ctx, m.Notifications, m.Sync, m.BlockReader, m.ChainConfig, m.Log, m.UpdateHead)
	if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, logger, m.BlockReader, hook); err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= highestSeenHeader; i++ {
		header := <-newHeads
		fmt.Printf("Got header %d\n", header.Number.Uint64())
		require.Equal(i, header.Number.Uint64())
	}
}
