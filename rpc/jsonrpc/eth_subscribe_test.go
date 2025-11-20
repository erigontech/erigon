// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestEthSubscribe(t *testing.T) {
	m, require := mock.Mock(t), require.New(t)
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 7, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)

	b, err := rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(err)

	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceed

	ctx := context.Background()
	logger := log.New()
	backendServer := privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications, m.BlockReader, nil, logger, builder.NewLatestBlockBuiltStore(), nil)
	backendClient := direct.NewEthBackendClientDirect(backendServer)
	backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
	// Creating a new filter will set up new internal subscription channels actively managed by subscription tasks.
	// We must wait for the first NEW_SNAPSHOT notification, which is always sent unconditionally by EthBackendServer
	// at the start of Subscribe, to be sure that the subscription is ready, otherwise we could miss some events.
	subscriptionReadyWg := sync.WaitGroup{}
	subscriptionReadyWg.Add(1)
	onNewSnapshot := func() {
		subscriptionReadyWg.Done()
	}
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, backend, nil, nil, onNewSnapshot, m.Log)
	subscriptionReadyWg.Wait() // This is needed *before* stages.StageLoopIteration, which sends NEW_HEADER events

	newHeads, id := ff.SubscribeNewHeads(16)
	defer ff.UnsubscribeHeads(id)

	initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
	highestSeenHeader := chain.TopBlock.NumberU64()

	hook := stageloop.NewHook(m.Ctx, m.DB, m.Notifications, m.Sync, m.BlockReader, m.ChainConfig, m.Log, nil, nil, nil)
	if err := stageloop.StageLoopIteration(m.Ctx, m.DB, nil, nil, m.Sync, initialCycle, firstCycle, logger, m.BlockReader, hook); err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= highestSeenHeader; i++ {
		header := <-newHeads
		require.Equal(i, header.Number.Uint64())
	}
}
