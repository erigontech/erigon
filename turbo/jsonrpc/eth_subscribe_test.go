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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/direct"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/wrap"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/ethdb/privateapi"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/builder"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/stages"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func TestEthSubscribe(t *testing.T) {
	m, require := mock.Mock(t), require.New(t)
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
	backendServer := privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications, m.BlockReader, logger, builder.NewLatestBlockBuiltStore())
	backendClient := direct.NewEthBackendClientDirect(backendServer)
	backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, backend, nil, nil, func() {}, m.Log)

	newHeads, id := ff.SubscribeNewHeads(16)
	defer ff.UnsubscribeHeads(id)

	initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
	highestSeenHeader := chain.TopBlock.NumberU64()

	hook := stages.NewHook(m.Ctx, m.DB, m.Notifications, m.Sync, m.BlockReader, m.ChainConfig, m.Log, nil)
	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.TxContainer{}, m.Sync, initialCycle, firstCycle, logger, m.BlockReader, hook); err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= highestSeenHeader; i++ {
		header := <-newHeads
		fmt.Printf("Got header %d\n", header.Number.Uint64())
		require.Equal(i, header.Number.Uint64())
	}
}
