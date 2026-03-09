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
	"cmp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestEthSubscribe(t *testing.T) {
	ctx := t.Context()
	logger := log.New()
	m := execmoduletester.New(t)
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 7, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
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
	subscriptionReadyWg.Wait() // This is needed *before* inserting the blocks, which sends NEW_HEADER events
	newHeads, id := ff.SubscribeNewHeads(16)
	defer ff.UnsubscribeHeads(id)
	highestSeenHeader := chain.TopBlock.NumberU64()
	err = m.InsertChain(chain)
	require.NoError(t, err)
	for i := uint64(1); i <= highestSeenHeader; i++ {
		header := <-newHeads
		require.Equal(t, i, header.Number.Uint64())
	}
}

func TestEthSubscribeReceipts(t *testing.T) {
	ctx := t.Context()
	logger := log.New()
	m := execmoduletester.New(t)
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		tx, err := types.SignTx(types.NewTransaction(uint64(i), m.Address, uint256.NewInt(1), params.TxGas, uint256.NewInt(1), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	backendServer := privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications, m.BlockReader, nil, logger, builder.NewLatestBlockBuiltStore(), nil)
	backendClient := direct.NewEthBackendClientDirect(backendServer)
	backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
	subscriptionReadyWg := sync.WaitGroup{}
	subscriptionReadyWg.Add(1)
	onNewSnapshot := func() {
		subscriptionReadyWg.Done()
	}
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, backend, nil, nil, onNewSnapshot, m.Log)
	subscriptionReadyWg.Wait()
	newReceipts, id := ff.SubscribeReceipts(16, filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{},
	})
	defer ff.UnsubscribeReceipts(id)
	// Wait for the server-side receipt filter to be fully activated before inserting
	// blocks. SubscribeReceipts sends a filter update request through a channel which
	// is processed asynchronously by the server goroutine. Without this wait,
	// InsertChain may execute and call NotifyReceipts before HasReceiptSubscriptions
	// returns true, causing all receipt notifications to be silently dropped.
	require.Eventually(t, func() bool {
		return m.Notifications.Events.HasReceiptSubscriptions()
	}, 5*time.Second, time.Millisecond)
	err = m.InsertChain(chain)
	require.NoError(t, err)
	highestSeenHeader := chain.TopBlock.NumberU64()
	receipts := make([]*remoteproto.SubscribeReceiptsReply, highestSeenHeader)
	for i := uint64(1); i <= highestSeenHeader; i++ {
		// 1 tx per block -> 1 receipt per block
		receipts[i-1] = <-newReceipts
	}
	slices.SortFunc(receipts, func(a, b *remoteproto.SubscribeReceiptsReply) int {
		return cmp.Compare(a.BlockNumber, b.BlockNumber)
	})
	for i := uint64(1); i <= highestSeenHeader; i++ {
		require.Equal(t, i, receipts[i-1].BlockNumber)
	}
}
