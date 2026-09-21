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
	"encoding/json"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestEthSubscribe(t *testing.T) {
	ctx := t.Context()
	logger := log.New()
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(7, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	backendServer := privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications, m.BlockReader, logger, builder.NewLatestBlockBuiltStore(), nil)
	backendClient := direct.NewEthBackendClientDirect(backendServer)
	backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
	// Creating a new filter will set up new internal subscription channels actively managed by subscription tasks.
	// We must wait for the first NEW_SNAPSHOT notification, which is always sent unconditionally by EthBackendServer
	// at the start of Subscribe, to be sure that the subscription is ready, otherwise we could miss some events.
	subscriptionReadyWg := sync.WaitGroup{}
	subscriptionReadyWg.Add(1)
	// Only the first NEW_SNAPSHOT signals readiness; background block retirement emits more.
	onNewSnapshot := sync.OnceFunc(subscriptionReadyWg.Done)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, backend, nil, nil, onNewSnapshot, m.Log, nil)
	subscriptionReadyWg.Wait() // This is needed *before* inserting the blocks, which sends NEW_HEADER events
	newHeads, id := ff.SubscribeNewHeads(16, "")
	defer ff.UnsubscribeHeads(id)
	highestSeenHeader := chain.TopBlock.NumberU64()
	err = m.InsertChain(chain)
	require.NoError(t, err)
	for i := uint64(1); i <= highestSeenHeader; i++ {
		header := <-newHeads
		require.Equal(t, i, header.Value.Number.Uint64())
	}
}

func TestEthSubscribeReceipts(t *testing.T) {
	ctx := t.Context()
	logger := log.New()
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(3, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		tx, err := types.SignTx(types.NewTransaction(uint64(i), m.Address, uint256.NewInt(1), params.TxGas, uint256.NewInt(1), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	backendServer := privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications, m.BlockReader, logger, builder.NewLatestBlockBuiltStore(), nil)
	backendClient := direct.NewEthBackendClientDirect(backendServer)
	backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
	subscriptionReadyWg := sync.WaitGroup{}
	subscriptionReadyWg.Add(1)
	// Only the first NEW_SNAPSHOT signals readiness; background block retirement emits more.
	onNewSnapshot := sync.OnceFunc(subscriptionReadyWg.Done)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, backend, nil, nil, onNewSnapshot, m.Log, nil)
	subscriptionReadyWg.Wait()
	newReceipts, id, _ := ff.SubscribeReceipts(16, filters.ReceiptsFilterCriteria{
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
		receipts[i-1] = (<-newReceipts).Value
	}
	slices.SortFunc(receipts, func(a, b *remoteproto.SubscribeReceiptsReply) int {
		return cmp.Compare(a.BlockNumber, b.BlockNumber)
	})
	for i := uint64(1); i <= highestSeenHeader; i++ {
		require.Equal(t, i, receipts[i-1].BlockNumber)
	}
}

// sharedJSON reaches rpc through interfaces that package keeps unexported; these mirror them, so a
// pointer receiver or a renamed method fails here instead of sending {} to every subscriber.
var (
	_ interface{ MarshalFastJSON() ([]byte, error) } = sharedJSON[*types.Header]{}
	_ interface{ LocalValue() any }                  = sharedJSON[*types.Header]{}
)

func TestSharedJSONEncodesTheValue(t *testing.T) {
	h := &types.Header{Number: *uint256.NewInt(7)}
	s := sharedJSON[*types.Header]{&rpchelper.Shared[*types.Header]{Value: h}, headerValue}
	got, err := s.MarshalFastJSON()
	require.NoError(t, err)
	want, err := json.Marshal(h)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
	require.Same(t, h, s.LocalValue())
}

// newHeads through the rpc package's notifier, as a websocket client receives it.
func TestEthSubscribeNewHeadsOverWebsocket(t *testing.T) {
	m := execmoduletester.New(t)
	ff := rpchelper.New(t.Context(), rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, nil)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, kvcache.New(kvcache.DefaultCoherentConfig), m), m.DB, nil, nil)

	server := rpc.NewServer(50, false, false, true, m.Log, 100)
	require.NoError(t, server.RegisterName("eth", api))
	defer server.Stop()
	httpsrv := httptest.NewServer(server.WebsocketHandler([]string{"*"}, nil, false, m.Log))
	defer httpsrv.Close()
	client, err := rpc.DialWebsocket(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), "", m.Log)
	require.NoError(t, err)
	defer client.Close()

	ch := make(chan json.RawMessage, 1)
	sub, err := client.EthSubscribe(t.Context(), ch, "newHeads")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	header := &types.Header{Number: *uint256.NewInt(7)}
	payload, err := rlp.EncodeToBytes(header)
	require.NoError(t, err)
	ff.OnNewEvent(&remoteproto.SubscribeReply{Type: remoteproto.Event_HEADER, Data: payload})

	want, err := json.Marshal(header)
	require.NoError(t, err)
	select {
	case got := <-ch:
		require.Equal(t, string(want), string(got))
	case err := <-sub.Err():
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("no newHeads notification")
	}
}
