// Copyright 2026 The Erigon Authors
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
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// plainEthSyncingService stands in for the regular (non-subscription)
// APIImpl.Syncing method that already lives under the "eth" namespace, so the
// test can prove the two coexist as daemon.go registers them.
type plainEthSyncingService struct{}

func (plainEthSyncingService) Syncing(_ context.Context) (any, error) {
	return false, nil
}

func syncingSubscribeReply(t *testing.T, reply *remoteproto.SyncingReply) *remoteproto.SubscribeReply {
	t.Helper()
	data, err := proto.Marshal(reply)
	require.NoError(t, err)
	return &remoteproto.SubscribeReply{Type: remoteproto.Event_SYNCING, Data: data}
}

// TestEthSubscribeSyncingOverWebsocket drives eth_subscribe("syncing") through
// a real websocket connection, the same transport wsClients use in production,
// and checks it coexists with the regular eth_syncing call under the same
// "eth" namespace exactly as daemon.go registers them.
func TestEthSubscribeSyncingOverWebsocket(t *testing.T) {
	logger := log.New()
	ff := rpchelper.New(t.Context(), rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, logger, nil)
	// The last state seen on the shared event stream seeds new subscribers.
	ff.OnNewEvent(syncingSubscribeReply(t, &remoteproto.SyncingReply{
		Syncing:          true,
		CurrentBlock:     100,
		LastNewBlockSeen: 200,
		Stages: []*remoteproto.SyncingReply_StageProgress{
			{StageName: "Headers", BlockNumber: 200},
		},
	}))

	server := rpc.NewServer(50, false, false, true, logger, 100)
	require.NoError(t, server.RegisterName("eth", plainEthSyncingService{}))
	require.NoError(t, server.RegisterName("eth", NewEthSyncingSubscriptionAPI(ff, logger)))
	defer server.Stop()

	httpsrv := httptest.NewServer(server.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()
	wsURL := "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")

	client, err := rpc.DialWebsocket(context.Background(), wsURL, "", logger)
	require.NoError(t, err)
	defer client.Close()

	// The plain eth_syncing call must still work alongside the subscription.
	var syncingResp any
	require.NoError(t, client.CallContext(context.Background(), &syncingResp, "eth_syncing"))
	require.Equal(t, false, syncingResp)

	ch := make(chan syncingResult, 4)
	sub, err := client.EthSubscribe(context.Background(), ch, "syncing")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// First notification: the seed, i.e. the last state seen on the stream.
	select {
	case result := <-ch:
		require.True(t, result.Syncing)
		require.EqualValues(t, 100, result.CurrentBlock)
		require.EqualValues(t, 200, result.HighestBlock)
		require.EqualValues(t, 100, result.StartingBlock)
		require.Len(t, result.Stages, 1)
		require.Equal(t, "Headers", result.Stages[0].StageName)
	case err := <-sub.Err():
		t.Fatalf("subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for initial syncing notification over websocket")
	}

	// Second notification: a SYNCING event pushed through the filters, as the
	// gRPC subscription delivers it in production.
	ff.OnNewEvent(syncingSubscribeReply(t, &remoteproto.SyncingReply{
		Syncing:          true,
		CurrentBlock:     150,
		LastNewBlockSeen: 210,
	}))

	select {
	case result := <-ch:
		require.True(t, result.Syncing)
		require.EqualValues(t, 150, result.CurrentBlock)
		require.EqualValues(t, 210, result.HighestBlock)
		require.EqualValues(t, 100, result.StartingBlock, "startingBlock must stay pinned to the sync session start")
	case err := <-sub.Err():
		t.Fatalf("subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for pushed syncing notification over websocket")
	}
}
