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

package txpool

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

// TestOnAddSubscriberThatStopsReadingOverTCP exercises OnAdd over a real
// gRPC connection, where a client that stops calling Recv drains its HTTP/2
// flow-control window and wedges the server-side Send.
func TestOnAddSubscriberThatStopsReadingOverTCP(t *testing.T) {
	ctx := t.Context()
	logger := log.New()

	newSlotsStreams := &NewSlotsStreams{}
	grpcServer := NewGrpcServer(ctx, nil, nil, newSlotsStreams, *uint256.NewInt(1), logger)
	srv := grpc.NewServer()
	txpoolproto.RegisterTxpoolServer(srv, grpcServer)
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go srv.Serve(listener) //nolint:errcheck // stopped below
	t.Cleanup(srv.Stop)

	// Separate connections, so that connection-level flow control on the
	// stalled client cannot account for anything the healthy one sees.
	subscribe := func() txpoolproto.Txpool_OnAddClient {
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		stream, err := txpoolproto.NewTxpoolClient(conn).OnAdd(ctx, &txpoolproto.OnAddRequest{})
		require.NoError(t, err)
		return stream
	}

	// A subscriber is registered only once its handler runs on the server, so
	// probe until the client sees a message.
	awaitRegistered := func(stream txpoolproto.Txpool_OnAddClient) {
		t.Helper()
		probing := make(chan struct{})
		go func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-probing:
					return
				case <-ticker.C:
					newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{}, logger)
				}
			}
		}()
		defer close(probing)
		_, err := stream.Recv()
		require.NoError(t, err)
	}

	stalled := subscribe()
	awaitRegistered(stalled)
	healthy := subscribe()
	awaitRegistered(healthy)

	// stalled is never read again from here on. healthy keeps reading, so its
	// own queue stays empty and only the stalled one can hold the broadcaster up.
	received := make(chan *txpoolproto.OnAddReply, 1024)
	go func() {
		defer close(received)
		for {
			reply, err := healthy.Recv()
			if err != nil {
				return
			}
			received <- reply
		}
	}()

	const (
		messages    = 40
		payloadSize = 256 * 1024
	)
	marker := []byte("last")
	for i := range messages {
		payload := make([]byte, payloadSize)
		if i == messages-1 {
			copy(payload, marker)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{RplTxs: [][]byte{payload}}, logger)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("Broadcast %d blocked on the subscriber that stopped reading", i)
		}
	}

	timeout := time.After(30 * time.Second)
	for {
		select {
		case reply, ok := <-received:
			require.True(t, ok, "healthy subscriber's stream ended before the marker message")
			if len(reply.RplTxs) == 1 && bytes.Equal(reply.RplTxs[0][:len(marker)], marker) {
				return
			}
		case <-timeout:
			t.Fatal("healthy subscriber never received the marker message")
		}
	}
}

// A handler wedged in Send is released when the server tears its transport
// down, so a subscriber that stopped reading cannot outlive the server.
func TestStalledOnAddSubscriberIsReleasedWhenTheServerStops(t *testing.T) {
	ctx := t.Context()
	logger := log.New()

	handlerDone := make(chan struct{})
	newSlotsStreams := &NewSlotsStreams{}
	srv := grpc.NewServer(grpc.StreamInterceptor(
		func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			defer close(handlerDone)
			return handler(srv, ss)
		}))
	txpoolproto.RegisterTxpoolServer(srv, NewGrpcServer(ctx, nil, nil, newSlotsStreams, *uint256.NewInt(1), logger))
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go srv.Serve(listener) //nolint:errcheck // stopped below

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	stalled, err := txpoolproto.NewTxpoolClient(conn).OnAdd(ctx, &txpoolproto.OnAddRequest{})
	require.NoError(t, err)

	probing := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-probing:
				return
			case <-ticker.C:
				newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{}, logger)
			}
		}
	}()
	_, err = stalled.Recv()
	require.NoError(t, err)
	close(probing)

	// stalled is never read again: wedge its handler inside Send.
	for range 40 {
		newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{RplTxs: [][]byte{make([]byte, 256*1024)}}, logger)
	}

	srv.Stop()
	select {
	case <-handlerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the wedged OnAdd handler was not released when the server stopped")
	}
}
