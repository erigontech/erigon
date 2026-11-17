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

package service

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

func TestSentinelUnsupportedRPCs(t *testing.T) {
	server := NewSentinelServer(t.Context(), nil, log.New())
	for _, transport := range []string{"grpc", "direct"} {
		t.Run(transport, func(t *testing.T) {
			var client sentinelproto.SentinelClient
			if transport == "grpc" {
				client = newTestSentinelClient(t, server)
			} else {
				client = direct.NewSentinelClientDirect(server)
			}
			t.Run("PublishGossip", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				_, err := client.PublishGossip(ctx, &sentinelproto.GossipData{})
				require.Equal(t, codes.Unimplemented, status.Code(err))
			})
			t.Run("SubscribeGossip", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				stream, err := client.SubscribeGossip(ctx, &sentinelproto.SubscriptionData{})
				require.NoError(t, err)
				_, err = stream.Recv()
				require.Equal(t, codes.Unimplemented, status.Code(err))
			})
			t.Run("SetSubscribeExpiry", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				_, err := client.SetSubscribeExpiry(ctx, &sentinelproto.RequestSubscribeExpiry{})
				require.Equal(t, codes.Unimplemented, status.Code(err))
			})
		})
	}
}

func TestSentinelGRPCRecoversPanics(t *testing.T) {
	client := newTestSentinelClient(t, &panickingSentinelServer{})
	for _, rpcType := range []string{"unary", "stream"} {
		t.Run(rpcType, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if rpcType == "unary" {
				_, err := client.PublishGossip(ctx, &sentinelproto.GossipData{})
				require.Equal(t, codes.Unknown, status.Code(err))
			} else {
				stream, err := client.SubscribeGossip(ctx, &sentinelproto.SubscriptionData{})
				require.NoError(t, err)
				_, err = stream.Recv()
				require.Equal(t, codes.Unknown, status.Code(err))
			}
			_, err := client.GetPeers(ctx, &sentinelproto.EmptyMessage{})
			require.NoError(t, err)
		})
	}
}

type panickingSentinelServer struct {
	sentinelproto.UnimplementedSentinelServer
}

func (*panickingSentinelServer) PublishGossip(context.Context, *sentinelproto.GossipData) (*sentinelproto.EmptyMessage, error) {
	panic("unary panic")
}

func (*panickingSentinelServer) SubscribeGossip(*sentinelproto.SubscriptionData, sentinelproto.Sentinel_SubscribeGossipServer) error {
	panic("stream panic")
}

func (*panickingSentinelServer) GetPeers(context.Context, *sentinelproto.EmptyMessage) (*sentinelproto.PeerCount, error) {
	return &sentinelproto.PeerCount{}, nil
}

func newTestSentinelClient(t *testing.T, server sentinelproto.SentinelServer) sentinelproto.SentinelClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := newGRPCServer(server, nil)
	t.Cleanup(grpcServer.Stop)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	conn, err := grpc.NewClient("passthrough:///sentinel",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return sentinelproto.NewSentinelClient(conn)
}
