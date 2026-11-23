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

package rpcservices

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/privateapi"
)

// gatedAckServer applies each filter request only when released, then acks it if the
// request asks. One send on release releases one request.
type gatedAckServer struct {
	remoteproto.UnimplementedETHBACKENDServer
	release chan struct{}
	version *typesproto.VersionReply
}

func newGatedAckServer() gatedAckServer {
	return gatedAckServer{release: make(chan struct{}), version: privateapi.EthBackendAPIVersion}
}

func (s gatedAckServer) Version(context.Context, *emptypb.Empty) (*typesproto.VersionReply, error) {
	return s.version, nil
}

func (s gatedAckServer) SubscribeLogs(st grpc.BidiStreamingServer[remoteproto.LogsFilterRequest, remoteproto.SubscribeLogsReply]) error {
	for {
		req, err := st.Recv()
		if err != nil {
			return err
		}
		if !req.Ack {
			continue
		}
		<-s.release
		if err := st.Send(&remoteproto.SubscribeLogsReply{FilterApplied: true}); err != nil {
			return err
		}
	}
}

func (s gatedAckServer) SubscribeReceipts(st grpc.BidiStreamingServer[remoteproto.ReceiptsFilterRequest, remoteproto.SubscribeReceiptsReply]) error {
	for {
		req, err := st.Recv()
		if err != nil {
			return err
		}
		if !req.Ack {
			continue
		}
		<-s.release
		if err := st.Send(&remoteproto.SubscribeReceiptsReply{FilterApplied: true}); err != nil {
			return err
		}
	}
}

// requireSendWaitsForAck checks that send returns only after the server applied the request.
func requireSendWaitsForAck(t *testing.T, srv gatedAckServer, send func() error) {
	var sent atomic.Bool
	go func() {
		assert.NoError(t, send())
		sent.Store(true)
	}()
	synctest.Wait()
	assert.False(t, sent.Load(), "the filter update returned before the server applied it")
	srv.release <- struct{}{}
	synctest.Wait()
	require.True(t, sent.Load())
}

func TestSubscribeLogsUpdateWaitsForFilterApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		srv := newGatedAckServer()
		back := NewRemoteBackend(direct.NewEthBackendClientDirect(srv), nil, nil)
		var requestor atomic.Value
		var delivered atomic.Int32
		go back.SubscribeLogs(ctx, func(*remoteproto.SubscribeLogsReply) { delivered.Add(1) }, &requestor) //nolint:errcheck
		synctest.Wait()
		send := requestor.Load().(func(*remoteproto.LogsFilterRequest) error)
		requireSendWaitsForAck(t, srv, func() error { return send(&remoteproto.LogsFilterRequest{AllAddresses: true}) })
		require.Zero(t, delivered.Load(), "an ack is not a log")
	})
}

func TestSubscribeReceiptsUpdateWaitsForFilterApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		srv := newGatedAckServer()
		back := NewRemoteBackend(direct.NewEthBackendClientDirect(srv), nil, nil)
		ready := make(chan func(*remoteproto.ReceiptsFilterRequest) error, 1)
		var delivered atomic.Int32
		go back.SubscribeReceipts(ctx, func(*remoteproto.SubscribeReceiptsReply) { delivered.Add(1) }, func(send func(*remoteproto.ReceiptsFilterRequest) error) { //nolint:errcheck
			ready <- send
		})
		send := <-ready
		requireSendWaitsForAck(t, srv, func() error { return send(&remoteproto.ReceiptsFilterRequest{AllTransactions: true}) })
		require.Zero(t, delivered.Load(), "an ack is not a receipt")
	})
}

func TestFilterUpdateIgnoresLateAckOfTimedOutUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		srv := newGatedAckServer()
		back := NewRemoteBackend(direct.NewEthBackendClientDirect(srv), nil, nil)
		var requestor atomic.Value
		go back.SubscribeLogs(ctx, func(*remoteproto.SubscribeLogsReply) {}, &requestor) //nolint:errcheck
		synctest.Wait()
		send := requestor.Load().(func(*remoteproto.LogsFilterRequest) error)

		require.ErrorContains(t, send(&remoteproto.LogsFilterRequest{}), "did not apply")
		srv.release <- struct{}{} // the late ack of the timed-out update
		requireSendWaitsForAck(t, srv, func() error { return send(&remoteproto.LogsFilterRequest{AllAddresses: true}) })
	})
}

// A stream can reconnect to an erigon that predates acks; its updates must not wait.
func TestFilterUpdateDoesNotWaitOnServerWithoutAcks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		srv := newGatedAckServer()
		srv.version = &typesproto.VersionReply{Major: 5, Minor: 0}
		back := NewRemoteBackend(direct.NewEthBackendClientDirect(srv), nil, nil)
		var requestor atomic.Value
		go back.SubscribeLogs(ctx, func(*remoteproto.SubscribeLogsReply) {}, &requestor) //nolint:errcheck
		synctest.Wait()
		send := requestor.Load().(func(*remoteproto.LogsFilterRequest) error)
		req := &remoteproto.LogsFilterRequest{AllAddresses: true}
		require.NoError(t, send(req))
		require.False(t, req.Ack, "a server without acks must not be asked for one")
	})
}
