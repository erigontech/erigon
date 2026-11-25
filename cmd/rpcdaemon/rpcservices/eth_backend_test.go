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
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/privateapi"
)

// gatedAckServer applies each filter request only when released, then acks it if the
// request asks. One send on release releases one request.
type gatedAckServer struct {
	remoteproto.UnimplementedETHBACKENDServer
	release    chan struct{}
	version    *typesproto.VersionReply
	versionErr error
}

func newGatedAckServer() gatedAckServer {
	return gatedAckServer{release: make(chan struct{}), version: privateapi.EthBackendAPIVersion}
}

func (s gatedAckServer) Version(context.Context, *emptypb.Empty) (*typesproto.VersionReply, error) {
	return s.version, s.versionErr
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

		require.ErrorContains(t, send(&remoteproto.LogsFilterRequest{}), "no ack of the filter update")
		srv.release <- struct{}{} // the late ack of the timed-out update
		requireSendWaitsForAck(t, srv, func() error { return send(&remoteproto.LogsFilterRequest{AllAddresses: true}) })
	})
}

// Acks are opt-in on the wire, so an erigon and an rpcdaemon from either side of the change still connect.
func TestFilterAckKeepsTheBackendVersionCompatible(t *testing.T) {
	current := gointerfaces.VersionFromProto(privateapi.EthBackendAPIVersion)
	before := &typesproto.VersionReply{Major: 5, Minor: 0, Patch: 0}
	require.True(t, gointerfaces.EnsureVersion(current, before), "a new rpcdaemon must accept an erigon without acks")
	require.True(t, gointerfaces.EnsureVersion(gointerfaces.VersionFromProto(before), privateapi.EthBackendAPIVersion), "an old rpcdaemon must accept a new erigon")
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

// A stream that can not learn the server's version ends, so the reconnect asks again, instead of
// sending updates that never wait for the server.
func TestFilterStreamEndsWhenTheVersionCallFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := newGatedAckServer()
		srv.versionErr = errors.New("version unavailable")
		back := NewRemoteBackend(direct.NewEthBackendClientDirect(srv), nil, nil)
		var requestor atomic.Value
		done := make(chan error, 1)
		go func() { done <- back.SubscribeLogs(t.Context(), func(*remoteproto.SubscribeLogsReply) {}, &requestor) }()
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorContains(t, err, "version unavailable")
		default:
			t.Fatal("the stream kept running without knowing whether the server acks")
		}
		require.Nil(t, requestor.Load())
	})
}

// Each connect asks the server's version again: a reconnect to an erigon that acks waits for acks.
func TestFilterStreamReadsTheVersionOnEachConnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := newGatedAckServer()
		version := &typesproto.VersionReply{Major: 5}
		srv.version = version
		back := NewRemoteBackend(direct.NewEthBackendClientDirect(srv), nil, nil)
		connect := func() (send func(*remoteproto.LogsFilterRequest) error, end func()) {
			ctx, cancel := context.WithCancel(t.Context())
			var requestor atomic.Value
			go back.SubscribeLogs(ctx, func(*remoteproto.SubscribeLogsReply) {}, &requestor) //nolint:errcheck
			synctest.Wait()
			return requestor.Load().(func(*remoteproto.LogsFilterRequest) error), cancel
		}
		send, end := connect()
		req := &remoteproto.LogsFilterRequest{AllAddresses: true}
		require.NoError(t, send(req))
		require.False(t, req.Ack)
		end()

		*version = typesproto.VersionReply{Major: 5, Patch: 1}
		send, end = connect()
		defer end()
		req = &remoteproto.LogsFilterRequest{AllAddresses: true}
		done := make(chan error, 1)
		go func() { done <- send(req) }()
		synctest.Wait()
		require.True(t, req.Ack, "a reconnect to an erigon that acks asks for one")
		srv.release <- struct{}{}
		require.NoError(t, <-done)
	})
}
