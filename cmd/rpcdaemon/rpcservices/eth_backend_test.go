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
	"time"

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

type (
	logsSend     = func(context.Context, *remoteproto.LogsFilterRequest) error
	receiptsSend = func(context.Context, *remoteproto.ReceiptsFilterRequest) error
)

// connectLogs opens a logs stream to srv until ctx ends and returns its filter requestor.
func connectLogs(ctx context.Context, srv gatedAckServer, onNewLogs func(*remoteproto.SubscribeLogsReply)) logsSend {
	back := NewRemoteBackend(direct.NewEthBackendClientDirect(srv), nil, nil)
	var requestor atomic.Value
	go back.SubscribeLogs(ctx, onNewLogs, &requestor) //nolint:errcheck
	synctest.Wait()
	return requestor.Load().(logsSend)
}

// connectReceipts opens a receipts stream to srv until ctx ends and returns its filter requestor.
func connectReceipts(ctx context.Context, srv gatedAckServer, onNewReceipts func(*remoteproto.SubscribeReceiptsReply)) receiptsSend {
	back := NewRemoteBackend(direct.NewEthBackendClientDirect(srv), nil, nil)
	ready := make(chan receiptsSend, 1)
	go back.SubscribeReceipts(ctx, onNewReceipts, func(send receiptsSend) { ready <- send }) //nolint:errcheck
	return <-ready
}

func ignoreLogs(*remoteproto.SubscribeLogsReply) {}

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
		srv := newGatedAckServer()
		var delivered atomic.Int32
		send := connectLogs(t.Context(), srv, func(*remoteproto.SubscribeLogsReply) { delivered.Add(1) })
		requireSendWaitsForAck(t, srv, func() error {
			return send(t.Context(), &remoteproto.LogsFilterRequest{AllAddresses: true, Ack: true})
		})
		require.Zero(t, delivered.Load(), "an ack is not a log")
	})
}

func TestSubscribeReceiptsUpdateWaitsForFilterApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := newGatedAckServer()
		var delivered atomic.Int32
		send := connectReceipts(t.Context(), srv, func(*remoteproto.SubscribeReceiptsReply) { delivered.Add(1) })
		requireSendWaitsForAck(t, srv, func() error {
			return send(t.Context(), &remoteproto.ReceiptsFilterRequest{AllTransactions: true, Ack: true})
		})
		require.Zero(t, delivered.Load(), "an ack is not a receipt")
	})
}

// An update that does not ask for an ack, such as an unsubscribe, returns once it is sent.
func TestFilterUpdateWithoutAckDoesNotWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		send := connectLogs(t.Context(), newGatedAckServer(), ignoreLogs)
		require.NoError(t, send(t.Context(), &remoteproto.LogsFilterRequest{AllAddresses: true}))
	})
}

func TestFilterUpdateIgnoresLateAckOfTimedOutUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := newGatedAckServer()
		send := connectLogs(t.Context(), srv, ignoreLogs)

		require.ErrorContains(t, send(t.Context(), &remoteproto.LogsFilterRequest{Ack: true}), "no ack of the filter update")
		srv.release <- struct{}{} // the late ack of the timed-out update
		requireSendWaitsForAck(t, srv, func() error {
			return send(t.Context(), &remoteproto.LogsFilterRequest{AllAddresses: true, Ack: true})
		})
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
		srv := newGatedAckServer()
		srv.version = &typesproto.VersionReply{Major: 5, Minor: 0}
		send := connectLogs(t.Context(), srv, ignoreLogs)
		req := &remoteproto.LogsFilterRequest{AllAddresses: true, Ack: true}
		require.NoError(t, send(t.Context(), req))
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
		go func() { done <- back.SubscribeLogs(t.Context(), ignoreLogs, &requestor) }()
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
		ctx, end := context.WithCancel(t.Context())
		send := connectLogs(ctx, srv, ignoreLogs)
		req := &remoteproto.LogsFilterRequest{AllAddresses: true, Ack: true}
		require.NoError(t, send(t.Context(), req))
		require.False(t, req.Ack)
		end()

		*version = typesproto.VersionReply{Major: 5, Patch: 1}
		send = connectLogs(t.Context(), srv, ignoreLogs)
		requireSendWaitsForAck(t, srv, func() error {
			return send(t.Context(), &remoteproto.LogsFilterRequest{AllAddresses: true, Ack: true})
		})
	})
}

// A filter update waits for the server's ack at most until the caller's deadline.
func TestFilterUpdateEndsAtTheCallersDeadline(t *testing.T) {
	for name, connect := range map[string]func(context.Context, gatedAckServer) func(context.Context) error{
		"logs": func(ctx context.Context, srv gatedAckServer) func(context.Context) error {
			send := connectLogs(ctx, srv, ignoreLogs)
			return func(callCtx context.Context) error {
				return send(callCtx, &remoteproto.LogsFilterRequest{AllAddresses: true, Ack: true})
			}
		},
		"receipts": func(ctx context.Context, srv gatedAckServer) func(context.Context) error {
			send := connectReceipts(ctx, srv, func(*remoteproto.SubscribeReceiptsReply) {})
			return func(callCtx context.Context) error {
				return send(callCtx, &remoteproto.ReceiptsFilterRequest{AllTransactions: true, Ack: true})
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv := newGatedAckServer()
				send := connect(t.Context(), srv)
				deadline := filterAckTimeout / 5
				callCtx, cancelCall := context.WithTimeout(t.Context(), deadline)
				defer cancelCall()
				start := time.Now()
				require.ErrorIs(t, send(callCtx), context.DeadlineExceeded)
				require.Equal(t, deadline, time.Since(start))
				srv.release <- struct{}{} // the server applies the abandoned update
			})
		})
	}
}
