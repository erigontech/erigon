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

package l1_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/privateapi"
)

// servePrivateAPI serves m's KV and backend APIs over TCP, as an Erigon
// node's private API does, and returns their address.
func servePrivateAPI(t *testing.T, m *execmoduletester.ExecModuleTester) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logger := log.New()
	server := grpc.NewServer()
	remoteproto.RegisterKVServer(server, remotedbserver.NewKvServer(ctx, m.DB, nil, nil, logger))
	remoteproto.RegisterETHBACKENDServer(server, privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications, m.BlockReader, logger,
		builder.NewLatestBlockBuiltStore(), m.ChainConfig))
	lis, err := new(net.ListenConfig).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		cancel()
		server.Stop()
	})
	return lis.Addr().String()
}

// TestRemoteConformance is T-L1-1 for the native reader over the private
// API of a separate Erigon L1, which also pushes its new heads.
func TestRemoteConformance(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	r, err := l1.DialRemote(t.Context(), servePrivateAPI(t, m), m.Dirs, log.New())
	require.NoError(t, err)
	t.Cleanup(r.Close)
	l1test.Conformance(t, r, model, l1test.SampleFilters)

	heads, err := r.Heads(t.Context())
	require.NoError(t, err)
	head, err := r.Head(t.Context())
	require.NoError(t, err)
	tx, err := m.DB.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	parent, err := m.BlockReader.BlockByNumber(t.Context(), tx, head.Number)
	require.NoError(t, err)
	tx.Rollback()
	next, err := m.GenerateChainFrom(parent, 1, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), next.Blocks))
	select {
	case <-heads:
	case <-time.After(10 * time.Second):
		t.Fatal("no head event")
	}
	require.Eventually(t, func() bool {
		h, err := r.Head(context.Background())
		return err == nil && h.Hash == next.Blocks[0].Hash()
	}, 10*time.Second, 10*time.Millisecond)
}

// oneHead streams one header event, then ends.
type oneHead struct {
	grpc.ClientStream
	sent bool
}

func (s *oneHead) Recv() (*remoteproto.SubscribeReply, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	return &remoteproto.SubscribeReply{Type: remoteproto.Event_HEADER}, nil
}

// restarting is a node whose head stream ends after each event and which
// refuses the first resubscription.
type restarting struct {
	remoteproto.ETHBACKENDClient
	subscribed atomic.Int32
}

func (b *restarting) Subscribe(context.Context, *remoteproto.SubscribeRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[remoteproto.SubscribeReply], error) {
	if b.subscribed.Add(1) == 2 {
		return nil, errors.New("connection refused")
	}
	return &oneHead{}, nil
}

// TestRemoteResubscribes: head events survive the end of the node's stream.
func TestRemoteResubscribes(t *testing.T) {
	backend := &restarting{}
	ctx, cancel := context.WithCancel(t.Context())
	heads, err := l1.NewRemoteHeads(backend, time.Millisecond).Heads(ctx)
	require.NoError(t, err)
	for range 3 {
		select {
		case <-heads:
		case <-time.After(10 * time.Second):
			t.Fatal("no head event")
		}
	}
	require.GreaterOrEqual(t, backend.subscribed.Load(), int32(3))
	cancel()
	require.Eventually(t, func() bool {
		select {
		case _, ok := <-heads:
			return !ok
		default:
			return false
		}
	}, 10*time.Second, time.Millisecond)
}
