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

package freezeblocks

import (
	"context"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type frozenBlocksTestServer struct {
	remoteproto.UnimplementedETHBACKENDServer
	client remoteproto.ETHBACKENDClient
}

func (s frozenBlocksTestServer) FrozenBlocks(ctx context.Context, request *emptypb.Empty) (*remoteproto.FrozenBlocksReply, error) {
	return s.client.FrozenBlocks(ctx, request)
}

func newFrozenBlocksTestReader(t *testing.T, client remoteproto.ETHBACKENDClient) *RemoteBlockReader {
	t.Helper()
	server := grpc.NewServer()
	remoteproto.RegisterETHBACKENDServer(server, frozenBlocksTestServer{client: client})
	listener := bufconn.Listen(1024 * 1024)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///frozen-blocks", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})
	return NewRemoteBlockReader(remoteproto.NewETHBACKENDClient(conn))
}

func TestRemoteBlockReaderIsFrozenDoesNotShareCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &blockingFrozenBlocksClient{entered: make(chan struct{}, 1), release: make(chan struct{})}
		reader := newFrozenBlocksTestReader(t, client)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		leader := make(chan error, 1)
		go func() {
			_, err := reader.IsFrozen(ctx, 20)
			leader <- err
		}()
		<-client.entered
		waiter := make(chan error, 1)
		go func() {
			frozen, err := reader.IsFrozen(t.Context(), 20)
			if err == nil {
				assert.True(t, frozen)
			}
			waiter <- err
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case err := <-leader:
			assert.ErrorIs(t, err, context.Canceled)
		default:
			t.Error("cancelled initiator must return before the backend finishes")
		}
		close(client.release)
		require.NoError(t, <-waiter, "a different request's cancellation must not fail the shared lookup")
		require.EqualValues(t, 1, client.calls.Load())
	})
}

func TestRemoteBlockReaderIsFrozenCancellationKeepsObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &blockingFrozenBlocksClient{entered: make(chan struct{}, 1), release: make(chan struct{})}
		reader := newFrozenBlocksTestReader(t, client)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := reader.IsFrozen(ctx, 20)
			result <- err
		}()
		<-client.entered
		cancel()
		synctest.Wait()
		close(client.release)
		err := <-result
		for range 2 {
			count, observed := reader.FrozenBlocksObserved()
			require.True(t, observed, "a cancelled request must not leave the cold cache in its failure backoff")
			require.EqualValues(t, 42, count)
			require.EqualValues(t, 1, client.calls.Load())
			time.Sleep(29 * time.Second)
		}
		require.ErrorIs(t, err, context.Canceled)
	})
}
