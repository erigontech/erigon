// Copyright 2025 The Erigon Authors
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

package shutter

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type retryStateChangesClient struct {
	subscribe func(context.Context) (remoteproto.KV_StateChangesClient, error)
}

func (c retryStateChangesClient) StateChanges(ctx context.Context, _ *remoteproto.StateChangeRequest, _ ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error) {
	return c.subscribe(ctx)
}

type blockEventStream struct {
	remoteproto.KV_StateChangesClient
	recv func() (*remoteproto.StateChangeBatch, error)
}

func (s blockEventStream) Recv() (*remoteproto.StateChangeBatch, error) { return s.recv() }

func TestBlockListenerRetriesStateChanges(t *testing.T) {
	for _, setupError := range []bool{true, false} {
		name := "receive"
		if setupError {
			name = "subscribe"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var attempts []time.Time
				client := retryStateChangesClient{subscribe: func(streamCtx context.Context) (remoteproto.KV_StateChangesClient, error) {
					attempts = append(attempts, time.Now())
					if len(attempts) == 1 {
						err := errors.New("snapshot read failed")
						if setupError {
							return nil, err
						}
						return blockEventStream{recv: func() (*remoteproto.StateChangeBatch, error) { return nil, err }}, nil
					}
					sent := false
					return blockEventStream{recv: func() (*remoteproto.StateChangeBatch, error) {
						if sent {
							<-streamCtx.Done()
							return nil, streamCtx.Err()
						}
						sent = true
						return &remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 42}}}, nil
					}}, nil
				}}
				listener := NewBlockListener(log.New(), client)
				var events []BlockEvent
				listener.RegisterObserver(func(e BlockEvent) {
					events = append(events, e)
					cancel()
				})
				require.ErrorIs(t, listener.Run(ctx), context.Canceled)
				require.Equal(t, []BlockEvent{{LatestBlockNum: 42}}, events)
				require.Len(t, attempts, 2)
				require.Equal(t, 3*time.Second, attempts[1].Sub(attempts[0]))
			})
		})
	}
}
