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
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	for _, tc := range []struct {
		name       string
		setupError bool
		err        error
		warn       bool
	}{
		{"subscribe failure", true, errors.New("snapshot read failed"), true},
		{"receive failure", false, errors.New("snapshot read failed"), true},
		{"subscribe unavailable", true, status.Error(codes.Unavailable, "executed header unavailable"), false},
		{"receive unavailable", false, status.Error(codes.Unavailable, "executed header unavailable"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var attempts []time.Time
				client := retryStateChangesClient{subscribe: func(streamCtx context.Context) (remoteproto.KV_StateChangesClient, error) {
					attempts = append(attempts, time.Now())
					if len(attempts) == 1 {
						if tc.setupError {
							return nil, tc.err
						}
						return blockEventStream{recv: func() (*remoteproto.StateChangeBatch, error) { return nil, tc.err }}, nil
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
				var logs bytes.Buffer
				logger := log.New()
				logger.SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
				listener := NewBlockListener(logger, client)
				var events []BlockEvent
				listener.RegisterObserver(func(e BlockEvent) {
					events = append(events, e)
					cancel()
				})
				require.ErrorIs(t, listener.Run(ctx), context.Canceled)
				require.Equal(t, []BlockEvent{{LatestBlockNum: 42}}, events)
				require.Len(t, attempts, 2)
				require.Equal(t, 3*time.Second, attempts[1].Sub(attempts[0]))
				require.Equal(t, tc.warn, bytes.Contains(logs.Bytes(), []byte("block listener subscription failed")))
			})
		})
	}
}
