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

package remotedbserver

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type stateChangesTestStream struct {
	remoteproto.KV_StateChangesServer
	ctx          context.Context
	onSubscribed func()
	onSend       func(*remoteproto.StateChangeBatch) error
}

func (s *stateChangesTestStream) Context() context.Context { return s.ctx }
func (s *stateChangesTestStream) NotifySubscribed()        { s.onSubscribed() }
func (s *stateChangesTestStream) Send(batch *remoteproto.StateChangeBatch) error {
	return s.onSend(batch)
}

func TestStateChangesSubscriptionStartsIncomplete(t *testing.T) {
	server := NewKvServer(t.Context(), nil, nil, nil, log.New())
	client := direct.NewStateDiffClientDirect(server)
	for range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		stream, err := client.StateChanges(ctx, &remoteproto.StateChangeRequest{})
		require.NoError(t, err)
		for _, first := range []bool{true, false} {
			batch := &remoteproto.StateChangeBatch{}
			server.SendStateChanges(ctx, batch)
			reply, recvErr := stream.Recv()
			require.NoError(t, recvErr)
			require.Equal(t, first, reply.StateChangesIncomplete)
			require.False(t, batch.StateChangesIncomplete)
		}
		cancel()
		_, err = stream.Recv()
		require.ErrorIs(t, err, io.EOF)
		server.SendStateChanges(t.Context(), &remoteproto.StateChangeBatch{})
	}
}

func TestStateChangesSubscriptionReplaysLatestView(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	server := NewKvServer(ctx, nil, nil, nil, log.New())
	server.SendStateChanges(ctx, &remoteproto.StateChangeBatch{TargetView: &remoteproto.ExecutionView{BlockHeight: 7}})
	stream, err := direct.NewStateDiffClientDirect(server).StateChanges(ctx, &remoteproto.StateChangeRequest{})
	require.NoError(t, err)
	reply, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(7), reply.TargetView.BlockHeight)
	require.True(t, reply.StateChangesIncomplete)
}

func TestStateChangesSubscriberOverflow(t *testing.T) {
	ctx := t.Context()
	server := NewKvServer(ctx, nil, nil, nil, log.New())
	fast, err := direct.NewStateDiffClientDirect(server).StateChanges(ctx, &remoteproto.StateChangeRequest{})
	require.NoError(t, err)

	var published, fastReplies []*remoteproto.StateChangeBatch
	publish := func(id uint64) {
		batch := &remoteproto.StateChangeBatch{StateVersionId: id}
		published = append(published, batch)
		server.SendStateChanges(ctx, batch)
		reply, err := fast.Recv()
		require.NoError(t, err)
		fastReplies = append(fastReplies, reply)
		require.Equal(t, id, reply.StateVersionId)
		if id > 1 {
			require.False(t, reply.StateChangesIncomplete, "a slow subscriber must not affect other subscribers")
		}
		require.False(t, batch.StateChangesIncomplete, "published batches are shared and must stay immutable")
	}
	var replies []*remoteproto.StateChangeBatch
	stream := &stateChangesTestStream{
		ctx:          ctx,
		onSubscribed: func() { publish(1) },
		onSend: func(reply *remoteproto.StateChangeBatch) error {
			replies = append(replies, reply)
			if reply.StateVersionId == 1 {
				// Hold this subscriber's Send while publishing enough batches to overflow its queue.
				for id := uint64(2); id <= 10; id++ {
					publish(id)
				}
			}
			if reply.StateVersionId == 10 {
				return io.EOF
			}
			return nil
		},
	}
	require.ErrorIs(t, server.StateChanges(&remoteproto.StateChangeRequest{}, stream), io.EOF)
	require.Greater(t, len(replies), 2)
	require.Greater(t, replies[1].StateVersionId, replies[0].StateVersionId+1, "the queue must have dropped batches")
	require.True(t, replies[1].StateChangesIncomplete, "mark the first delivery after the gap")
	for _, reply := range replies[2:] {
		require.False(t, reply.StateChangesIncomplete, "later consecutive batches are complete")
	}
	for _, reply := range fastReplies[1:] {
		require.False(t, reply.StateChangesIncomplete, "subscriber flags must not change shared batches")
	}
	for _, batch := range published {
		require.False(t, batch.StateChangesIncomplete)
	}
}
