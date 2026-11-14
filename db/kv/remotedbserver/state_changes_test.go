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
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
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
		require.ErrorIs(t, err, context.Canceled)
		server.SendStateChanges(t.Context(), &remoteproto.StateChangeBatch{})
	}
}

func TestStateChangesReconnectReceivesCurrentHead(t *testing.T) {
	server := NewKvServer(t.Context(), nil, nil, nil, log.New())
	first, remove := server.stateChangeStreams.Sub(nil)
	server.SendStateChanges(t.Context(), &remoteproto.StateChangeBatch{
		StateVersionId: 1,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 10, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{1}),
		}},
	})
	<-first
	remove()

	head := &remoteproto.StateChange{
		BlockHeight: 10, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{2}),
		BlockTime: 120, SlotNumber: common.NewUint64(11),
	}
	changed := proto.Clone(head).(*remoteproto.StateChange)
	changed.Txs = [][]byte{{1}}
	changed.Changes = []*remoteproto.AccountChange{{Action: remoteproto.Action_REMOVE}}
	batch := &remoteproto.StateChangeBatch{
		StateVersionId: 2, PendingBlockBaseFee: 3, PendingBlobFeePerGas: 4,
		BlockGasLimit: 30_000_000, FinalizedBlock: 8,
		ChangeBatch: []*remoteproto.StateChange{
			{Direction: remoteproto.Direction_UNWIND, BlockHeight: 9, Txs: [][]byte{{2}}},
			changed,
		},
	}
	server.SendStateChanges(t.Context(), batch)
	second, remove := server.stateChangeStreams.Sub(nil)
	defer remove()
	select {
	case event := <-second:
		want := &remoteproto.StateChangeBatch{
			StateVersionId: 2, StateChangesIncomplete: true,
			PendingBlockBaseFee: 3, PendingBlobFeePerGas: 4,
			BlockGasLimit: 30_000_000, FinalizedBlock: 8,
			ChangeBatch: []*remoteproto.StateChange{head},
		}
		require.True(t, proto.Equal(want, event.batch), "reset must carry the latest head metadata without old deltas: %v", event.batch)
		require.False(t, batch.StateChangesIncomplete, "published batches must remain unchanged")
	default:
		t.Fatal("reconnecting must receive the current head without another publication")
	}
}

func TestStateChangesSnapshotDoesNotInvalidateNextPublication(t *testing.T) {
	server := NewKvServer(t.Context(), nil, nil, nil, log.New())
	server.SetStateChangesSnapshot(func(context.Context) (*remoteproto.StateChangeBatch, error) {
		return &remoteproto.StateChangeBatch{StateChangesIncomplete: true}, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := direct.NewStateDiffClientDirect(server).StateChanges(ctx, &remoteproto.StateChangeRequest{})
	require.NoError(t, err)
	reset, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, reset.StateChangesIncomplete)
	server.SendStateChanges(ctx, &remoteproto.StateChangeBatch{StateVersionId: 1})
	next, err := stream.Recv()
	require.NoError(t, err)
	require.False(t, next.StateChangesIncomplete, "the first publication follows the reset without a gap")
}

func TestStateChangesKeepsPublicationDuringSnapshot(t *testing.T) {
	server := NewKvServer(t.Context(), nil, nil, nil, log.New())
	newHead := &remoteproto.StateChange{BlockHeight: 2, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{2})}
	server.SetStateChangesSnapshot(func(context.Context) (*remoteproto.StateChangeBatch, error) {
		server.SendStateChanges(t.Context(), &remoteproto.StateChangeBatch{StateVersionId: 2, ChangeBatch: []*remoteproto.StateChange{newHead}})
		return &remoteproto.StateChangeBatch{StateVersionId: 1, StateChangesIncomplete: true}, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := direct.NewStateDiffClientDirect(server).StateChanges(ctx, &remoteproto.StateChangeRequest{})
	require.NoError(t, err)
	reset, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(2), reset.StateVersionId)
	require.True(t, reset.StateChangesIncomplete)
	require.Len(t, reset.ChangeBatch, 1)
	require.True(t, proto.Equal(newHead, reset.ChangeBatch[0]))
}

func TestStateChangesSnapshotError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		server := NewKvServer(ctx, nil, nil, nil, log.New())
		readErr := errors.New("snapshot read failed")
		server.SetStateChangesSnapshot(func(context.Context) (*remoteproto.StateChangeBatch, error) {
			return nil, readErr
		})
		stream, err := direct.NewStateDiffClientDirect(server).StateChanges(ctx, &remoteproto.StateChangeRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.ErrorIs(t, err, readErr)
		require.NoError(t, ctx.Err(), "the setup error must arrive without waiting for cancellation")
	})
}

func TestStateChangesDirectSubscriberBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		server := NewKvServer(ctx, nil, nil, nil, log.New())
		stream, err := direct.NewStateDiffClientDirect(server).StateChanges(ctx, &remoteproto.StateChangeRequest{})
		require.NoError(t, err)

		const updates = 20
		for id := uint64(1); id <= updates; id++ {
			server.SendStateChanges(ctx, &remoteproto.StateChangeBatch{StateVersionId: id})
			synctest.Wait()
		}

		var previous uint64
		var gap bool
		for previous < updates {
			batch, err := stream.Recv()
			require.NoError(t, err)
			require.Greater(t, batch.StateVersionId, previous)
			if batch.StateVersionId > previous+1 {
				gap = true
				require.True(t, batch.StateChangesIncomplete, "the first delivery after a queue gap must trigger recovery")
			}
			previous = batch.StateVersionId
		}
		require.True(t, gap, "a stalled direct client must use the bounded server queue instead of retaining every batch")
	})
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
