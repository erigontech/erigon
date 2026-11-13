// Copyright 2021 The Erigon Authors
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

package direct

import (
	"context"
	"io"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type StateDiffClient interface {
	StateChanges(ctx context.Context, in *remoteproto.StateChangeRequest, opts ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error)
	Snapshots(ctx context.Context, in *remoteproto.SnapshotsRequest, opts ...grpc.CallOption) (*remoteproto.SnapshotsReply, error)
}

var _ StateDiffClient = (*StateDiffClientDirect)(nil) // compile-time interface check

// StateDiffClientDirect calls an in-process KV server without gRPC transport.
type StateDiffClientDirect struct {
	server remoteproto.KVServer
}

func NewStateDiffClientDirect(server remoteproto.KVServer) *StateDiffClientDirect {
	return &StateDiffClientDirect{server: server}
}

func (c *StateDiffClientDirect) Snapshots(ctx context.Context, in *remoteproto.SnapshotsRequest, opts ...grpc.CallOption) (*remoteproto.SnapshotsReply, error) {
	return c.server.Snapshots(ctx, in)
}

// -- start StateChanges

func (c *StateDiffClientDirect) StateChanges(ctx context.Context, in *remoteproto.StateChangeRequest, opts ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error) {
	// One slot lets setup errors return before the first Recv. Further backlog
	// belongs in the server's bounded queue, which detects gaps.
	ch := make(chan *stateDiffReply, 1)
	subscribed := make(chan struct{})
	done := make(chan struct{})
	streamServer := &StateDiffStreamS{ch: ch, ctx: ctx, subscribed: subscribed}
	go func() {
		defer close(done)
		defer close(ch)
		streamServer.Err(c.server.StateChanges(in, streamServer))
	}()
	select {
	case <-subscribed:
	case <-done:
		// Setup can fail before the server registers the subscription.
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &StateDiffStreamC{ch: ch, ctx: ctx}, nil
}

type stateDiffReply struct {
	r   *remoteproto.StateChangeBatch
	err error
}

type StateDiffStreamC struct {
	ch  chan *stateDiffReply
	ctx context.Context
	grpc.ClientStream
}

func (c *StateDiffStreamC) Recv() (*remoteproto.StateChangeBatch, error) {
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case m, ok := <-c.ch:
		if !ok || m == nil {
			return nil, io.EOF
		}
		return m.r, m.err
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}
func (c *StateDiffStreamC) Context() context.Context { return c.ctx }

// StateDiffStreamS implements remoteproto.KV_StateChangesServer.
type StateDiffStreamS struct {
	ch         chan *stateDiffReply
	ctx        context.Context
	subscribed chan struct{}
	grpc.ServerStream
}

func (s *StateDiffStreamS) NotifySubscribed() {
	close(s.subscribed)
}

func (s *StateDiffStreamS) Send(m *remoteproto.StateChangeBatch) error {
	select {
	case s.ch <- &stateDiffReply{r: m}:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (s *StateDiffStreamS) Context() context.Context { return s.ctx }
func (s *StateDiffStreamS) Err(err error) {
	if err == nil {
		return
	}
	select {
	case s.ch <- &stateDiffReply{err: err}:
	case <-s.ctx.Done():
	}
}

// -- end StateChanges
