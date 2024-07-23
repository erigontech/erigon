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

	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"google.golang.org/grpc"
)

type StateDiffClient interface {
	StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error)
	Snapshots(ctx context.Context, in *remote.SnapshotsRequest, opts ...grpc.CallOption) (*remote.SnapshotsReply, error)
}

var _ StateDiffClient = (*StateDiffClientDirect)(nil) // compile-time interface check

// SentryClientDirect implements SentryClient interface by connecting the instance of the client directly with the corresponding
// instance of SentryServer
type StateDiffClientDirect struct {
	server remote.KVServer
}

func NewStateDiffClientDirect(server remote.KVServer) *StateDiffClientDirect {
	return &StateDiffClientDirect{server: server}
}

func (c *StateDiffClientDirect) Snapshots(ctx context.Context, in *remote.SnapshotsRequest, opts ...grpc.CallOption) (*remote.SnapshotsReply, error) {
	return c.server.Snapshots(ctx, in)
}

// -- start StateChanges

func (c *StateDiffClientDirect) StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error) {
	ch := make(chan *stateDiffReply, 16384)
	streamServer := &StateDiffStreamS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(c.server.StateChanges(in, streamServer))
	}()
	return &StateDiffStreamC{ch: ch, ctx: ctx}, nil
}

type stateDiffReply struct {
	r   *remote.StateChangeBatch
	err error
}

type StateDiffStreamC struct {
	ch  chan *stateDiffReply
	ctx context.Context
	grpc.ClientStream
}

func (c *StateDiffStreamC) Recv() (*remote.StateChangeBatch, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}
func (c *StateDiffStreamC) Context() context.Context { return c.ctx }

// StateDiffStreamS implements proto_sentry.Sentry_ReceiveMessagesServer
type StateDiffStreamS struct {
	ch  chan *stateDiffReply
	ctx context.Context
	grpc.ServerStream
}

func (s *StateDiffStreamS) Send(m *remote.StateChangeBatch) error {
	s.ch <- &stateDiffReply{r: m}
	return nil
}
func (s *StateDiffStreamS) Context() context.Context { return s.ctx }
func (s *StateDiffStreamS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &stateDiffReply{err: err}
}

// -- end StateChanges
