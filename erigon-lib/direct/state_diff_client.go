/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package direct

import (
	"context"
	"io"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
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
