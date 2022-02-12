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

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
)

type StateDiffClient interface {
	StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error)
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

// -- start StateChanges

func (c *StateDiffClientDirect) StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error) {
	messageCh := make(chan *remote.StateChangeBatch, 16384)
	streamServer := &StateDiffServerStream{messageCh: messageCh, ctx: ctx}
	go func() {
		if err := c.server.StateChanges(in, streamServer); err != nil {
			log.Warn("StateChanges returned", "err", err)
		}
		close(messageCh)
	}()
	return &StateDiffClientStream{messageCh: messageCh, ctx: ctx}, nil
}

type StateDiffClientStream struct {
	messageCh chan *remote.StateChangeBatch
	ctx       context.Context
	grpc.ClientStream
}

func (c *StateDiffClientStream) Recv() (*remote.StateChangeBatch, error) {
	m := <-c.messageCh
	return m, nil
}
func (c *StateDiffClientStream) Context() context.Context { return c.ctx }

// StateDiffServerStream implements proto_sentry.Sentry_ReceiveMessagesServer
type StateDiffServerStream struct {
	messageCh chan *remote.StateChangeBatch
	ctx       context.Context
	grpc.ServerStream
}

func (s *StateDiffServerStream) Send(m *remote.StateChangeBatch) error {
	s.messageCh <- m
	return nil
}
func (s *StateDiffServerStream) Context() context.Context { return s.ctx }

// -- end StateChanges
