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

	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ txpool_proto.TxpoolClient = (*TxPoolClient)(nil)

type TxPoolClient struct {
	server txpool_proto.TxpoolServer
}

func NewTxPoolClient(server txpool_proto.TxpoolServer) *TxPoolClient {
	return &TxPoolClient{server}
}

func (s *TxPoolClient) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.VersionReply, error) {
	return s.server.Version(ctx, in)
}

func (s *TxPoolClient) FindUnknown(ctx context.Context, in *txpool_proto.TxHashes, opts ...grpc.CallOption) (*txpool_proto.TxHashes, error) {
	return s.server.FindUnknown(ctx, in)
}

func (s *TxPoolClient) Add(ctx context.Context, in *txpool_proto.AddRequest, opts ...grpc.CallOption) (*txpool_proto.AddReply, error) {
	return s.server.Add(ctx, in)
}

func (s *TxPoolClient) Transactions(ctx context.Context, in *txpool_proto.TransactionsRequest, opts ...grpc.CallOption) (*txpool_proto.TransactionsReply, error) {
	return s.server.Transactions(ctx, in)
}

func (s *TxPoolClient) All(ctx context.Context, in *txpool_proto.AllRequest, opts ...grpc.CallOption) (*txpool_proto.AllReply, error) {
	return s.server.All(ctx, in)
}

func (s *TxPoolClient) Pending(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*txpool_proto.PendingReply, error) {
	return s.server.Pending(ctx, in)
}

// -- start OnAdd

func (s *TxPoolClient) OnAdd(ctx context.Context, in *txpool_proto.OnAddRequest, opts ...grpc.CallOption) (txpool_proto.Txpool_OnAddClient, error) {
	ch := make(chan *onAddReply, 16384)
	streamServer := &TxPoolOnAddS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(s.server.OnAdd(in, streamServer))
	}()
	return &TxPoolOnAddC{ch: ch, ctx: ctx}, nil
}

type onAddReply struct {
	r   *txpool_proto.OnAddReply
	err error
}

type TxPoolOnAddS struct {
	ch  chan *onAddReply
	ctx context.Context
	grpc.ServerStream
}

func (s *TxPoolOnAddS) Send(m *txpool_proto.OnAddReply) error {
	s.ch <- &onAddReply{r: m}
	return nil
}
func (s *TxPoolOnAddS) Context() context.Context { return s.ctx }
func (s *TxPoolOnAddS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &onAddReply{err: err}
}

type TxPoolOnAddC struct {
	ch  chan *onAddReply
	ctx context.Context
	grpc.ClientStream
}

func (c *TxPoolOnAddC) Recv() (*txpool_proto.OnAddReply, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}
func (c *TxPoolOnAddC) Context() context.Context { return c.ctx }

// -- end OnAdd

func (s *TxPoolClient) Status(ctx context.Context, in *txpool_proto.StatusRequest, opts ...grpc.CallOption) (*txpool_proto.StatusReply, error) {
	return s.server.Status(ctx, in)
}

func (s *TxPoolClient) Nonce(ctx context.Context, in *txpool_proto.NonceRequest, opts ...grpc.CallOption) (*txpool_proto.NonceReply, error) {
	return s.server.Nonce(ctx, in)
}
