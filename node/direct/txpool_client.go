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
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
)

var _ txpoolproto.TxpoolClient = (*TxPoolClient)(nil)

type TxPoolClient struct {
	server txpoolproto.TxpoolServer
}

func NewTxPoolClient(server txpoolproto.TxpoolServer) *TxPoolClient {
	return &TxPoolClient{server}
}

func (s *TxPoolClient) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*typesproto.VersionReply, error) {
	return s.server.Version(ctx, in)
}

func (s *TxPoolClient) FindUnknown(ctx context.Context, in *txpoolproto.TxHashes, opts ...grpc.CallOption) (*txpoolproto.TxHashes, error) {
	return s.server.FindUnknown(ctx, in)
}

func (s *TxPoolClient) Add(ctx context.Context, in *txpoolproto.AddRequest, opts ...grpc.CallOption) (*txpoolproto.AddReply, error) {
	return s.server.Add(ctx, in)
}

func (s *TxPoolClient) Transactions(ctx context.Context, in *txpoolproto.TransactionsRequest, opts ...grpc.CallOption) (*txpoolproto.TransactionsReply, error) {
	return s.server.Transactions(ctx, in)
}

func (s *TxPoolClient) All(ctx context.Context, in *txpoolproto.AllRequest, opts ...grpc.CallOption) (*txpoolproto.AllReply, error) {
	return s.server.All(ctx, in)
}

func (s *TxPoolClient) Pending(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*txpoolproto.PendingReply, error) {
	return s.server.Pending(ctx, in)
}

// -- start OnAdd

func (s *TxPoolClient) OnAdd(ctx context.Context, in *txpoolproto.OnAddRequest, opts ...grpc.CallOption) (txpoolproto.Txpool_OnAddClient, error) {
	ch := make(chan *onAddReply, 16384)
	streamServer := &TxPoolOnAddS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(s.server.OnAdd(in, streamServer))
	}()
	return &TxPoolOnAddC{ch: ch, ctx: ctx}, nil
}

type onAddReply struct {
	r   *txpoolproto.OnAddReply
	err error
}

type TxPoolOnAddS struct {
	ch  chan *onAddReply
	ctx context.Context
	grpc.ServerStream
}

func (s *TxPoolOnAddS) Send(m *txpoolproto.OnAddReply) error {
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

func (c *TxPoolOnAddC) Recv() (*txpoolproto.OnAddReply, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}
func (c *TxPoolOnAddC) Context() context.Context { return c.ctx }

// -- end OnAdd

func (s *TxPoolClient) Status(ctx context.Context, in *txpoolproto.StatusRequest, opts ...grpc.CallOption) (*txpoolproto.StatusReply, error) {
	return s.server.Status(ctx, in)
}

func (s *TxPoolClient) Nonce(ctx context.Context, in *txpoolproto.NonceRequest, opts ...grpc.CallOption) (*txpoolproto.NonceReply, error) {
	return s.server.Nonce(ctx, in)
}

func (s *TxPoolClient) GetBlobs(ctx context.Context, in *txpoolproto.GetBlobsRequest, opts ...grpc.CallOption) (*txpoolproto.GetBlobsReply, error) {
	return s.server.GetBlobs(ctx, in)
}
