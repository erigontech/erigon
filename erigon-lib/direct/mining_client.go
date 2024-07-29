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

	txpool_proto "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ txpool_proto.MiningClient = (*MiningClient)(nil)

type MiningClient struct {
	server txpool_proto.MiningServer
}

func NewMiningClient(server txpool_proto.MiningServer) *MiningClient {
	return &MiningClient{server: server}
}

func (s *MiningClient) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.VersionReply, error) {
	return s.server.Version(ctx, in)
}

// -- start OnPendingBlock

func (s *MiningClient) OnPendingBlock(ctx context.Context, in *txpool_proto.OnPendingBlockRequest, opts ...grpc.CallOption) (txpool_proto.Mining_OnPendingBlockClient, error) {
	ch := make(chan *onPendigBlockReply, 16384)
	streamServer := &MiningOnPendingBlockS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(s.server.OnPendingBlock(in, streamServer))
	}()
	return &MiningOnPendingBlockC{ch: ch, ctx: ctx}, nil
}

type onPendigBlockReply struct {
	r   *txpool_proto.OnPendingBlockReply
	err error
}

type MiningOnPendingBlockS struct {
	ch  chan *onPendigBlockReply
	ctx context.Context
	grpc.ServerStream
}

func (s *MiningOnPendingBlockS) Send(m *txpool_proto.OnPendingBlockReply) error {
	s.ch <- &onPendigBlockReply{r: m}
	return nil
}
func (s *MiningOnPendingBlockS) Context() context.Context { return s.ctx }
func (s *MiningOnPendingBlockS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &onPendigBlockReply{err: err}
}

type MiningOnPendingBlockC struct {
	ch  chan *onPendigBlockReply
	ctx context.Context
	grpc.ClientStream
}

func (c *MiningOnPendingBlockC) Recv() (*txpool_proto.OnPendingBlockReply, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}
func (c *MiningOnPendingBlockC) Context() context.Context { return c.ctx }

// -- end OnPendingBlock
// -- start OnMinedBlock

func (s *MiningClient) OnMinedBlock(ctx context.Context, in *txpool_proto.OnMinedBlockRequest, opts ...grpc.CallOption) (txpool_proto.Mining_OnMinedBlockClient, error) {
	ch := make(chan *onMinedBlockReply, 16384)
	streamServer := &MiningOnMinedBlockS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(s.server.OnMinedBlock(in, streamServer))
	}()
	return &MiningOnMinedBlockC{ch: ch, ctx: ctx}, nil
}

type onMinedBlockReply struct {
	r   *txpool_proto.OnMinedBlockReply
	err error
}

type MiningOnMinedBlockS struct {
	ch  chan *onMinedBlockReply
	ctx context.Context
	grpc.ServerStream
}

func (s *MiningOnMinedBlockS) Send(m *txpool_proto.OnMinedBlockReply) error {
	s.ch <- &onMinedBlockReply{r: m}
	return nil
}
func (s *MiningOnMinedBlockS) Context() context.Context { return s.ctx }
func (s *MiningOnMinedBlockS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &onMinedBlockReply{err: err}
}

type MiningOnMinedBlockC struct {
	ch  chan *onMinedBlockReply
	ctx context.Context
	grpc.ClientStream
}

func (c *MiningOnMinedBlockC) Recv() (*txpool_proto.OnMinedBlockReply, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}
func (c *MiningOnMinedBlockC) Context() context.Context { return c.ctx }

// -- end OnMinedBlock
// -- end OnPendingLogs

func (s *MiningClient) OnPendingLogs(ctx context.Context, in *txpool_proto.OnPendingLogsRequest, opts ...grpc.CallOption) (txpool_proto.Mining_OnPendingLogsClient, error) {
	ch := make(chan *onPendingLogsReply, 16384)
	streamServer := &MiningOnPendingLogsS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(s.server.OnPendingLogs(in, streamServer))
	}()
	return &MiningOnPendingLogsC{ch: ch, ctx: ctx}, nil
}

type onPendingLogsReply struct {
	r   *txpool_proto.OnPendingLogsReply
	err error
}
type MiningOnPendingLogsS struct {
	ch  chan *onPendingLogsReply
	ctx context.Context
	grpc.ServerStream
}

func (s *MiningOnPendingLogsS) Send(m *txpool_proto.OnPendingLogsReply) error {
	s.ch <- &onPendingLogsReply{r: m}
	return nil
}
func (s *MiningOnPendingLogsS) Context() context.Context { return s.ctx }
func (s *MiningOnPendingLogsS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &onPendingLogsReply{err: err}
}

type MiningOnPendingLogsC struct {
	ch  chan *onPendingLogsReply
	ctx context.Context
	grpc.ClientStream
}

func (c *MiningOnPendingLogsC) Recv() (*txpool_proto.OnPendingLogsReply, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}
func (c *MiningOnPendingLogsC) Context() context.Context { return c.ctx }

// -- end OnPendingLogs

func (s *MiningClient) GetWork(ctx context.Context, in *txpool_proto.GetWorkRequest, opts ...grpc.CallOption) (*txpool_proto.GetWorkReply, error) {
	return s.server.GetWork(ctx, in)
}

func (s *MiningClient) SubmitWork(ctx context.Context, in *txpool_proto.SubmitWorkRequest, opts ...grpc.CallOption) (*txpool_proto.SubmitWorkReply, error) {
	return s.server.SubmitWork(ctx, in)
}

func (s *MiningClient) SubmitHashRate(ctx context.Context, in *txpool_proto.SubmitHashRateRequest, opts ...grpc.CallOption) (*txpool_proto.SubmitHashRateReply, error) {
	return s.server.SubmitHashRate(ctx, in)
}

func (s *MiningClient) HashRate(ctx context.Context, in *txpool_proto.HashRateRequest, opts ...grpc.CallOption) (*txpool_proto.HashRateReply, error) {
	return s.server.HashRate(ctx, in)
}

func (s *MiningClient) Mining(ctx context.Context, in *txpool_proto.MiningRequest, opts ...grpc.CallOption) (*txpool_proto.MiningReply, error) {
	return s.server.Mining(ctx, in)
}
