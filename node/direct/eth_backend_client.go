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

	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
)

type EthBackendClientDirect struct {
	server remoteproto.ETHBACKENDServer
}

func NewEthBackendClientDirect(server remoteproto.ETHBACKENDServer) *EthBackendClientDirect {
	return &EthBackendClientDirect{server: server}
}

func (s *EthBackendClientDirect) Etherbase(ctx context.Context, in *remoteproto.EtherbaseRequest, opts ...grpc.CallOption) (*remoteproto.EtherbaseReply, error) {
	return s.server.Etherbase(ctx, in)
}

func (s *EthBackendClientDirect) NetVersion(ctx context.Context, in *remoteproto.NetVersionRequest, opts ...grpc.CallOption) (*remoteproto.NetVersionReply, error) {
	return s.server.NetVersion(ctx, in)
}

func (s *EthBackendClientDirect) NetPeerCount(ctx context.Context, in *remoteproto.NetPeerCountRequest, opts ...grpc.CallOption) (*remoteproto.NetPeerCountReply, error) {
	return s.server.NetPeerCount(ctx, in)
}

func (s *EthBackendClientDirect) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*typesproto.VersionReply, error) {
	return s.server.Version(ctx, in)
}

func (s *EthBackendClientDirect) Syncing(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.SyncingReply, error) {
	return s.server.Syncing(ctx, in)
}

func (s *EthBackendClientDirect) ProtocolVersion(ctx context.Context, in *remoteproto.ProtocolVersionRequest, opts ...grpc.CallOption) (*remoteproto.ProtocolVersionReply, error) {
	return s.server.ProtocolVersion(ctx, in)
}

func (s *EthBackendClientDirect) ClientVersion(ctx context.Context, in *remoteproto.ClientVersionRequest, opts ...grpc.CallOption) (*remoteproto.ClientVersionReply, error) {
	return s.server.ClientVersion(ctx, in)
}

// -- start Subscribe

func (s *EthBackendClientDirect) Subscribe(ctx context.Context, in *remoteproto.SubscribeRequest, opts ...grpc.CallOption) (remoteproto.ETHBACKEND_SubscribeClient, error) {
	ch := make(chan *subscribeReply, 16384)
	streamServer := &SubscribeStreamS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(s.server.Subscribe(in, streamServer))
	}()
	return &SubscribeStreamC{ch: ch, ctx: ctx}, nil
}

type subscribeReply struct {
	r   *remoteproto.SubscribeReply
	err error
}
type SubscribeStreamS struct {
	ch  chan *subscribeReply
	ctx context.Context
	grpc.ServerStream
}

func (s *SubscribeStreamS) Send(m *remoteproto.SubscribeReply) error {
	s.ch <- &subscribeReply{r: m}
	return nil
}
func (s *SubscribeStreamS) Context() context.Context { return s.ctx }
func (s *SubscribeStreamS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &subscribeReply{err: err}
}

type SubscribeStreamC struct {
	ch  chan *subscribeReply
	ctx context.Context
	grpc.ClientStream
}

func (c *SubscribeStreamC) Recv() (*remoteproto.SubscribeReply, error) {
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

func (c *SubscribeStreamC) Context() context.Context { return c.ctx }

// -- end Subscribe

// -- SubscribeLogs

func (s *EthBackendClientDirect) SubscribeLogs(ctx context.Context, opts ...grpc.CallOption) (remoteproto.ETHBACKEND_SubscribeLogsClient, error) {
	subscribeLogsRequestChan := make(chan *subscribeLogsRequest, 16384)
	subscribeLogsReplyChan := make(chan *subscribeLogsReply, 16384)
	srv := &SubscribeLogsStreamS{
		chSend: subscribeLogsReplyChan,
		chRecv: subscribeLogsRequestChan,
		ctx:    ctx,
	}
	go func() {
		defer close(subscribeLogsRequestChan)
		defer close(subscribeLogsReplyChan)
		srv.Err(s.server.SubscribeLogs(srv))
	}()
	cli := &SubscribeLogsStreamC{
		chSend: subscribeLogsRequestChan,
		chRecv: subscribeLogsReplyChan,
		ctx:    ctx,
	}
	return cli, nil
}

type SubscribeLogsStreamS struct {
	chSend chan *subscribeLogsReply
	chRecv chan *subscribeLogsRequest
	ctx    context.Context
	grpc.ServerStream
}

type subscribeLogsReply struct {
	r   *remoteproto.SubscribeLogsReply
	err error
}

type subscribeLogsRequest struct {
	r   *remoteproto.LogsFilterRequest
	err error
}

func (s *SubscribeLogsStreamS) Send(m *remoteproto.SubscribeLogsReply) error {
	s.chSend <- &subscribeLogsReply{r: m}
	return nil
}

func (s *SubscribeLogsStreamS) Recv() (*remoteproto.LogsFilterRequest, error) {
	select {
	case m, ok := <-s.chRecv:
		if !ok || m == nil {
			return nil, io.EOF
		}
		return m.r, m.err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *SubscribeLogsStreamS) Err(err error) {
	if err == nil {
		return
	}
	s.chSend <- &subscribeLogsReply{err: err}
}

type SubscribeLogsStreamC struct {
	chSend chan *subscribeLogsRequest
	chRecv chan *subscribeLogsReply
	ctx    context.Context
	grpc.ClientStream
}

func (c *SubscribeLogsStreamC) Send(m *remoteproto.LogsFilterRequest) error {
	c.chSend <- &subscribeLogsRequest{r: m}
	return nil
}

func (c *SubscribeLogsStreamC) Recv() (*remoteproto.SubscribeLogsReply, error) {
	select {
	case m, ok := <-c.chRecv:
		if !ok || m == nil {
			return nil, io.EOF
		}
		return m.r, m.err
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}

// -- end SubscribeLogs

func (s *EthBackendClientDirect) CanonicalBodyForStorage(ctx context.Context, in *remoteproto.CanonicalBodyForStorageRequest, opts ...grpc.CallOption) (*remoteproto.CanonicalBodyForStorageReply, error) {
	return s.server.CanonicalBodyForStorage(ctx, in)
}

func (s *EthBackendClientDirect) CanonicalHash(ctx context.Context, in *remoteproto.CanonicalHashRequest, opts ...grpc.CallOption) (*remoteproto.CanonicalHashReply, error) {
	return s.server.CanonicalHash(ctx, in)
}

func (s *EthBackendClientDirect) HeaderNumber(ctx context.Context, in *remoteproto.HeaderNumberRequest, opts ...grpc.CallOption) (*remoteproto.HeaderNumberReply, error) {
	return s.server.HeaderNumber(ctx, in)
}

func (s *EthBackendClientDirect) Block(ctx context.Context, in *remoteproto.BlockRequest, opts ...grpc.CallOption) (*remoteproto.BlockReply, error) {
	return s.server.Block(ctx, in)
}

func (s *EthBackendClientDirect) TxnLookup(ctx context.Context, in *remoteproto.TxnLookupRequest, opts ...grpc.CallOption) (*remoteproto.TxnLookupReply, error) {
	return s.server.TxnLookup(ctx, in)
}

func (s *EthBackendClientDirect) NodeInfo(ctx context.Context, in *remoteproto.NodesInfoRequest, opts ...grpc.CallOption) (*remoteproto.NodesInfoReply, error) {
	return s.server.NodeInfo(ctx, in)
}

func (s *EthBackendClientDirect) Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.PeersReply, error) {
	return s.server.Peers(ctx, in)
}

func (s *EthBackendClientDirect) AddPeer(ctx context.Context, in *remoteproto.AddPeerRequest, opts ...grpc.CallOption) (*remoteproto.AddPeerReply, error) {
	return s.server.AddPeer(ctx, in)
}

func (s *EthBackendClientDirect) RemovePeer(ctx context.Context, in *remoteproto.RemovePeerRequest, opts ...grpc.CallOption) (*remoteproto.RemovePeerReply, error) {
	return s.server.RemovePeer(ctx, in)
}

func (s *EthBackendClientDirect) PendingBlock(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.PendingBlockReply, error) {
	return s.server.PendingBlock(ctx, in)
}

func (s *EthBackendClientDirect) BorTxnLookup(ctx context.Context, in *remoteproto.BorTxnLookupRequest, opts ...grpc.CallOption) (*remoteproto.BorTxnLookupReply, error) {
	return s.server.BorTxnLookup(ctx, in)
}

func (s *EthBackendClientDirect) BorEvents(ctx context.Context, in *remoteproto.BorEventsRequest, opts ...grpc.CallOption) (*remoteproto.BorEventsReply, error) {
	return s.server.BorEvents(ctx, in)
}

func (s *EthBackendClientDirect) AAValidation(ctx context.Context, in *remoteproto.AAValidationRequest, opts ...grpc.CallOption) (*remoteproto.AAValidationReply, error) {
	return s.server.AAValidation(ctx, in)
}

func (s *EthBackendClientDirect) BlockForTxNum(ctx context.Context, in *remoteproto.BlockForTxNumRequest, opts ...grpc.CallOption) (*remoteproto.BlockForTxNumResponse, error) {
	return s.server.BlockForTxNum(ctx, in)
}
