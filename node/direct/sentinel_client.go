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

	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"google.golang.org/grpc"
)

type SentinelClientDirect struct {
	server sentinel.SentinelServer
}

func NewSentinelClientDirect(sentinel sentinel.SentinelServer) sentinel.SentinelClient {
	return &SentinelClientDirect{server: sentinel}
}

func (s *SentinelClientDirect) SendRequest(ctx context.Context, in *sentinel.RequestData, opts ...grpc.CallOption) (*sentinel.ResponseData, error) {
	return s.server.SendRequest(ctx, in)
}

func (s *SentinelClientDirect) SendPeerRequest(ctx context.Context, in *sentinel.RequestDataWithPeer, opts ...grpc.CallOption) (*sentinel.ResponseData, error) {
	return s.server.SendPeerRequest(ctx, in)
}

func (s *SentinelClientDirect) SetStatus(ctx context.Context, in *sentinel.Status, opts ...grpc.CallOption) (*sentinel.EmptyMessage, error) {
	return s.server.SetStatus(ctx, in)
}

func (s *SentinelClientDirect) GetPeers(ctx context.Context, in *sentinel.EmptyMessage, opts ...grpc.CallOption) (*sentinel.PeerCount, error) {
	return s.server.GetPeers(ctx, in)
}

func (s *SentinelClientDirect) BanPeer(ctx context.Context, p *sentinel.Peer, opts ...grpc.CallOption) (*sentinel.EmptyMessage, error) {
	return s.server.BanPeer(ctx, p)
}
func (s *SentinelClientDirect) UnbanPeer(ctx context.Context, p *sentinel.Peer, opts ...grpc.CallOption) (*sentinel.EmptyMessage, error) {
	return s.server.UnbanPeer(ctx, p)
}
func (s *SentinelClientDirect) RewardPeer(ctx context.Context, p *sentinel.Peer, opts ...grpc.CallOption) (*sentinel.EmptyMessage, error) {
	return s.server.RewardPeer(ctx, p)
}
func (s *SentinelClientDirect) PenalizePeer(ctx context.Context, p *sentinel.Peer, opts ...grpc.CallOption) (*sentinel.EmptyMessage, error) {
	return s.server.PenalizePeer(ctx, p)
}

func (s *SentinelClientDirect) PublishGossip(ctx context.Context, in *sentinel.GossipData, opts ...grpc.CallOption) (*sentinel.EmptyMessage, error) {
	return s.server.PublishGossip(ctx, in)
}

func (s *SentinelClientDirect) Identity(ctx context.Context, in *sentinel.EmptyMessage, opts ...grpc.CallOption) (*sentinel.IdentityResponse, error) {
	return s.server.Identity(ctx, in)
}

func (s *SentinelClientDirect) PeersInfo(ctx context.Context, in *sentinel.PeersInfoRequest, opts ...grpc.CallOption) (*sentinel.PeersInfoResponse, error) {
	return s.server.PeersInfo(ctx, in)
}

// Subscribe gossip part. the only complex section of this bullshit

func (s *SentinelClientDirect) SubscribeGossip(ctx context.Context, in *sentinel.SubscriptionData, opts ...grpc.CallOption) (sentinel.Sentinel_SubscribeGossipClient, error) {
	ch := make(chan *gossipReply, 1<<16)
	streamServer := &SentinelSubscribeGossipS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(s.server.SubscribeGossip(in, streamServer))
	}()
	return &SentinelSubscribeGossipC{ch: ch, ctx: ctx}, nil
}

func (s *SentinelClientDirect) SetSubscribeExpiry(ctx context.Context, expiryReq *sentinel.RequestSubscribeExpiry, opts ...grpc.CallOption) (*sentinel.EmptyMessage, error) {
	return s.server.SetSubscribeExpiry(ctx, expiryReq)
}

type SentinelSubscribeGossipC struct {
	ch  chan *gossipReply
	ctx context.Context
	grpc.ClientStream
}

func (c *SentinelSubscribeGossipC) Recv() (*sentinel.GossipData, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}
func (c *SentinelSubscribeGossipC) Context() context.Context { return c.ctx }

type SentinelSubscribeGossipS struct {
	ch  chan *gossipReply
	ctx context.Context
	grpc.ServerStream
}

type gossipReply struct {
	r   *sentinel.GossipData
	err error
}

func (s *SentinelSubscribeGossipS) Send(m *sentinel.GossipData) error {
	s.ch <- &gossipReply{r: m}
	return nil
}
func (s *SentinelSubscribeGossipS) Context() context.Context { return s.ctx }
func (s *SentinelSubscribeGossipS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &gossipReply{err: err}
}
