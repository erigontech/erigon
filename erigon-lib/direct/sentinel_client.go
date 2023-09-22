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

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
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

// Subscribe gossip part. the only complex section of this bullshit

func (s *SentinelClientDirect) SubscribeGossip(ctx context.Context, in *sentinel.EmptyMessage, opts ...grpc.CallOption) (sentinel.Sentinel_SubscribeGossipClient, error) {
	ch := make(chan *gossipReply, 16384)
	streamServer := &SentinelSubscribeGossipS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(s.server.SubscribeGossip(in, streamServer))
	}()
	return &SentinelSubscribeGossipC{ch: ch, ctx: ctx}, nil
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
