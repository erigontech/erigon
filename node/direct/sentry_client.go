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
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

const (
	ETH65 = 65
	ETH66 = 66
	ETH67 = 67
	ETH68 = 68

	WIT0 = 1
)

//go:generate mockgen -typed=true -destination=./sentry_client_mock.go -package=direct . SentryClient
type SentryClient interface {
	sentryproto.SentryClient
	Protocol() uint
	Ready() bool
	MarkDisconnected()
}

type SentryClientRemote struct {
	sentryproto.SentryClient
	sync.RWMutex
	protocol sentryproto.Protocol
	ready    bool
}

var _ SentryClient = (*SentryClientRemote)(nil) // compile-time interface check
var _ SentryClient = (*SentryClientDirect)(nil) // compile-time interface check

// NewSentryClientRemote - app code must use this class
// to avoid concurrency - it accepts protocol (which received async by SetStatus) in constructor,
// means app can't use client which protocol unknown yet
func NewSentryClientRemote(client sentryproto.SentryClient) *SentryClientRemote {
	return &SentryClientRemote{SentryClient: client}
}

func (c *SentryClientRemote) Protocol() uint {
	c.RLock()
	defer c.RUnlock()
	return ETH65 + uint(c.protocol)
}

func (c *SentryClientRemote) Ready() bool {
	c.RLock()
	defer c.RUnlock()
	return c.ready
}

func (c *SentryClientRemote) MarkDisconnected() {
	c.Lock()
	defer c.Unlock()
	c.ready = false
}

func (c *SentryClientRemote) HandShake(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.HandShakeReply, error) {
	reply, err := c.SentryClient.HandShake(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	c.Lock()
	defer c.Unlock()
	switch reply.Protocol {
	case sentryproto.Protocol_ETH67, sentryproto.Protocol_ETH68:
		c.protocol = reply.Protocol
	default:
		return nil, fmt.Errorf("unexpected protocol: %d", reply.Protocol)
	}
	c.ready = true
	return reply, nil
}
func (c *SentryClientRemote) SetStatus(ctx context.Context, in *sentryproto.StatusData, opts ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
	return c.SentryClient.SetStatus(ctx, in, opts...)
}

func (c *SentryClientRemote) Messages(ctx context.Context, in *sentryproto.MessagesRequest, opts ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
	in = &sentryproto.MessagesRequest{
		Ids: filterIds(in.Ids, c.protocol),
	}
	return c.SentryClient.Messages(ctx, in, opts...)
}

func (c *SentryClientRemote) PeerCount(ctx context.Context, in *sentryproto.PeerCountRequest, opts ...grpc.CallOption) (*sentryproto.PeerCountReply, error) {
	return c.SentryClient.PeerCount(ctx, in)
}

// Contains implementations of SentryServer, SentryClient, ControlClient, and ControlServer, that may be linked to each other
// SentryClient is linked directly to the SentryServer, for example, so any function call on the instance of the SentryClient
// cause invocations directly on the corresponding instance of the SentryServer. However, the link between SentryClient and
// SentryServer is established outside of the constructor. This means that the reference from the SentyClient to the corresponding
// SentryServer can be injected at any point in time.

// SentryClientDirect implements SentryClient interface by connecting the instance of the client directly with the corresponding
// instance of SentryServer
type SentryClientDirect struct {
	server   sentryproto.SentryServer
	protocol sentryproto.Protocol
}

func NewSentryClientDirect(protocol uint, sentryServer sentryproto.SentryServer) *SentryClientDirect {
	return &SentryClientDirect{protocol: sentryproto.Protocol(protocol - ETH65), server: sentryServer}
}

func (c *SentryClientDirect) Protocol() uint    { return uint(c.protocol) + ETH65 }
func (c *SentryClientDirect) Ready() bool       { return true }
func (c *SentryClientDirect) MarkDisconnected() {}

func (c *SentryClientDirect) PenalizePeer(ctx context.Context, in *sentryproto.PenalizePeerRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.PenalizePeer(ctx, in)
}

func (c *SentryClientDirect) PeerMinBlock(ctx context.Context, in *sentryproto.PeerMinBlockRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.PeerMinBlock(ctx, in)
}

func (c *SentryClientDirect) SendMessageByMinBlock(ctx context.Context, in *sentryproto.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	return c.server.SendMessageByMinBlock(ctx, in)
}

func (c *SentryClientDirect) SendMessageById(ctx context.Context, in *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	return c.server.SendMessageById(ctx, in)
}

func (c *SentryClientDirect) SendMessageToRandomPeers(ctx context.Context, in *sentryproto.SendMessageToRandomPeersRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	return c.server.SendMessageToRandomPeers(ctx, in)
}

func (c *SentryClientDirect) SendMessageToAll(ctx context.Context, in *sentryproto.OutboundMessageData, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	return c.server.SendMessageToAll(ctx, in)
}

func (c *SentryClientDirect) HandShake(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.HandShakeReply, error) {
	return c.server.HandShake(ctx, in)
}

func (c *SentryClientDirect) SetStatus(ctx context.Context, in *sentryproto.StatusData, opts ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
	return c.server.SetStatus(ctx, in)
}

func (c *SentryClientDirect) Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.PeersReply, error) {
	return c.server.Peers(ctx, in)
}

func (c *SentryClientDirect) PeerCount(ctx context.Context, in *sentryproto.PeerCountRequest, opts ...grpc.CallOption) (*sentryproto.PeerCountReply, error) {
	return c.server.PeerCount(ctx, in)
}

func (c *SentryClientDirect) PeerById(ctx context.Context, in *sentryproto.PeerByIdRequest, opts ...grpc.CallOption) (*sentryproto.PeerByIdReply, error) {
	return c.server.PeerById(ctx, in)
}

// -- start Messages

func (c *SentryClientDirect) Messages(ctx context.Context, in *sentryproto.MessagesRequest, opts ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
	in = &sentryproto.MessagesRequest{
		Ids: filterIds(in.Ids, c.protocol),
	}
	ch := make(chan *inboundMessageReply, 16384)
	streamServer := &SentryMessagesStreamS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(c.server.Messages(in, streamServer))
	}()
	return &SentryMessagesStreamC{ch: ch, ctx: ctx}, nil
}

type inboundMessageReply struct {
	r   *sentryproto.InboundMessage
	err error
}

// SentryMessagesStreamS implements proto_sentryproto.Sentry_ReceiveMessagesServer
type SentryMessagesStreamS struct {
	ch  chan *inboundMessageReply
	ctx context.Context
	grpc.ServerStream
}

func (s *SentryMessagesStreamS) Send(m *sentryproto.InboundMessage) error {
	s.ch <- &inboundMessageReply{r: m}
	return nil
}

func (s *SentryMessagesStreamS) Context() context.Context { return s.ctx }

func (s *SentryMessagesStreamS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &inboundMessageReply{err: err}
}

type SentryMessagesStreamC struct {
	ch  chan *inboundMessageReply
	ctx context.Context
	grpc.ClientStream
}

func (c *SentryMessagesStreamC) Recv() (*sentryproto.InboundMessage, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}

func (c *SentryMessagesStreamC) Context() context.Context { return c.ctx }

func (c *SentryMessagesStreamC) RecvMsg(anyMessage interface{}) error {
	m, err := c.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(*sentryproto.InboundMessage)
	proto.Merge(outMessage, m)
	return nil
}

// -- end Messages
// -- start Peers

func (c *SentryClientDirect) PeerEvents(ctx context.Context, in *sentryproto.PeerEventsRequest, opts ...grpc.CallOption) (sentryproto.Sentry_PeerEventsClient, error) {
	ch := make(chan *peersReply, 16384)
	streamServer := &SentryPeersStreamS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(c.server.PeerEvents(in, streamServer))
	}()
	return &SentryPeersStreamC{ch: ch, ctx: ctx}, nil
}

func (c *SentryClientDirect) AddPeer(ctx context.Context, in *sentryproto.AddPeerRequest, opts ...grpc.CallOption) (*sentryproto.AddPeerReply, error) {
	return c.server.AddPeer(ctx, in)
}

func (c *SentryClientDirect) RemovePeer(ctx context.Context, in *sentryproto.RemovePeerRequest, opts ...grpc.CallOption) (*sentryproto.RemovePeerReply, error) {
	return c.server.RemovePeer(ctx, in)
}

type peersReply struct {
	r   *sentryproto.PeerEvent
	err error
}

// SentryPeersStreamS - implements proto_sentryproto.Sentry_ReceivePeersServer
type SentryPeersStreamS struct {
	ch  chan *peersReply
	ctx context.Context
	grpc.ServerStream
}

func (s *SentryPeersStreamS) Send(m *sentryproto.PeerEvent) error {
	s.ch <- &peersReply{r: m}
	return nil
}

func (s *SentryPeersStreamS) Context() context.Context { return s.ctx }

func (s *SentryPeersStreamS) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &peersReply{err: err}
}

type SentryPeersStreamC struct {
	ch  chan *peersReply
	ctx context.Context
	grpc.ClientStream
}

func (c *SentryPeersStreamC) Recv() (*sentryproto.PeerEvent, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}

func (c *SentryPeersStreamC) Context() context.Context { return c.ctx }

func (c *SentryPeersStreamC) RecvMsg(anyMessage interface{}) error {
	m, err := c.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(*sentryproto.PeerEvent)
	proto.Merge(outMessage, m)
	return nil
}

// -- end Peers

func (c *SentryClientDirect) NodeInfo(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.NodeInfoReply, error) {
	return c.server.NodeInfo(ctx, in)
}

func filterIds(in []sentryproto.MessageId, protocol sentryproto.Protocol) (filtered []sentryproto.MessageId) {
	for _, id := range in {
		if _, ok := libsentry.ProtoIds[protocol][id]; ok {
			filtered = append(filtered, id)
		} else if _, ok := libsentry.ProtoIds[sentryproto.Protocol_WIT0][id]; ok {
			// Allow witness messages through ETH protocol clients
			filtered = append(filtered, id)
		} else {
		}
	}
	return filtered
}
