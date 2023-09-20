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
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

const (
	ETH65 = 65
	ETH66 = 66
	ETH67 = 67
	ETH68 = 68
)

var ProtoIds = map[uint]map[sentry.MessageId]struct{}{
	ETH65: {
		sentry.MessageId_GET_BLOCK_HEADERS_65:             struct{}{},
		sentry.MessageId_BLOCK_HEADERS_65:                 struct{}{},
		sentry.MessageId_GET_BLOCK_BODIES_65:              struct{}{},
		sentry.MessageId_BLOCK_BODIES_65:                  struct{}{},
		sentry.MessageId_GET_NODE_DATA_65:                 struct{}{},
		sentry.MessageId_NODE_DATA_65:                     struct{}{},
		sentry.MessageId_GET_RECEIPTS_65:                  struct{}{},
		sentry.MessageId_RECEIPTS_65:                      struct{}{},
		sentry.MessageId_NEW_BLOCK_HASHES_65:              struct{}{},
		sentry.MessageId_NEW_BLOCK_65:                     struct{}{},
		sentry.MessageId_TRANSACTIONS_65:                  struct{}{},
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65: struct{}{},
		sentry.MessageId_GET_POOLED_TRANSACTIONS_65:       struct{}{},
		sentry.MessageId_POOLED_TRANSACTIONS_65:           struct{}{},
	},
	ETH66: {
		sentry.MessageId_GET_BLOCK_HEADERS_66:             struct{}{},
		sentry.MessageId_BLOCK_HEADERS_66:                 struct{}{},
		sentry.MessageId_GET_BLOCK_BODIES_66:              struct{}{},
		sentry.MessageId_BLOCK_BODIES_66:                  struct{}{},
		sentry.MessageId_GET_NODE_DATA_66:                 struct{}{},
		sentry.MessageId_NODE_DATA_66:                     struct{}{},
		sentry.MessageId_GET_RECEIPTS_66:                  struct{}{},
		sentry.MessageId_RECEIPTS_66:                      struct{}{},
		sentry.MessageId_NEW_BLOCK_HASHES_66:              struct{}{},
		sentry.MessageId_NEW_BLOCK_66:                     struct{}{},
		sentry.MessageId_TRANSACTIONS_66:                  struct{}{},
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66: struct{}{},
		sentry.MessageId_GET_POOLED_TRANSACTIONS_66:       struct{}{},
		sentry.MessageId_POOLED_TRANSACTIONS_66:           struct{}{},
	},
	ETH67: {
		sentry.MessageId_GET_BLOCK_HEADERS_66:             struct{}{},
		sentry.MessageId_BLOCK_HEADERS_66:                 struct{}{},
		sentry.MessageId_GET_BLOCK_BODIES_66:              struct{}{},
		sentry.MessageId_BLOCK_BODIES_66:                  struct{}{},
		sentry.MessageId_GET_RECEIPTS_66:                  struct{}{},
		sentry.MessageId_RECEIPTS_66:                      struct{}{},
		sentry.MessageId_NEW_BLOCK_HASHES_66:              struct{}{},
		sentry.MessageId_NEW_BLOCK_66:                     struct{}{},
		sentry.MessageId_TRANSACTIONS_66:                  struct{}{},
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66: struct{}{},
		sentry.MessageId_GET_POOLED_TRANSACTIONS_66:       struct{}{},
		sentry.MessageId_POOLED_TRANSACTIONS_66:           struct{}{},
	},
	ETH68: {
		sentry.MessageId_GET_BLOCK_HEADERS_66:             struct{}{},
		sentry.MessageId_BLOCK_HEADERS_66:                 struct{}{},
		sentry.MessageId_GET_BLOCK_BODIES_66:              struct{}{},
		sentry.MessageId_BLOCK_BODIES_66:                  struct{}{},
		sentry.MessageId_GET_RECEIPTS_66:                  struct{}{},
		sentry.MessageId_RECEIPTS_66:                      struct{}{},
		sentry.MessageId_NEW_BLOCK_HASHES_66:              struct{}{},
		sentry.MessageId_NEW_BLOCK_66:                     struct{}{},
		sentry.MessageId_TRANSACTIONS_66:                  struct{}{},
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68: struct{}{},
		sentry.MessageId_GET_POOLED_TRANSACTIONS_66:       struct{}{},
		sentry.MessageId_POOLED_TRANSACTIONS_66:           struct{}{},
	},
}

type SentryClient interface {
	sentry.SentryClient
	Protocol() uint
	Ready() bool
	MarkDisconnected()
}

type SentryClientRemote struct {
	sentry.SentryClient
	sync.RWMutex
	protocol uint
	ready    bool
}

var _ SentryClient = (*SentryClientRemote)(nil) // compile-time interface check
var _ SentryClient = (*SentryClientDirect)(nil) // compile-time interface check

// NewSentryClientRemote - app code must use this class
// to avoid concurrency - it accepts protocol (which received async by SetStatus) in constructor,
// means app can't use client which protocol unknown yet
func NewSentryClientRemote(client sentry.SentryClient) *SentryClientRemote {
	return &SentryClientRemote{SentryClient: client}
}

func (c *SentryClientRemote) Protocol() uint {
	c.RLock()
	defer c.RUnlock()
	return c.protocol
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

func (c *SentryClientRemote) HandShake(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentry.HandShakeReply, error) {
	reply, err := c.SentryClient.HandShake(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	c.Lock()
	defer c.Unlock()
	switch reply.Protocol {
	case sentry.Protocol_ETH65:
		c.protocol = ETH65
	case sentry.Protocol_ETH66:
		c.protocol = ETH66
	case sentry.Protocol_ETH67:
		c.protocol = ETH67
	case sentry.Protocol_ETH68:
		c.protocol = ETH68
	default:
		return nil, fmt.Errorf("unexpected protocol: %d", reply.Protocol)
	}
	c.ready = true
	return reply, nil
}
func (c *SentryClientRemote) SetStatus(ctx context.Context, in *sentry.StatusData, opts ...grpc.CallOption) (*sentry.SetStatusReply, error) {
	return c.SentryClient.SetStatus(ctx, in, opts...)
}

func (c *SentryClientRemote) Messages(ctx context.Context, in *sentry.MessagesRequest, opts ...grpc.CallOption) (sentry.Sentry_MessagesClient, error) {
	in.Ids = filterIds(in.Ids, c.Protocol())
	return c.SentryClient.Messages(ctx, in, opts...)
}

func (c *SentryClientRemote) PeerCount(ctx context.Context, in *sentry.PeerCountRequest, opts ...grpc.CallOption) (*sentry.PeerCountReply, error) {
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
	server   sentry.SentryServer
	protocol uint
}

func NewSentryClientDirect(protocol uint, sentryServer sentry.SentryServer) *SentryClientDirect {
	return &SentryClientDirect{protocol: protocol, server: sentryServer}
}

func (c *SentryClientDirect) Protocol() uint    { return c.protocol }
func (c *SentryClientDirect) Ready() bool       { return true }
func (c *SentryClientDirect) MarkDisconnected() {}

func (c *SentryClientDirect) PenalizePeer(ctx context.Context, in *sentry.PenalizePeerRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.PenalizePeer(ctx, in)
}

func (c *SentryClientDirect) PeerMinBlock(ctx context.Context, in *sentry.PeerMinBlockRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.server.PeerMinBlock(ctx, in)
}

func (c *SentryClientDirect) SendMessageByMinBlock(ctx context.Context, in *sentry.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*sentry.SentPeers, error) {
	return c.server.SendMessageByMinBlock(ctx, in)
}

func (c *SentryClientDirect) SendMessageById(ctx context.Context, in *sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentry.SentPeers, error) {
	return c.server.SendMessageById(ctx, in)
}

func (c *SentryClientDirect) SendMessageToRandomPeers(ctx context.Context, in *sentry.SendMessageToRandomPeersRequest, opts ...grpc.CallOption) (*sentry.SentPeers, error) {
	return c.server.SendMessageToRandomPeers(ctx, in)
}

func (c *SentryClientDirect) SendMessageToAll(ctx context.Context, in *sentry.OutboundMessageData, opts ...grpc.CallOption) (*sentry.SentPeers, error) {
	return c.server.SendMessageToAll(ctx, in)
}

func (c *SentryClientDirect) HandShake(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentry.HandShakeReply, error) {
	return c.server.HandShake(ctx, in)
}

func (c *SentryClientDirect) SetStatus(ctx context.Context, in *sentry.StatusData, opts ...grpc.CallOption) (*sentry.SetStatusReply, error) {
	return c.server.SetStatus(ctx, in)
}

func (c *SentryClientDirect) Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentry.PeersReply, error) {
	return c.server.Peers(ctx, in)
}

func (c *SentryClientDirect) PeerCount(ctx context.Context, in *sentry.PeerCountRequest, opts ...grpc.CallOption) (*sentry.PeerCountReply, error) {
	return c.server.PeerCount(ctx, in)
}

func (c *SentryClientDirect) PeerById(ctx context.Context, in *sentry.PeerByIdRequest, opts ...grpc.CallOption) (*sentry.PeerByIdReply, error) {
	return c.server.PeerById(ctx, in)
}

// -- start Messages

func (c *SentryClientDirect) Messages(ctx context.Context, in *sentry.MessagesRequest, opts ...grpc.CallOption) (sentry.Sentry_MessagesClient, error) {
	in.Ids = filterIds(in.Ids, c.Protocol())
	ch := make(chan *inboundMessageReply, 16384)
	streamServer := &SentryMessagesStreamS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(c.server.Messages(in, streamServer))
	}()
	return &SentryMessagesStreamC{ch: ch, ctx: ctx}, nil
}

type inboundMessageReply struct {
	r   *sentry.InboundMessage
	err error
}

// SentryMessagesStreamS implements proto_sentry.Sentry_ReceiveMessagesServer
type SentryMessagesStreamS struct {
	ch  chan *inboundMessageReply
	ctx context.Context
	grpc.ServerStream
}

func (s *SentryMessagesStreamS) Send(m *sentry.InboundMessage) error {
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

func (c *SentryMessagesStreamC) Recv() (*sentry.InboundMessage, error) {
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
	outMessage := anyMessage.(*sentry.InboundMessage)
	proto.Merge(outMessage, m)
	return nil
}

// -- end Messages
// -- start Peers

func (c *SentryClientDirect) PeerEvents(ctx context.Context, in *sentry.PeerEventsRequest, opts ...grpc.CallOption) (sentry.Sentry_PeerEventsClient, error) {
	ch := make(chan *peersReply, 16384)
	streamServer := &SentryPeersStreamS{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(c.server.PeerEvents(in, streamServer))
	}()
	return &SentryPeersStreamC{ch: ch, ctx: ctx}, nil
}

func (c *SentryClientDirect) AddPeer(ctx context.Context, in *sentry.AddPeerRequest, opts ...grpc.CallOption) (*sentry.AddPeerReply, error) {
	return c.server.AddPeer(ctx, in)
}

type peersReply struct {
	r   *sentry.PeerEvent
	err error
}

// SentryPeersStreamS - implements proto_sentry.Sentry_ReceivePeersServer
type SentryPeersStreamS struct {
	ch  chan *peersReply
	ctx context.Context
	grpc.ServerStream
}

func (s *SentryPeersStreamS) Send(m *sentry.PeerEvent) error {
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

func (c *SentryPeersStreamC) Recv() (*sentry.PeerEvent, error) {
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
	outMessage := anyMessage.(*sentry.PeerEvent)
	proto.Merge(outMessage, m)
	return nil
}

// -- end Peers

func (c *SentryClientDirect) NodeInfo(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.NodeInfoReply, error) {
	return c.server.NodeInfo(ctx, in)
}

func filterIds(in []sentry.MessageId, protocol uint) (filtered []sentry.MessageId) {
	for _, id := range in {
		if _, ok := ProtoIds[protocol][id]; ok {
			filtered = append(filtered, id)
		}
	}
	return filtered
}
