package remote

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	proto_sentry "github.com/ledgerwatch/erigon/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/log"

	"google.golang.org/grpc"
)

type SentryClient interface {
	proto_sentry.SentryClient
	Protocol() uint
	Ready() bool
	MarkDisconnected()
}

type SentryClientRemote struct {
	proto_sentry.SentryClient
	sync.RWMutex
	protocol uint
	ready    bool
}

// NewSentryClientRemote - app code must use this class
// to avoid concurrency - it accepts protocol (which received async by SetStatus) in constructor,
// means app can't use client which protocol unknown yet
func NewSentryClientRemote(client proto_sentry.SentryClient) *SentryClientRemote {
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

func (c *SentryClientRemote) SetStatus(ctx context.Context, in *proto_sentry.StatusData, opts ...grpc.CallOption) (*proto_sentry.SetStatusReply, error) {
	reply, err := c.SentryClient.SetStatus(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	c.Lock()
	defer c.Unlock()
	switch reply.Protocol {
	case proto_sentry.Protocol_ETH65:
		c.protocol = eth.ETH65
	case proto_sentry.Protocol_ETH66:
		c.protocol = eth.ETH66
	default:
		return nil, fmt.Errorf("unexpected protocol: %d", reply.Protocol)
	}
	c.ready = true
	return reply, nil
}

func (c *SentryClientRemote) Messages(ctx context.Context, in *proto_sentry.MessagesRequest, opts ...grpc.CallOption) (proto_sentry.Sentry_MessagesClient, error) {
	in.Ids = filterIds(in.Ids, c.Protocol())
	return c.SentryClient.Messages(ctx, in, opts...)
}

// Contains implementations of SentryServer, SentryClient, ControlClient, and ControlServer, that may be linked to each other
// SentryClient is linked directly to the SentryServer, for example, so any function call on the instance of the SentryClient
// cause invocations directly on the corresponding instance of the SentryServer. However, the link between SentryClient and
// SentryServer is established outside of the constructor. This means that the reference from the SentyClient to the corresponding
// SentryServer can be injected at any point in time.

// SentryClientDirect implements SentryClient interface by connecting the instance of the client directly with the corresponding
// instance of SentryServer
type SentryClientDirect struct {
	protocol uint
	server   proto_sentry.SentryServer
}

func NewSentryClientDirect(protocol uint, sentryServer proto_sentry.SentryServer) *SentryClientDirect {
	return &SentryClientDirect{protocol: protocol, server: sentryServer}
}

func (c *SentryClientDirect) Protocol() uint    { return c.protocol }
func (c *SentryClientDirect) Ready() bool       { return true }
func (c *SentryClientDirect) MarkDisconnected() {}

func (c *SentryClientDirect) PenalizePeer(ctx context.Context, in *proto_sentry.PenalizePeerRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return c.server.PenalizePeer(ctx, in)
}

func (c *SentryClientDirect) PeerMinBlock(ctx context.Context, in *proto_sentry.PeerMinBlockRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return c.server.PeerMinBlock(ctx, in)
}

func (c *SentryClientDirect) SendMessageByMinBlock(ctx context.Context, in *proto_sentry.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return c.server.SendMessageByMinBlock(ctx, in)
}

func (c *SentryClientDirect) SendMessageById(ctx context.Context, in *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return c.server.SendMessageById(ctx, in)
}

func (c *SentryClientDirect) SendMessageToRandomPeers(ctx context.Context, in *proto_sentry.SendMessageToRandomPeersRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return c.server.SendMessageToRandomPeers(ctx, in)
}

func (c *SentryClientDirect) SendMessageToAll(ctx context.Context, in *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return c.server.SendMessageToAll(ctx, in)
}

func (c *SentryClientDirect) SetStatus(ctx context.Context, in *proto_sentry.StatusData, opts ...grpc.CallOption) (*proto_sentry.SetStatusReply, error) {
	return c.server.SetStatus(ctx, in)
}

// implements proto_sentry.Sentry_ReceiveMessagesServer
type SentryReceiveServerDirect struct {
	messageCh chan *proto_sentry.InboundMessage
	ctx       context.Context
	grpc.ServerStream
}

func (s *SentryReceiveServerDirect) Send(m *proto_sentry.InboundMessage) error {
	s.messageCh <- m
	return nil
}
func (s *SentryReceiveServerDirect) Context() context.Context {
	return s.ctx
}

type SentryReceiveClientDirect struct {
	messageCh chan *proto_sentry.InboundMessage
	ctx       context.Context
	grpc.ClientStream
}

func (c *SentryReceiveClientDirect) Recv() (*proto_sentry.InboundMessage, error) {
	m := <-c.messageCh
	return m, nil
}
func (c *SentryReceiveClientDirect) Context() context.Context {
	return c.ctx
}

func (c *SentryClientDirect) Messages(ctx context.Context, in *proto_sentry.MessagesRequest, opts ...grpc.CallOption) (proto_sentry.Sentry_MessagesClient, error) {
	in.Ids = filterIds(in.Ids, c.Protocol())
	messageCh := make(chan *proto_sentry.InboundMessage, 16384)
	streamServer := &SentryReceiveServerDirect{messageCh: messageCh, ctx: ctx}
	go func() {
		if err := c.server.Messages(in, streamServer); err != nil {
			log.Error("ReceiveMessages returned", "error", err)
		}
		close(messageCh)
	}()
	return &SentryReceiveClientDirect{messageCh: messageCh, ctx: ctx}, nil
}

func filterIds(in []proto_sentry.MessageId, protocol uint) (filtered []proto_sentry.MessageId) {
	for _, id := range in {
		if _, ok := eth.FromProto[protocol][id]; ok {
			filtered = append(filtered, id)
		}
	}
	return filtered
}
