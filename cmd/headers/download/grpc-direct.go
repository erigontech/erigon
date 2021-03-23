package download

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/log"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Contains implementations of SentryServer, SentryClient, ControlClient, and ControlServer, that may be linked to each other
// SentryClient is linked directly to the SentryServer, for example, so any function call on the instance of the SentryClient
// cause invocations directly on the corresponding instance of the SentryServer. However, the link between SentryClient and
// SentryServer is established outside of the constructor. This means that the reference from the SentyClient to the corresponding
// SentryServer can be injected at any point in time.

// SentryClientDirect implements SentryClient interface by connecting the instance of the client directly with the corresponding
// instance of SentryServer
type SentryClientDirect struct {
	server proto_sentry.SentryServer
}

// SetServer injects a reference to the SentryServer into the client
func (scd *SentryClientDirect) SetServer(sentryServer proto_sentry.SentryServer) {
	scd.server = sentryServer
}

func (scd *SentryClientDirect) PenalizePeer(ctx context.Context, in *proto_sentry.PenalizePeerRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return scd.server.PenalizePeer(ctx, in)
}

func (scd *SentryClientDirect) PeerMinBlock(ctx context.Context, in *proto_sentry.PeerMinBlockRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return scd.server.PeerMinBlock(ctx, in)
}

func (scd *SentryClientDirect) SendMessageByMinBlock(ctx context.Context, in *proto_sentry.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return scd.server.SendMessageByMinBlock(ctx, in)
}

func (scd *SentryClientDirect) SendMessageById(ctx context.Context, in *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return scd.server.SendMessageById(ctx, in)
}

func (scd *SentryClientDirect) SendMessageToRandomPeers(ctx context.Context, in *proto_sentry.SendMessageToRandomPeersRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return scd.server.SendMessageToRandomPeers(ctx, in)
}

func (scd *SentryClientDirect) SendMessageToAll(ctx context.Context, in *proto_sentry.OutboundMessageData, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
	return scd.server.SendMessageToAll(ctx, in)
}

func (scd *SentryClientDirect) SetStatus(ctx context.Context, in *proto_sentry.StatusData, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return scd.server.SetStatus(ctx, in)
}

// implements proto_sentry.Sentry_ReceiveMessagesServer
type SentryReceiveServerDirect struct {
	messageCh chan *proto_sentry.InboundMessage
	grpc.ServerStream
}

func (s *SentryReceiveServerDirect) Send(m *proto_sentry.InboundMessage) error {
	s.messageCh <- m
	return nil
}

type SentryReceiveClientDirect struct {
	messageCh chan *proto_sentry.InboundMessage
	grpc.ClientStream
}

func (c *SentryReceiveClientDirect) Recv() (*proto_sentry.InboundMessage, error) {
	m := <-c.messageCh
	return m, nil
}

func (scd *SentryClientDirect) ReceiveMessages(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (proto_sentry.Sentry_ReceiveMessagesClient, error) {
	messageCh := make(chan *proto_sentry.InboundMessage, 16384)
	streamServer := &SentryReceiveServerDirect{messageCh: messageCh}
	go func() {
		if err := scd.server.ReceiveMessages(&empty.Empty{}, streamServer); err != nil {
			log.Error("ReceiveMessages returned", "error", err)
		}
		close(messageCh)
	}()
	return &SentryReceiveClientDirect{messageCh: messageCh}, nil
}

func (scd *SentryClientDirect) ReceiveUploadMessages(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (proto_sentry.Sentry_ReceiveUploadMessagesClient, error) {
	messageCh := make(chan *proto_sentry.InboundMessage, 16384)
	streamServer := &SentryReceiveServerDirect{messageCh: messageCh}
	go func() {
		if err := scd.server.ReceiveUploadMessages(&empty.Empty{}, streamServer); err != nil {
			log.Error("ReceiveUploadMessages returned", "error", err)
		}
		close(messageCh)
	}()
	return &SentryReceiveClientDirect{messageCh: messageCh}, nil
}

func (scd *SentryClientDirect) ReceiveTxMessages(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (proto_sentry.Sentry_ReceiveTxMessagesClient, error) {
	messageCh := make(chan *proto_sentry.InboundMessage, 16384)
	streamServer := &SentryReceiveServerDirect{messageCh: messageCh}
	go func() {
		if err := scd.server.ReceiveTxMessages(&empty.Empty{}, streamServer); err != nil {
			log.Error("ReceiveTxMessages returned", "error", err)
		}
		close(messageCh)
	}()
	return &SentryReceiveClientDirect{messageCh: messageCh}, nil
}
