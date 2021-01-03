package download

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	proto_core "github.com/ledgerwatch/turbo-geth/cmd/headers/core"
	proto_sentry "github.com/ledgerwatch/turbo-geth/cmd/headers/sentry"

	"google.golang.org/grpc"
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

// ControlClientDirect implement ControlClient interface by connecting the instance of the client directly with the corresponding
// instance of ControlServer
type ControlClientDirect struct {
	server proto_core.ControlServer
}

// SetServer inject a reference to the ControlServer into the client
func (ccd *ControlClientDirect) SetServer(controlServer proto_core.ControlServer) {
	ccd.server = controlServer
}

func (ccd *ControlClientDirect) ForwardInboundMessage(ctx context.Context, in *proto_core.InboundMessage, opts ...grpc.CallOption) (*empty.Empty, error) {
	return ccd.server.ForwardInboundMessage(ctx, in)
}

func (ccd *ControlClientDirect) GetStatus(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*proto_core.StatusData, error) {
	return ccd.server.GetStatus(ctx, in)
}
