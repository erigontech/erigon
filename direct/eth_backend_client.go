package direct

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type EthBackendClientDirect struct {
	server remote.ETHBACKENDServer
}

func NewEthBackendClientDirect(server remote.ETHBACKENDServer) *EthBackendClientDirect {
	return &EthBackendClientDirect{server: server}
}

func (s *EthBackendClientDirect) Etherbase(ctx context.Context, in *remote.EtherbaseRequest, opts ...grpc.CallOption) (*remote.EtherbaseReply, error) {
	return s.server.Etherbase(ctx, in)
}

func (s *EthBackendClientDirect) NetVersion(ctx context.Context, in *remote.NetVersionRequest, opts ...grpc.CallOption) (*remote.NetVersionReply, error) {
	return s.server.NetVersion(ctx, in)
}

func (s *EthBackendClientDirect) NetPeerCount(ctx context.Context, in *remote.NetPeerCountRequest, opts ...grpc.CallOption) (*remote.NetPeerCountReply, error) {
	return s.server.NetPeerCount(ctx, in)
}

func (s *EthBackendClientDirect) EngineGetPayloadV1(ctx context.Context, in *remote.EngineGetPayloadRequest, opts ...grpc.CallOption) (*types.ExecutionPayload, error) {
	return s.server.EngineGetPayloadV1(ctx, in)
}

func (s *EthBackendClientDirect) EngineNewPayloadV1(ctx context.Context, in *types.ExecutionPayload, opts ...grpc.CallOption) (*remote.EnginePayloadStatus, error) {
	return s.server.EngineNewPayloadV1(ctx, in)
}

func (s *EthBackendClientDirect) EngineForkChoiceUpdatedV1(ctx context.Context, in *remote.EngineForkChoiceUpdatedRequest, opts ...grpc.CallOption) (*remote.EngineForkChoiceUpdatedReply, error) {
	return s.server.EngineForkChoiceUpdatedV1(ctx, in)
}

func (s *EthBackendClientDirect) Version(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*types.VersionReply, error) {
	return s.server.Version(ctx, in)
}

func (s *EthBackendClientDirect) ProtocolVersion(ctx context.Context, in *remote.ProtocolVersionRequest, opts ...grpc.CallOption) (*remote.ProtocolVersionReply, error) {
	return s.server.ProtocolVersion(ctx, in)
}

func (s *EthBackendClientDirect) ClientVersion(ctx context.Context, in *remote.ClientVersionRequest, opts ...grpc.CallOption) (*remote.ClientVersionReply, error) {
	return s.server.ClientVersion(ctx, in)
}

// -- start Subscribe

func (s *EthBackendClientDirect) Subscribe(ctx context.Context, in *remote.SubscribeRequest, opts ...grpc.CallOption) (remote.ETHBACKEND_SubscribeClient, error) {
	messageCh := make(chan *remote.SubscribeReply, 16384)
	streamServer := &SubscribeServerStreamDirect{messageCh: messageCh, ctx: ctx}
	go func() {
		if err := s.server.Subscribe(in, streamServer); err != nil {
			log.Warn("Messages returned", "err", err)
		}
		close(messageCh)
	}()
	return &SubscribeClientStreamDirect{messageCh: messageCh, ctx: ctx}, nil
}

type SubscribeServerStreamDirect struct {
	messageCh chan *remote.SubscribeReply
	ctx       context.Context
	grpc.ServerStream
}

func (s *SubscribeServerStreamDirect) Send(m *remote.SubscribeReply) error {
	s.messageCh <- m
	return nil
}
func (s *SubscribeServerStreamDirect) Context() context.Context {
	return s.ctx
}

type SubscribeClientStreamDirect struct {
	messageCh chan *remote.SubscribeReply
	ctx       context.Context
	grpc.ClientStream
}

func (c *SubscribeClientStreamDirect) Recv() (*remote.SubscribeReply, error) {
	m := <-c.messageCh
	return m, nil
}
func (c *SubscribeClientStreamDirect) Context() context.Context {
	return c.ctx
}

// -- end Subscribe

func (s *EthBackendClientDirect) Block(ctx context.Context, in *remote.BlockRequest, opts ...grpc.CallOption) (*remote.BlockReply, error) {
	return s.server.Block(ctx, in)
}

func (s *EthBackendClientDirect) TxnLookup(ctx context.Context, in *remote.TxnLookupRequest, opts ...grpc.CallOption) (*remote.TxnLookupReply, error) {
	return s.server.TxnLookup(ctx, in)
}

func (s *EthBackendClientDirect) NodeInfo(ctx context.Context, in *remote.NodesInfoRequest, opts ...grpc.CallOption) (*remote.NodesInfoReply, error) {
	return s.server.NodeInfo(ctx, in)
}
