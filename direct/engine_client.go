package direct

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/engine"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type EngineClient struct {
	server engine.EngineServer
}

func NewEngineClient(server engine.EngineServer) *EngineClient {
	return &EngineClient{server: server}
}

func (s *EngineClient) EngineNewPayload(ctx context.Context, in *types.ExecutionPayload, opts ...grpc.CallOption) (*engine.EnginePayloadStatus, error) {
	return s.server.EngineNewPayload(ctx, in)
}

func (s *EngineClient) EngineForkChoiceUpdated(ctx context.Context, in *engine.EngineForkChoiceUpdatedRequest, opts ...grpc.CallOption) (*engine.EngineForkChoiceUpdatedResponse, error) {
	return s.server.EngineForkChoiceUpdated(ctx, in)
}

func (s *EngineClient) EngineGetPayload(ctx context.Context, in *engine.EngineGetPayloadRequest, opts ...grpc.CallOption) (*engine.EngineGetPayloadResponse, error) {
	return s.server.EngineGetPayload(ctx, in)
}

func (s *EngineClient) EngineGetPayloadBodiesByHashV1(ctx context.Context, in *engine.EngineGetPayloadBodiesByHashV1Request, opts ...grpc.CallOption) (*engine.EngineGetPayloadBodiesV1Response, error) {
	return s.server.EngineGetPayloadBodiesByHashV1(ctx, in)
}

func (s *EngineClient) EngineGetPayloadBodiesByRangeV1(ctx context.Context, in *engine.EngineGetPayloadBodiesByRangeV1Request, opts ...grpc.CallOption) (*engine.EngineGetPayloadBodiesV1Response, error) {
	return s.server.EngineGetPayloadBodiesByRangeV1(ctx, in)
}

func (s *EngineClient) EngineGetBlobsBundleV1(ctx context.Context, in *engine.EngineGetBlobsBundleRequest, opts ...grpc.CallOption) (*types.BlobsBundleV1, error) {
	return s.server.EngineGetBlobsBundleV1(ctx, in)
}

func (s *EngineClient) PendingBlock(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*engine.PendingBlockReply, error) {
	return s.server.PendingBlock(ctx, in)
}
