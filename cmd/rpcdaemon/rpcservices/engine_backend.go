package rpcservices

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/engine"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

type EngineBackend struct {
	server engine.EngineClient
}

func NewEngineBackend(server engine.EngineClient) *EngineBackend {
	return &EngineBackend{server: server}
}

func (back *EngineBackend) EngineNewPayload(ctx context.Context, payload *types2.ExecutionPayload) (res *engine.EnginePayloadStatus, err error) {
	return back.server.EngineNewPayload(ctx, payload)
}

func (back *EngineBackend) EngineForkchoiceUpdated(ctx context.Context, request *engine.EngineForkChoiceUpdatedRequest) (*engine.EngineForkChoiceUpdatedResponse, error) {
	return back.server.EngineForkChoiceUpdated(ctx, request)
}

func (back *EngineBackend) EngineGetPayload(ctx context.Context, payloadId uint64) (res *engine.EngineGetPayloadResponse, err error) {
	return back.server.EngineGetPayload(ctx, &engine.EngineGetPayloadRequest{
		PayloadId: payloadId,
	})
}

func (back *EngineBackend) EngineGetPayloadBodiesByHashV1(ctx context.Context, request *engine.EngineGetPayloadBodiesByHashV1Request) (*engine.EngineGetPayloadBodiesV1Response, error) {
	return back.server.EngineGetPayloadBodiesByHashV1(ctx, request)
}

func (back *EngineBackend) EngineGetPayloadBodiesByRangeV1(ctx context.Context, request *engine.EngineGetPayloadBodiesByRangeV1Request) (*engine.EngineGetPayloadBodiesV1Response, error) {
	return back.server.EngineGetPayloadBodiesByRangeV1(ctx, request)
}
