package engineapi

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
)

// EngineAPI Beacon chain communication endpoint
type EngineAPI interface {
	NewPayloadV1(context.Context, *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error)
	NewPayloadV2(context.Context, *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error)
	NewPayloadV3(ctx context.Context, executionPayload *engine_types.ExecutionPayload, expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash) (*engine_types.PayloadStatus, error)
	ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error)
	ForkchoiceUpdatedV2(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error)
	ForkchoiceUpdatedV3(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error)
	GetPayloadV1(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.ExecutionPayload, error)
	GetPayloadV2(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error)
	GetPayloadV3(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error)
	ExchangeTransitionConfigurationV1(ctx context.Context, transitionConfiguration *engine_types.TransitionConfiguration) (*engine_types.TransitionConfiguration, error)
	GetPayloadBodiesByHashV1(ctx context.Context, hashes []common.Hash) ([]*engine_types.ExecutionPayloadBodyV1, error)
	GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBodyV1, error)
}
