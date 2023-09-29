package engineapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
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

func (e *EngineServer) GetPayloadV1(ctx context.Context, payloadId hexutility.Bytes) (*engine_types.ExecutionPayload, error) {

	decodedPayloadId := binary.BigEndian.Uint64(payloadId)
	e.logger.Info("Received GetPayloadV1", "payloadId", decodedPayloadId)

	response, err := e.getPayload(ctx, decodedPayloadId, clparams.BellatrixVersion)
	if err != nil {
		return nil, err
	}

	return response.ExecutionPayload, nil
}

func (e *EngineServer) GetPayloadV2(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	e.logger.Info("Received GetPayloadV2", "payloadId", decodedPayloadId)
	return e.getPayload(ctx, decodedPayloadId, clparams.CapellaVersion)
}

func (e *EngineServer) GetPayloadV3(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	e.logger.Info("Received GetPayloadV3", "payloadId", decodedPayloadId)
	return e.getPayload(ctx, decodedPayloadId, clparams.DenebVersion)
}

func (e *EngineServer) ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.BellatrixVersion)
}

func (e *EngineServer) ForkchoiceUpdatedV2(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.CapellaVersion)
}

func (e *EngineServer) ForkchoiceUpdatedV3(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.DenebVersion)
}

// NewPayloadV1 processes new payloads (blocks) from the beacon chain without withdrawals.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_newpayloadv1
func (e *EngineServer) NewPayloadV1(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, nil, nil, clparams.BellatrixVersion)
}

// NewPayloadV2 processes new payloads (blocks) from the beacon chain with withdrawals.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_newpayloadv2
func (e *EngineServer) NewPayloadV2(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, nil, nil, clparams.CapellaVersion)
}

// NewPayloadV3 processes new payloads (blocks) from the beacon chain with withdrawals & blob gas.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#engine_newpayloadv3
func (e *EngineServer) NewPayloadV3(ctx context.Context, payload *engine_types.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash, parentBeaconBlockRoot *libcommon.Hash) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, expectedBlobHashes, parentBeaconBlockRoot, clparams.DenebVersion)
}

// Receives consensus layer's transition configuration and checks if the execution layer has the correct configuration.
// Can also be used to ping the execution layer (heartbeats).
// See https://github.com/ethereum/execution-apis/blob/v1.0.0-beta.1/src/engine/specification.md#engine_exchangetransitionconfigurationv1
func (e *EngineServer) ExchangeTransitionConfigurationV1(ctx context.Context, beaconConfig *engine_types.TransitionConfiguration) (*engine_types.TransitionConfiguration, error) {
	terminalTotalDifficulty := e.config.TerminalTotalDifficulty

	if terminalTotalDifficulty == nil {
		return nil, fmt.Errorf("the execution layer doesn't have a terminal total difficulty. expected: %v", beaconConfig.TerminalTotalDifficulty)
	}

	if terminalTotalDifficulty.Cmp((*big.Int)(beaconConfig.TerminalTotalDifficulty)) != 0 {
		return nil, fmt.Errorf("the execution layer has a wrong terminal total difficulty. expected %v, but instead got: %d", beaconConfig.TerminalTotalDifficulty, terminalTotalDifficulty)
	}

	return &engine_types.TransitionConfiguration{
		TerminalTotalDifficulty: (*hexutil.Big)(terminalTotalDifficulty),
		TerminalBlockHash:       libcommon.Hash{},
		TerminalBlockNumber:     (*hexutil.Big)(libcommon.Big0),
	}, nil
}

func (e *EngineServer) GetPayloadBodiesByHashV1(ctx context.Context, hashes []libcommon.Hash) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	if len(hashes) > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}

	return e.getPayloadBodiesByHash(ctx, hashes, clparams.DenebVersion)
}

func (e *EngineServer) GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	if start == 0 || count == 0 {
		return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("invalid start or count, start: %v count: %v", start, count)}
	}
	if count > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}

	return e.getPayloadBodiesByRange(ctx, uint64(start), uint64(count), clparams.CapellaVersion)
}
