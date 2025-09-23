// Copyright 2024 The Erigon Authors
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

package engineapi

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

// EngineAPI Beacon chain communication endpoint
type EngineAPI interface {
	NewPayloadV1(context.Context, *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error)
	NewPayloadV2(context.Context, *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error)
	NewPayloadV3(ctx context.Context, executionPayload *engine_types.ExecutionPayload, expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash) (*engine_types.PayloadStatus, error)
	NewPayloadV4(ctx context.Context, executionPayload *engine_types.ExecutionPayload, expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash, executionRequests []hexutil.Bytes) (*engine_types.PayloadStatus, error)
	ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error)
	ForkchoiceUpdatedV2(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error)
	ForkchoiceUpdatedV3(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error)
	GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.ExecutionPayload, error)
	GetPayloadV2(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error)
	GetPayloadV3(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error)
	GetPayloadV4(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error)
	GetPayloadV5(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error)
	GetPayloadBodiesByHashV1(ctx context.Context, hashes []common.Hash) ([]*engine_types.ExecutionPayloadBody, error)
	GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBody, error)
	GetClientVersionV1(ctx context.Context, callerVersion *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error)
	GetBlobsV1(ctx context.Context, blobHashes []common.Hash) ([]*engine_types.BlobAndProofV1, error)
}
