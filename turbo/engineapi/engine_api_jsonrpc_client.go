// Copyright 2025 The Erigon Authors
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
	"net/http"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/jwt"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/rpc"
	enginetypes "github.com/erigontech/erigon/turbo/engineapi/engine_types"
)

type JsonRpcClient struct {
	rpcClient *rpc.Client
}

func DialJsonRpcClient(url string, jwtSecret []byte, logger log.Logger) (*JsonRpcClient, error) {
	jwtRoundTripper := jwt.NewHttpRoundTripper(http.DefaultTransport, jwtSecret)
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: jwtRoundTripper}
	client, err := rpc.DialHTTPWithClient(url, httpClient, logger)
	if err != nil {
		return nil, err
	}

	return &JsonRpcClient{rpcClient: client}, nil
}

func (c *JsonRpcClient) NewPayloadV1(ctx context.Context, payload *enginetypes.ExecutionPayload) (*enginetypes.PayloadStatus, error) {
	var result enginetypes.PayloadStatus
	err := c.rpcClient.CallContext(ctx, &result, "engine_newPayloadV1", payload)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) NewPayloadV2(ctx context.Context, payload *enginetypes.ExecutionPayload) (*enginetypes.PayloadStatus, error) {
	var result enginetypes.PayloadStatus
	err := c.rpcClient.CallContext(ctx, &result, "engine_newPayloadV2", payload)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) NewPayloadV3(
	ctx context.Context,
	executionPayload *enginetypes.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash,
	parentBeaconBlockRoot *libcommon.Hash,
) (*enginetypes.PayloadStatus, error) {
	var result enginetypes.PayloadStatus
	err := c.rpcClient.CallContext(
		ctx,
		&result,
		"engine_newPayloadV3",
		executionPayload,
		expectedBlobHashes,
		parentBeaconBlockRoot,
	)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) NewPayloadV4(
	ctx context.Context,
	executionPayload *enginetypes.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash,
	parentBeaconBlockRoot *libcommon.Hash,
	executionRequests []hexutil.Bytes,
) (*enginetypes.PayloadStatus, error) {
	var result enginetypes.PayloadStatus
	err := c.rpcClient.CallContext(
		ctx,
		&result,
		"engine_newPayloadV4",
		executionPayload,
		expectedBlobHashes,
		parentBeaconBlockRoot,
		executionRequests,
	)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) ForkchoiceUpdatedV1(
	ctx context.Context,
	forkChoiceState *enginetypes.ForkChoiceState,
	payloadAttributes *enginetypes.PayloadAttributes,
) (*enginetypes.ForkChoiceUpdatedResponse, error) {
	var result enginetypes.ForkChoiceUpdatedResponse
	err := c.rpcClient.CallContext(ctx, &result, "engine_forkchoiceUpdatedV1", forkChoiceState, payloadAttributes)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) ForkchoiceUpdatedV2(
	ctx context.Context,
	forkChoiceState *enginetypes.ForkChoiceState,
	payloadAttributes *enginetypes.PayloadAttributes,
) (*enginetypes.ForkChoiceUpdatedResponse, error) {
	var result enginetypes.ForkChoiceUpdatedResponse
	err := c.rpcClient.CallContext(ctx, &result, "engine_forkchoiceUpdatedV2", forkChoiceState, payloadAttributes)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) ForkchoiceUpdatedV3(
	ctx context.Context,
	forkChoiceState *enginetypes.ForkChoiceState,
	payloadAttributes *enginetypes.PayloadAttributes,
) (*enginetypes.ForkChoiceUpdatedResponse, error) {
	var result enginetypes.ForkChoiceUpdatedResponse
	err := c.rpcClient.CallContext(ctx, &result, "engine_forkchoiceUpdatedV3", forkChoiceState, payloadAttributes)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*enginetypes.ExecutionPayload, error) {
	var result enginetypes.ExecutionPayload
	err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadV1", payloadID)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) GetPayloadV2(ctx context.Context, payloadID hexutil.Bytes) (*enginetypes.GetPayloadResponse, error) {
	var result enginetypes.GetPayloadResponse
	err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadV2", payloadID)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) GetPayloadV3(ctx context.Context, payloadID hexutil.Bytes) (*enginetypes.GetPayloadResponse, error) {
	var result enginetypes.GetPayloadResponse
	err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadV3", payloadID)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) GetPayloadV4(ctx context.Context, payloadID hexutil.Bytes) (*enginetypes.GetPayloadResponse, error) {
	var result enginetypes.GetPayloadResponse
	err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadV4", payloadID)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *JsonRpcClient) GetPayloadBodiesByHashV1(ctx context.Context, hashes []libcommon.Hash) ([]*enginetypes.ExecutionPayloadBody, error) {
	var result []*enginetypes.ExecutionPayloadBody
	err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadBodiesByHashV1", hashes)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *JsonRpcClient) GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*enginetypes.ExecutionPayloadBody, error) {
	var result []*enginetypes.ExecutionPayloadBody
	err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadBodiesByRangeV1", start, count)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *JsonRpcClient) GetClientVersionV1(ctx context.Context, callerVersion *enginetypes.ClientVersionV1) ([]enginetypes.ClientVersionV1, error) {
	var result []enginetypes.ClientVersionV1
	err := c.rpcClient.CallContext(ctx, &result, "engine_getClientVersionV1", callerVersion)
	if err != nil {
		return nil, err
	}
	return result, nil
}
