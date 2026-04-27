// Copyright 2026 The Erigon Authors
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

package execution_client

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/erigontech/erigon/cl/phase1/execution_client/rpc_helper"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/jwt"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/rpc"
)

// EngineAPIRPCClient implements engineapi.EngineAPI over HTTP JSON-RPC.
// It is a thin transport layer — no version negotiation, just direct method dispatch.
type EngineAPIRPCClient struct {
	client *rpc.Client
}

func NewEngineAPIRPCClient(jwtSecret []byte, addr string, port int) (*EngineAPIRPCClient, *rpc.Client, error) {
	roundTripper := jwt.NewHttpRoundTripper(http.DefaultTransport, jwtSecret)
	httpClient := &http.Client{Timeout: DefaultRPCHTTPTimeout, Transport: roundTripper}

	hasScheme := strings.HasPrefix(addr, "http")
	isHTTPS := strings.HasPrefix(addr, "https")
	protocol := ""
	if isHTTPS {
		protocol = "https://"
	} else if !hasScheme {
		protocol = "http://"
	}
	rpcClient, err := rpc.DialHTTPWithClient(fmt.Sprintf("%s%s:%d", protocol, addr, port), httpClient, nil)
	if err != nil {
		return nil, nil, err
	}

	return &EngineAPIRPCClient{client: rpcClient}, rpcClient, nil
}

func (c *EngineAPIRPCClient) NewPayloadV1(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	result := &engine_types.PayloadStatus{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineNewPayloadV1, payload); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) NewPayloadV2(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	result := &engine_types.PayloadStatus{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineNewPayloadV2, payload); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) NewPayloadV3(ctx context.Context, payload *engine_types.ExecutionPayload, expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash) (*engine_types.PayloadStatus, error) {
	result := &engine_types.PayloadStatus{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineNewPayloadV3, payload, expectedBlobHashes, parentBeaconBlockRoot); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) NewPayloadV4(ctx context.Context, payload *engine_types.ExecutionPayload, expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash, executionRequests []hexutil.Bytes) (*engine_types.PayloadStatus, error) {
	result := &engine_types.PayloadStatus{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineNewPayloadV4, payload, expectedBlobHashes, parentBeaconBlockRoot, executionRequests); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	result := &engine_types.ForkChoiceUpdatedResponse{}
	if err := c.client.CallContext(ctx, result, rpc_helper.ForkChoiceUpdatedV1, forkChoiceState, payloadAttributes); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) ForkchoiceUpdatedV2(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	result := &engine_types.ForkChoiceUpdatedResponse{}
	if err := c.client.CallContext(ctx, result, rpc_helper.ForkChoiceUpdatedV2, forkChoiceState, payloadAttributes); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) ForkchoiceUpdatedV3(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	result := &engine_types.ForkChoiceUpdatedResponse{}
	if err := c.client.CallContext(ctx, result, rpc_helper.ForkChoiceUpdatedV3, forkChoiceState, payloadAttributes); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) ForkchoiceUpdatedV4(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	result := &engine_types.ForkChoiceUpdatedResponse{}
	if err := c.client.CallContext(ctx, result, rpc_helper.ForkChoiceUpdatedV4, forkChoiceState, payloadAttributes); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.ExecutionPayload, error) {
	result := &engine_types.ExecutionPayload{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineGetPayloadV1, payloadID); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadV2(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	result := &engine_types.GetPayloadResponse{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineGetPayloadV2, payloadID); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadV3(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	result := &engine_types.GetPayloadResponse{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineGetPayloadV3, payloadID); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadV4(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	result := &engine_types.GetPayloadResponse{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineGetPayloadV4, payloadID); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadV5(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	result := &engine_types.GetPayloadResponse{}
	if err := c.client.CallContext(ctx, result, rpc_helper.EngineGetPayloadV5, payloadID); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadBodiesByHashV1(ctx context.Context, hashes []common.Hash) ([]*engine_types.ExecutionPayloadBody, error) {
	var result []*engine_types.ExecutionPayloadBody
	if err := c.client.CallContext(ctx, &result, rpc_helper.GetPayloadBodiesByHashV1, hashes); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadBodiesByHashV2(ctx context.Context, hashes []common.Hash) ([]*engine_types.ExecutionPayloadBodyV2, error) {
	var result []*engine_types.ExecutionPayloadBodyV2
	if err := c.client.CallContext(ctx, &result, rpc_helper.GetPayloadBodiesByHashV2, hashes); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBody, error) {
	var result []*engine_types.ExecutionPayloadBody
	if err := c.client.CallContext(ctx, &result, rpc_helper.GetPayloadBodiesByRangeV1, start, count); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetPayloadBodiesByRangeV2(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBodyV2, error) {
	var result []*engine_types.ExecutionPayloadBodyV2
	if err := c.client.CallContext(ctx, &result, rpc_helper.GetPayloadBodiesByRangeV2, start, count); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetClientVersionV1(ctx context.Context, callerVersion *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
	var result []engine_types.ClientVersionV1
	if err := c.client.CallContext(ctx, &result, rpc_helper.EngineGetClientVersionV1, callerVersion); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetBlobsV1(ctx context.Context, blobHashes []common.Hash) ([]*engine_types.BlobAndProofV1, error) {
	var result []*engine_types.BlobAndProofV1
	if err := c.client.CallContext(ctx, &result, rpc_helper.EngineGetBlobsV1, blobHashes); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetBlobsV2(ctx context.Context, blobHashes []common.Hash) ([]*engine_types.BlobAndProofV2, error) {
	var result []*engine_types.BlobAndProofV2
	if err := c.client.CallContext(ctx, &result, rpc_helper.EngineGetBlobsV2, blobHashes); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *EngineAPIRPCClient) GetBlobsV3(ctx context.Context, blobHashes []common.Hash) ([]*engine_types.BlobAndProofV2, error) {
	var result []*engine_types.BlobAndProofV2
	if err := c.client.CallContext(ctx, &result, rpc_helper.EngineGetBlobsV3, blobHashes); err != nil {
		return nil, err
	}
	return result, nil
}
