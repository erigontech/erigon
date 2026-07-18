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

func call[T any](ctx context.Context, client *rpc.Client, method string, args ...any) (*T, error) {
	result := new(T)
	if err := client.CallContext(ctx, result, method, args...); err != nil {
		return nil, err
	}
	return result, nil
}

func callValue[T any](ctx context.Context, client *rpc.Client, method string, args ...any) (T, error) {
	result, err := call[T](ctx, client, method, args...)
	if err != nil {
		var zero T
		return zero, err
	}
	return *result, nil
}

func (c *EngineAPIRPCClient) NewPayloadV1(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return call[engine_types.PayloadStatus](ctx, c.client, rpc_helper.EngineNewPayloadV1, payload)
}

func (c *EngineAPIRPCClient) NewPayloadV2(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return call[engine_types.PayloadStatus](ctx, c.client, rpc_helper.EngineNewPayloadV2, payload)
}

func (c *EngineAPIRPCClient) NewPayloadV3(ctx context.Context, payload *engine_types.ExecutionPayload, expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash) (*engine_types.PayloadStatus, error) {
	return call[engine_types.PayloadStatus](ctx, c.client, rpc_helper.EngineNewPayloadV3, payload, expectedBlobHashes, parentBeaconBlockRoot)
}

func (c *EngineAPIRPCClient) NewPayloadV4(ctx context.Context, payload *engine_types.ExecutionPayload, expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash, executionRequests []hexutil.Bytes) (*engine_types.PayloadStatus, error) {
	return call[engine_types.PayloadStatus](ctx, c.client, rpc_helper.EngineNewPayloadV4, payload, expectedBlobHashes, parentBeaconBlockRoot, executionRequests)
}

func (c *EngineAPIRPCClient) NewPayloadV5(ctx context.Context, payload *engine_types.ExecutionPayload, expectedBlobHashes []common.Hash, parentBeaconBlockRoot *common.Hash, executionRequests []hexutil.Bytes) (*engine_types.PayloadStatus, error) {
	return call[engine_types.PayloadStatus](ctx, c.client, rpc_helper.EngineNewPayloadV5, payload, expectedBlobHashes, parentBeaconBlockRoot, executionRequests)
}

func (c *EngineAPIRPCClient) ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return call[engine_types.ForkChoiceUpdatedResponse](ctx, c.client, rpc_helper.ForkChoiceUpdatedV1, forkChoiceState, payloadAttributes)
}

func (c *EngineAPIRPCClient) ForkchoiceUpdatedV2(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return call[engine_types.ForkChoiceUpdatedResponse](ctx, c.client, rpc_helper.ForkChoiceUpdatedV2, forkChoiceState, payloadAttributes)
}

func (c *EngineAPIRPCClient) ForkchoiceUpdatedV3(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return call[engine_types.ForkChoiceUpdatedResponse](ctx, c.client, rpc_helper.ForkChoiceUpdatedV3, forkChoiceState, payloadAttributes)
}

func (c *EngineAPIRPCClient) ForkchoiceUpdatedV4(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return call[engine_types.ForkChoiceUpdatedResponse](ctx, c.client, rpc_helper.ForkChoiceUpdatedV4, forkChoiceState, payloadAttributes)
}

func (c *EngineAPIRPCClient) GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.ExecutionPayload, error) {
	return call[engine_types.ExecutionPayload](ctx, c.client, rpc_helper.EngineGetPayloadV1, payloadID)
}

func (c *EngineAPIRPCClient) GetPayloadV2(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	return call[engine_types.GetPayloadResponse](ctx, c.client, rpc_helper.EngineGetPayloadV2, payloadID)
}

func (c *EngineAPIRPCClient) GetPayloadV3(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	return call[engine_types.GetPayloadResponse](ctx, c.client, rpc_helper.EngineGetPayloadV3, payloadID)
}

func (c *EngineAPIRPCClient) GetPayloadV4(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	return call[engine_types.GetPayloadResponse](ctx, c.client, rpc_helper.EngineGetPayloadV4, payloadID)
}

func (c *EngineAPIRPCClient) GetPayloadV5(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	return call[engine_types.GetPayloadResponse](ctx, c.client, rpc_helper.EngineGetPayloadV5, payloadID)
}

func (c *EngineAPIRPCClient) GetPayloadV6(ctx context.Context, payloadID hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	return call[engine_types.GetPayloadResponse](ctx, c.client, rpc_helper.EngineGetPayloadV6, payloadID)
}

func (c *EngineAPIRPCClient) GetPayloadBodiesByHashV1(ctx context.Context, hashes []common.Hash) ([]*engine_types.ExecutionPayloadBody, error) {
	return callValue[[]*engine_types.ExecutionPayloadBody](ctx, c.client, rpc_helper.GetPayloadBodiesByHashV1, hashes)
}

func (c *EngineAPIRPCClient) GetPayloadBodiesByHashV2(ctx context.Context, hashes []common.Hash) ([]*engine_types.ExecutionPayloadBodyV2, error) {
	return callValue[[]*engine_types.ExecutionPayloadBodyV2](ctx, c.client, rpc_helper.GetPayloadBodiesByHashV2, hashes)
}

func (c *EngineAPIRPCClient) GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBody, error) {
	return callValue[[]*engine_types.ExecutionPayloadBody](ctx, c.client, rpc_helper.GetPayloadBodiesByRangeV1, start, count)
}

func (c *EngineAPIRPCClient) GetPayloadBodiesByRangeV2(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBodyV2, error) {
	return callValue[[]*engine_types.ExecutionPayloadBodyV2](ctx, c.client, rpc_helper.GetPayloadBodiesByRangeV2, start, count)
}

func (c *EngineAPIRPCClient) GetClientVersionV1(ctx context.Context, callerVersion *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
	return callValue[[]engine_types.ClientVersionV1](ctx, c.client, rpc_helper.EngineGetClientVersionV1, callerVersion)
}

func (c *EngineAPIRPCClient) GetBlobsV1(ctx context.Context, blobHashes []common.Hash) (engine_types.BlobsBundleV1, error) {
	return callValue[engine_types.BlobsBundleV1](ctx, c.client, rpc_helper.EngineGetBlobsV1, blobHashes)
}

func (c *EngineAPIRPCClient) GetBlobsV2(ctx context.Context, blobHashes []common.Hash) (engine_types.BlobsBundleV2, error) {
	return callValue[engine_types.BlobsBundleV2](ctx, c.client, rpc_helper.EngineGetBlobsV2, blobHashes)
}

func (c *EngineAPIRPCClient) GetBlobsV3(ctx context.Context, blobHashes []common.Hash) (engine_types.BlobsBundleV2, error) {
	return callValue[engine_types.BlobsBundleV2](ctx, c.client, rpc_helper.EngineGetBlobsV3, blobHashes)
}
