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

	"github.com/cenkalti/backoff/v4"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/jwt"
	"github.com/erigontech/erigon-lib/log/v3"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/rpc"
)

type JsonRpcClientOption func(*JsonRpcClient)

func WithJsonRpcClientMaxRetries(maxRetries uint64) JsonRpcClientOption {
	return func(client *JsonRpcClient) {
		client.maxRetries = maxRetries
	}
}

func WithJsonRpcClientRetryBackOff(retryBackOff time.Duration) JsonRpcClientOption {
	return func(client *JsonRpcClient) {
		client.retryBackOff = retryBackOff
	}
}

type JsonRpcClient struct {
	rpcClient    *rpc.Client
	maxRetries   uint64
	retryBackOff time.Duration
}

func DialJsonRpcClient(url string, jwtSecret []byte, logger log.Logger, opts ...JsonRpcClientOption) (*JsonRpcClient, error) {
	jwtRoundTripper := jwt.NewHttpRoundTripper(http.DefaultTransport, jwtSecret)
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: jwtRoundTripper}
	client, err := rpc.DialHTTPWithClient(url, httpClient, logger)
	if err != nil {
		return nil, err
	}

	res := &JsonRpcClient{
		rpcClient:    client,
		maxRetries:   10,
		retryBackOff: 100 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(res)
	}

	return res, nil
}

func (c *JsonRpcClient) NewPayloadV1(ctx context.Context, payload *enginetypes.ExecutionPayload) (*enginetypes.PayloadStatus, error) {
	return backoff.RetryWithData(func() (*enginetypes.PayloadStatus, error) {
		var result enginetypes.PayloadStatus
		err := c.rpcClient.CallContext(ctx, &result, "engine_newPayloadV1", payload)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) NewPayloadV2(ctx context.Context, payload *enginetypes.ExecutionPayload) (*enginetypes.PayloadStatus, error) {
	return backoff.RetryWithData(func() (*enginetypes.PayloadStatus, error) {
		var result enginetypes.PayloadStatus
		err := c.rpcClient.CallContext(ctx, &result, "engine_newPayloadV2", payload)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) NewPayloadV3(
	ctx context.Context,
	executionPayload *enginetypes.ExecutionPayload,
	expectedBlobHashes []common.Hash,
	parentBeaconBlockRoot *common.Hash,
) (*enginetypes.PayloadStatus, error) {
	return backoff.RetryWithData(func() (*enginetypes.PayloadStatus, error) {
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
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) NewPayloadV4(
	ctx context.Context,
	executionPayload *enginetypes.ExecutionPayload,
	expectedBlobHashes []common.Hash,
	parentBeaconBlockRoot *common.Hash,
	executionRequests []hexutil.Bytes,
) (*enginetypes.PayloadStatus, error) {
	return backoff.RetryWithData(func() (*enginetypes.PayloadStatus, error) {
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
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) ForkchoiceUpdatedV1(
	ctx context.Context,
	forkChoiceState *enginetypes.ForkChoiceState,
	payloadAttributes *enginetypes.PayloadAttributes,
) (*enginetypes.ForkChoiceUpdatedResponse, error) {
	return backoff.RetryWithData(func() (*enginetypes.ForkChoiceUpdatedResponse, error) {
		var result enginetypes.ForkChoiceUpdatedResponse
		err := c.rpcClient.CallContext(ctx, &result, "engine_forkchoiceUpdatedV1", forkChoiceState, payloadAttributes)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) ForkchoiceUpdatedV2(
	ctx context.Context,
	forkChoiceState *enginetypes.ForkChoiceState,
	payloadAttributes *enginetypes.PayloadAttributes,
) (*enginetypes.ForkChoiceUpdatedResponse, error) {
	return backoff.RetryWithData(func() (*enginetypes.ForkChoiceUpdatedResponse, error) {
		var result enginetypes.ForkChoiceUpdatedResponse
		err := c.rpcClient.CallContext(ctx, &result, "engine_forkchoiceUpdatedV2", forkChoiceState, payloadAttributes)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) ForkchoiceUpdatedV3(
	ctx context.Context,
	forkChoiceState *enginetypes.ForkChoiceState,
	payloadAttributes *enginetypes.PayloadAttributes,
) (*enginetypes.ForkChoiceUpdatedResponse, error) {
	return backoff.RetryWithData(func() (*enginetypes.ForkChoiceUpdatedResponse, error) {
		var result enginetypes.ForkChoiceUpdatedResponse
		err := c.rpcClient.CallContext(ctx, &result, "engine_forkchoiceUpdatedV3", forkChoiceState, payloadAttributes)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) GetPayloadV1(ctx context.Context, payloadID hexutil.Bytes) (*enginetypes.ExecutionPayload, error) {
	return backoff.RetryWithData(func() (*enginetypes.ExecutionPayload, error) {
		var result enginetypes.ExecutionPayload
		err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadV1", payloadID)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) GetPayloadV2(ctx context.Context, payloadID hexutil.Bytes) (*enginetypes.GetPayloadResponse, error) {
	return backoff.RetryWithData(func() (*enginetypes.GetPayloadResponse, error) {
		var result enginetypes.GetPayloadResponse
		err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadV2", payloadID)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) GetPayloadV3(ctx context.Context, payloadID hexutil.Bytes) (*enginetypes.GetPayloadResponse, error) {
	return backoff.RetryWithData(func() (*enginetypes.GetPayloadResponse, error) {
		var result enginetypes.GetPayloadResponse
		err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadV3", payloadID)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) GetPayloadV4(ctx context.Context, payloadID hexutil.Bytes) (*enginetypes.GetPayloadResponse, error) {
	return backoff.RetryWithData(func() (*enginetypes.GetPayloadResponse, error) {
		var result enginetypes.GetPayloadResponse
		err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadV4", payloadID)
		if err != nil {
			return nil, err
		}
		return &result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) GetPayloadBodiesByHashV1(ctx context.Context, hashes []common.Hash) ([]*enginetypes.ExecutionPayloadBody, error) {
	return backoff.RetryWithData(func() ([]*enginetypes.ExecutionPayloadBody, error) {
		var result []*enginetypes.ExecutionPayloadBody
		err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadBodiesByHashV1", hashes)
		if err != nil {
			return nil, err
		}
		return result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*enginetypes.ExecutionPayloadBody, error) {
	return backoff.RetryWithData(func() ([]*enginetypes.ExecutionPayloadBody, error) {
		var result []*enginetypes.ExecutionPayloadBody
		err := c.rpcClient.CallContext(ctx, &result, "engine_getPayloadBodiesByRangeV1", start, count)
		if err != nil {
			return nil, err
		}
		return result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) GetClientVersionV1(ctx context.Context, callerVersion *enginetypes.ClientVersionV1) ([]enginetypes.ClientVersionV1, error) {
	return backoff.RetryWithData(func() ([]enginetypes.ClientVersionV1, error) {
		var result []enginetypes.ClientVersionV1
		err := c.rpcClient.CallContext(ctx, &result, "engine_getClientVersionV1", callerVersion)
		if err != nil {
			return nil, err
		}
		return result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) backOff(ctx context.Context) backoff.BackOff {
	var backOff backoff.BackOff
	backOff = backoff.NewConstantBackOff(c.retryBackOff)
	backOff = backoff.WithMaxRetries(backOff, c.maxRetries)
	return backoff.WithContext(backOff, ctx)
}
