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

package engineapi

import (
	"context"

	"github.com/cenkalti/backoff/v4"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
)

func (c *JsonRpcClient) GetBlobsV3(ctx context.Context, blobHashes []common.Hash) ([]*enginetypes.BlobAndProofV2, error) {
	return backoff.RetryWithData(func() ([]*enginetypes.BlobAndProofV2, error) {
		var result []*enginetypes.BlobAndProofV2
		err := c.rpcClient.CallContext(ctx, &result, "engine_getBlobsV3", blobHashes)
		if err != nil {
			return nil, c.maybeMakePermanent(err)
		}
		return result, nil
	}, c.backOff(ctx))
}

func (c *JsonRpcClient) GetBlobsV4(ctx context.Context, blobHashes []common.Hash, cellMask hexutil.Bytes) ([]*enginetypes.BlobCellsAndProofsV1, error) {
	return backoff.RetryWithData(func() ([]*enginetypes.BlobCellsAndProofsV1, error) {
		var result []*enginetypes.BlobCellsAndProofsV1
		err := c.rpcClient.CallContext(ctx, &result, "engine_getBlobsV4", blobHashes, cellMask)
		if err != nil {
			return nil, c.maybeMakePermanent(err)
		}
		return result, nil
	}, c.backOff(ctx))
}
