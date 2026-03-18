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

package jsonrpc

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// GetBlockAccessList returns the block access list for a given block (EIP-7928).
func (api *APIImpl) GetBlockAccessList(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) ([]*ethapi.RPCAccountAccess, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if numberOrHash.BlockNumber != nil && *numberOrHash.BlockNumber == rpc.PendingBlockNumber {
		return nil, errors.New("pending block access list is not available")
	}

	blockNum, blockHash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, numberOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil
		}
		return nil, err
	}

	header, err := api._blockReader.Header(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !chainConfig.IsAmsterdam(header.Time) {
		return nil, &rpc.CustomError{
			Code:    4445,
			Message: "block access list not available for pre-Amsterdam blocks",
		}
	}

	data, err := rawdb.ReadBlockAccessListBytes(tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, &rpc.CustomError{
			Code:    4444,
			Message: "pruned history unavailable",
		}
	}

	bal, err := types.DecodeBlockAccessListBytes(data)
	if err != nil {
		return nil, err
	}

	return ethapi.MarshalBlockAccessList(bal), nil
}
