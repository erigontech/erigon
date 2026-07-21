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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var errBlockAccessListNotFound = errors.New("block access list not found")

func blockAccessListResourceNotFoundError() *rpc.CustomError {
	return &rpc.CustomError{Code: -32001, Message: "Resource not found"}
}

func blockAccessListPrunedHistoryError() *rpc.CustomError {
	return &rpc.CustomError{Code: 4444, Message: "Pruned history unavailable"}
}

func (api *BaseAPI) blockAccessListBytes(ctx context.Context, tx kv.TemporalTx, numberOrHash rpc.BlockNumberOrHash) ([]byte, error) {
	if numberOrHash.BlockNumber != nil && *numberOrHash.BlockNumber == rpc.PendingBlockNumber {
		return nil, errBlockAccessListNotFound
	}
	blockNum, blockHash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, numberOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, errBlockAccessListNotFound
		}
		return nil, err
	}
	header, err := api._blockReader.Header(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, errBlockAccessListNotFound
	}
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !chainConfig.IsAmsterdam(header.Time) {
		return nil, blockAccessListResourceNotFoundError()
	}
	data, err := rawdb.ReadBlockAccessListBytes(tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		data, err = api.balRegenerator.GetBlockAccessListBytes(ctx, chainConfig, tx, blockHash, blockNum)
		if errors.Is(err, state.PrunedError) {
			return nil, blockAccessListPrunedHistoryError()
		}
		if err != nil {
			return nil, err
		}
	}
	if len(data) == 0 {
		return nil, blockAccessListPrunedHistoryError()
	}
	return data, nil
}
