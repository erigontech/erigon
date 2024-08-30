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

package jsonrpc

import (
	"context"

	"github.com/erigontech/erigon-lib/common/hexutil"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/adapter/ethapi"
	"github.com/erigontech/erigon/turbo/rpchelper"
)

// GetUncleByBlockNumberAndIndex implements eth_getUncleByBlockNumberAndIndex. Returns information about an uncle given a block's number and the index of the uncle.
func (api *APIImpl) GetUncleByBlockNumberAndIndex(ctx context.Context, number rpc.BlockNumber, index hexutil.Uint) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(number), tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}
	additionalFields := make(map[string]interface{})

	uncles := block.Uncles()
	if index >= hexutil.Uint(len(uncles)) {
		log.Trace("Requested uncle not found", "number", block.Number(), "hash", hash, "index", index)
		return nil, nil
	}
	uncle := types.NewBlockWithHeader(uncles[index])
	return ethapi.RPCMarshalBlock(uncle, false, false, additionalFields)
}

// GetUncleByBlockHashAndIndex implements eth_getUncleByBlockHashAndIndex. Returns information about an uncle given a block's hash and the index of the uncle.
func (api *APIImpl) GetUncleByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, err := api.blockByHashWithSenders(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}
	additionalFields := make(map[string]interface{})

	uncles := block.Uncles()
	if index >= hexutil.Uint(len(uncles)) {
		log.Trace("Requested uncle not found", "number", block.Number(), "hash", hash, "index", index)
		return nil, nil
	}
	uncle := types.NewBlockWithHeader(uncles[index])

	return ethapi.RPCMarshalBlock(uncle, false, false, additionalFields)
}

// GetUncleCountByBlockNumber implements eth_getUncleCountByBlockNumber. Returns the number of uncles in the block, if any.
func (api *APIImpl) GetUncleCountByBlockNumber(ctx context.Context, number rpc.BlockNumber) (*hexutil.Uint, error) {
	n := hexutil.Uint(0)

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return &n, err
	}
	defer tx.Rollback()

	blockNum, blockHash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(number), tx, api._blockReader, api.filters)
	if err != nil {
		return &n, err
	}

	block, err := api.blockWithSenders(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}
	n = hexutil.Uint(len(block.Uncles()))
	return &n, nil
}

// GetUncleCountByBlockHash implements eth_getUncleCountByBlockHash. Returns the number of uncles in the block, if any.
func (api *APIImpl) GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (*hexutil.Uint, error) {
	n := hexutil.Uint(0)
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return &n, err
	}
	defer tx.Rollback()

	number, err := api._blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if number == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}

	block, err := api.blockWithSenders(ctx, tx, hash, *number)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}
	n = hexutil.Uint(len(block.Uncles()))
	return &n, nil
}
