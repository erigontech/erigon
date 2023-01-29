package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

// GetUncleByBlockNumberAndIndex implements eth_getUncleByBlockNumberAndIndex. Returns information about an uncle given a block's number and the index of the uncle.
func (api *APIImpl) GetUncleByBlockNumberAndIndex(ctx context.Context, number rpc.BlockNumber, index hexutil.Uint) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, hash, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(number), tx, api.filters)
	if err != nil {
		return nil, err
	}
	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}
	additionalFields := make(map[string]interface{})
	td, err := rawdb.ReadTd(tx, block.Hash(), blockNum)
	if err != nil {
		return nil, err
	}
	additionalFields["totalDifficulty"] = (*hexutil.Big)(td)

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

	block, err := api.blockByHashWithSenders(tx, hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}
	number := block.NumberU64()
	additionalFields := make(map[string]interface{})
	td, err := rawdb.ReadTd(tx, hash, number)
	if err != nil {
		return nil, err
	}
	additionalFields["totalDifficulty"] = (*hexutil.Big)(td)

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

	blockNum, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(number), tx, api.filters)
	if err != nil {
		return &n, err
	}

	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
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

	number := rawdb.ReadHeaderNumber(tx, hash)
	if number == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	block, err := api.blockWithSenders(tx, hash, *number)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}
	n = hexutil.Uint(len(block.Uncles()))
	return &n, nil
}
