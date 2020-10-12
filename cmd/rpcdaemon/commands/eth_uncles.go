package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter/ethapi"
)

// GetUncleByBlockNumberAndIndex returns the uncle block for the given block hash and index. When fullTx is true
// all transactions in the block are returned in full detail, otherwise only the transaction hash is returned.
func (api *APIImpl) GetUncleByBlockNumberAndIndex(ctx context.Context, number rpc.BlockNumber, index hexutil.Uint) (map[string]interface{}, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(number, tx)
	if err != nil {
		return nil, err
	}
	block := rawdb.ReadBlockByNumber(tx, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}
	hash := block.Hash()
	additionalFields := make(map[string]interface{})
	additionalFields["totalDifficulty"] = (*hexutil.Big)(rawdb.ReadTd(tx, block.Hash(), blockNum))

	uncles := block.Uncles()
	if index >= hexutil.Uint(len(uncles)) {
		log.Debug("Requested uncle not found", "number", block.Number(), "hash", hash, "index", index)
		return nil, nil
	}
	uncle := types.NewBlockWithHeader(uncles[index])
	return ethapi.RPCMarshalBlock(uncle, false, false, additionalFields)
}

// GetUncleByBlockHashAndIndex returns the uncle block for the given block hash and index. When fullTx is true
// all transactions in the block are returned in full detail, otherwise only the transaction hash is returned.
func (api *APIImpl) GetUncleByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint) (map[string]interface{}, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block := rawdb.ReadBlockByHash(tx, hash)
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", hash)
	}
	number := block.NumberU64()
	additionalFields := make(map[string]interface{})
	additionalFields["totalDifficulty"] = (*hexutil.Big)(rawdb.ReadTd(tx, hash, number))

	uncles := block.Uncles()
	if index >= hexutil.Uint(len(uncles)) {
		log.Debug("Requested uncle not found", "number", block.Number(), "hash", hash, "index", index)
		return nil, nil
	}
	uncle := types.NewBlockWithHeader(uncles[index])

	return ethapi.RPCMarshalBlock(uncle, false, false, additionalFields)
}

// GetUncleCountByBlockNumber returns number of uncles in the block for the given block number
func (api *APIImpl) GetUncleCountByBlockNumber(ctx context.Context, number rpc.BlockNumber) (*hexutil.Uint, error) {
	n := hexutil.Uint(0)

	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return &n, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(number, tx)
	if err != nil {
		return &n, err
	}
	block := rawdb.ReadBlockByNumber(tx, blockNum)
	if block != nil {
		n = hexutil.Uint(len(block.Uncles()))
	}
	return &n, nil
}

// GetUncleCountByBlockHash returns number of uncles in the block for the given block hash
func (api *APIImpl) GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (*hexutil.Uint, error) {
	n := hexutil.Uint(0)
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return &n, err
	}
	defer tx.Rollback()

	block := rawdb.ReadBlockByHash(tx, hash)
	if block != nil {
		n = hexutil.Uint(len(block.Uncles()))
	}
	return &n, nil
}
