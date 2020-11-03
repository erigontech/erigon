package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter/ethapi"
)

// GetBlockByNumber implements eth_getBlockByNumber. Returns information about a block given the block's number.
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(number, tx)
	if err != nil {
		return nil, err
	}
	additionalFields := make(map[string]interface{})

	block, err := rawdb.ReadBlockByNumber(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}

	td, err := rawdb.ReadTd(tx, block.Hash(), blockNum)
	if err != nil {
		return nil, err
	}
	additionalFields["totalDifficulty"] = (*hexutil.Big)(td)
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

// GetBlockByHash implements eth_getBlockByHash. Returns information about a block given the block's hash.
func (api *APIImpl) GetBlockByHash(ctx context.Context, numberOrHash rpc.BlockNumberOrHash, fullTx bool) (map[string]interface{}, error) {
	if numberOrHash.BlockHash == nil {
		if numberOrHash.BlockNumber != nil {
			return api.GetBlockByNumber(ctx, *numberOrHash.BlockNumber, fullTx)
		}
		return nil, fmt.Errorf("block not found")
	}

	hash := *numberOrHash.BlockHash
	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	additionalFields := make(map[string]interface{})

	block, err := rawdb.ReadBlockByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", hash)
	}
	number := block.NumberU64()

	td, err := rawdb.ReadTd(tx, hash, number)
	if err != nil {
		return nil, err
	}
	additionalFields["totalDifficulty"] = (*hexutil.Big)(td)
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && int64(number) == rpc.PendingBlockNumber.Int64() {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

// GetBlockTransactionCountByNumber implements eth_getBlockTransactionCountByNumber. Returns the number of transactions in a block given the block's block number.
func (api *APIImpl) GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error) {
	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}

	block, err := rawdb.ReadBlockByNumber(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}

// GetBlockTransactionCountByHash implements eth_getBlockTransactionCountByHash. Returns the number of transactions in a block given the block's block hash.
func (api *APIImpl) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error) {
	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, err := rawdb.ReadBlockByHash(tx, blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", blockHash)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}
