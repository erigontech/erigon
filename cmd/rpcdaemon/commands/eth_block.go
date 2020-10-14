package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter/ethapi"
)

// GetBlockByNumber see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbynumber
// see internal/ethapi.PublicBlockChainAPI.GetBlockByNumber
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(number, tx)
	if err != nil {
		return nil, err
	}
	additionalFields := make(map[string]interface{})

	block := rawdb.ReadBlockByNumber(tx, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}

	additionalFields["totalDifficulty"] = (*hexutil.Big)(rawdb.ReadTd(tx, block.Hash(), blockNum))
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

// GetBlockByHash see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbyhash
// see internal/ethapi.PublicBlockChainAPI.GetBlockByHash
func (api *APIImpl) GetBlockByHash(ctx context.Context, hash common.Hash, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	additionalFields := make(map[string]interface{})

	block := rawdb.ReadBlockByHash(tx, hash)
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", hash)
	}
	number := block.NumberU64()

	additionalFields["totalDifficulty"] = (*hexutil.Big)(rawdb.ReadTd(tx, hash, number))
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && int64(number) == rpc.PendingBlockNumber.Int64() {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

// GetHeaderByNumber returns a block's header by number
func (api *APIImpl) GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header := rawdb.ReadHeaderByNumber(tx, uint64(number.Int64()))
	if header == nil {
		return nil, fmt.Errorf("block header not found: %d", number.Int64())
	}

	return header, nil
}

// GetHeaderByHash returns a block's header by hash
func (api *APIImpl) GetHeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header := rawdb.ReadHeaderByHash(tx, hash)
	if header == nil {
		return nil, fmt.Errorf("block header not found: %s", hash.String())
	}

	return header, nil
}

// GetBlockTransactionCountByNumber returns the number of transactions in the block
func (api *APIImpl) GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}

	block := rawdb.ReadBlockByNumber(tx, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}

// GetBlockTransactionCountByHash returns the number of transactions in the block
func (api *APIImpl) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block := rawdb.ReadBlockByHash(tx, blockHash)
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", blockHash)
	}
	n := hexutil.Uint(len(block.Transactions()))
	return &n, nil
}
