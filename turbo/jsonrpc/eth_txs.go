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
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/gointerfaces"
	txpool "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon/core/rawdb"
	types2 "github.com/erigontech/erigon/core/types"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/adapter/ethapi"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

// GetTransactionByHash implements eth_getTransactionByHash. Returns information about a transaction given the transaction's hash.
func (api *APIImpl) GetTransactionByHash(ctx context.Context, txnHash common.Hash) (*ethapi.RPCTransaction, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByHash
	blockNum, txNum, ok, err := api.txnLookup(ctx, tx, txnHash)
	if err != nil {
		return nil, err
	}

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.TxBlockIndexFromBlockReader(ctx, api._blockReader))

	// Private API returns 0 if transaction is not found.
	if blockNum == 0 && chainConfig.Bor != nil {
		if api.useBridgeReader {
			blockNum, ok, err = api.bridgeReader.EventTxnLookup(ctx, txnHash)
			if ok {
				txNumNextBlock, err := txNumsReader.Min(tx, blockNum+1)
				if err != nil {
					return nil, err
				}
				txNum = txNumNextBlock
			}
		} else {
			blockNum, ok, err = api._blockReader.EventLookup(ctx, tx, txnHash)
		}

		if err != nil {
			return nil, err
		}
	}
	if ok {
		txNumMin, err := txNumsReader.Min(tx, blockNum)
		if err != nil {
			return nil, err
		}

		if txNumMin+2 > txNum { //TODO: what a magic is this "2" and how to avoid it
			return nil, fmt.Errorf("uint underflow txnums error txNum: %d, txNumMin: %d, blockNum: %d", txNum, txNumMin, blockNum)
		}

		var txnIndex uint64 = txNum - txNumMin - 2

		txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, int(txnIndex))
		if err != nil {
			return nil, err
		}

		header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}
		if header == nil {
			return nil, nil
		}

		blockHash := header.Hash()

		// Add GasPrice for the DynamicFeeTransaction
		var baseFee *big.Int
		if chainConfig.IsLondon(blockNum) && blockHash != (common.Hash{}) {
			baseFee = header.BaseFee
		}

		// if no transaction was found then we return nil
		if txn == nil {
			if chainConfig.Bor == nil {
				return nil, nil
			}
			borTx := bortypes.NewBorTransaction()
			_, txCount, err := api._blockReader.Body(ctx, tx, blockHash, blockNum)
			if err != nil {
				return nil, err
			}
			return ethapi.NewRPCBorTransaction(borTx, txnHash, blockHash, blockNum, uint64(txCount), chainConfig.ChainID), nil
		}

		return ethapi.NewRPCTransaction(txn, blockHash, blockNum, txnIndex, baseFee), nil
	}

	curHeader := rawdb.ReadCurrentHeader(tx)
	if curHeader == nil {
		return nil, nil
	}

	// No finalized transaction, try to retrieve it from the pool
	reply, err := api.txPool.Transactions(ctx, &txpool.TransactionsRequest{Hashes: []*types.H256{gointerfaces.ConvertHashToH256(txnHash)}})
	if err != nil {
		return nil, err
	}
	if len(reply.RlpTxs[0]) > 0 {
		txn, err := types2.DecodeWrappedTransaction(reply.RlpTxs[0])
		if err != nil {
			return nil, err
		}

		// if no transaction was found in the txpool then we return nil and an error warning that we didn't find the transaction by the hash
		if txn == nil {
			return nil, nil
		}

		return newRPCPendingTransaction(txn, curHeader, chainConfig), nil
	}

	// Transaction unknown, return as such
	return nil, nil
}

// GetRawTransactionByHash returns the bytes of the transaction for the given hash.
func (api *APIImpl) GetRawTransactionByHash(ctx context.Context, hash common.Hash) (hexutility.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByHash
	blockNum, _, ok, err := api.txnLookup(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	var txn types2.Transaction
	for _, transaction := range block.Transactions() {
		if transaction.Hash() == hash {
			txn = transaction
			break
		}
	}

	if txn != nil {
		var buf bytes.Buffer
		err = txn.MarshalBinary(&buf)
		return buf.Bytes(), err
	}

	// No finalized transaction, try to retrieve it from the pool
	reply, err := api.txPool.Transactions(ctx, &txpool.TransactionsRequest{Hashes: []*types.H256{gointerfaces.ConvertHashToH256(hash)}})
	if err != nil {
		return nil, err
	}
	if len(reply.RlpTxs[0]) > 0 {
		return reply.RlpTxs[0], nil
	}
	return nil, nil
}

// GetTransactionByBlockHashAndIndex implements eth_getTransactionByBlockHashAndIndex. Returns information about a transaction given the block's hash and a transaction index.
func (api *APIImpl) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (*ethapi.RPCTransaction, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockHashAndIndex
	block, err := api.blockByHashWithSenders(ctx, tx, blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}

	txs := block.Transactions()
	if uint64(txIndex) > uint64(len(txs)) {
		return nil, nil // not error
	} else if uint64(txIndex) == uint64(len(txs)) {
		if chainConfig.Bor == nil {
			return nil, nil // not error
		}
		var borTx types2.Transaction
		if api.useBridgeReader {
			possibleBorTxnHash := bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash())
			_, ok, err := api.bridgeReader.EventTxnLookup(ctx, possibleBorTxnHash)
			if err != nil {
				return nil, err
			}
			if ok {
				borTx = bortypes.NewBorTransaction()
			}
		} else {
			borTx = rawdb.ReadBorTransactionForBlock(tx, block.NumberU64())
		}
		if borTx == nil {
			return nil, nil // not error
		}
		derivedBorTxHash := bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash())
		return ethapi.NewRPCBorTransaction(borTx, derivedBorTxHash, block.Hash(), block.NumberU64(), uint64(txIndex), chainConfig.ChainID), nil
	}

	return ethapi.NewRPCTransaction(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex), block.BaseFee()), nil
}

// GetRawTransactionByBlockHashAndIndex returns the bytes of the transaction for the given block hash and index.
func (api *APIImpl) GetRawTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index hexutil.Uint) (hexutility.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getRawTransactionByBlockHashAndIndex
	block, err := api.blockByHashWithSenders(ctx, tx, blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}

	return newRPCRawTransactionFromBlockIndex(block, uint64(index))
}

// GetTransactionByBlockNumberAndIndex implements eth_getTransactionByBlockNumberAndIndex. Returns information about a transaction given a block number and transaction index.
func (api *APIImpl) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*ethapi.RPCTransaction, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockNumberAndIndex
	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNr), tx, api._blockReader, api.filters)
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

	txs := block.Transactions()
	if uint64(txIndex) > uint64(len(txs)) {
		return nil, nil // not error
	} else if uint64(txIndex) == uint64(len(txs)) {
		if chainConfig.Bor == nil {
			return nil, nil // not error
		}
		var borTx types2.Transaction
		if api.useBridgeReader {
			possibleBorTxnHash := bortypes.ComputeBorTxHash(blockNum, hash)
			_, ok, err := api.bridgeReader.EventTxnLookup(ctx, possibleBorTxnHash)
			if err != nil {
				return nil, err
			}
			if ok {
				borTx = bortypes.NewBorTransaction()
			}
		} else {
			borTx = rawdb.ReadBorTransactionForBlock(tx, blockNum)
		}
		if borTx == nil {
			return nil, nil
		}
		derivedBorTxHash := bortypes.ComputeBorTxHash(blockNum, hash)
		return ethapi.NewRPCBorTransaction(borTx, derivedBorTxHash, hash, blockNum, uint64(txIndex), chainConfig.ChainID), nil
	}

	return ethapi.NewRPCTransaction(txs[txIndex], hash, blockNum, uint64(txIndex), block.BaseFee()), nil
}

// GetRawTransactionByBlockNumberAndIndex returns the bytes of the transaction for the given block number and index.
func (api *APIImpl) GetRawTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (hexutility.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getRawTransactionByBlockNumberAndIndex
	block, err := api.blockByRPCNumber(ctx, blockNr, tx)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}

	return newRPCRawTransactionFromBlockIndex(block, uint64(index))
}
