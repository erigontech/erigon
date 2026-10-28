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
	"errors"
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// GetTransactionByHash implements eth_getTransactionByHash. Returns information about a transaction given the transaction's hash.
func (api *APIImpl) GetTransactionByHash(ctx context.Context, txnHash common.Hash) (*ethapi.RPCTransaction, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	// https://www.quicknode.com/docs/ethereum/eth_getTransactionByHash
	blockNum, txNum, ok, err := api.txnLookup(ctx, tx, txnHash)
	if err != nil {
		return nil, err
	}
	if ok {
		err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}

		txnIndex, err := api.txnIndexInBlock(ctx, tx, blockNum, txNum)
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
		blockTime := header.Time

		// Add GasPrice for the DynamicFeeTransaction
		var baseFee *uint256.Int
		if chainConfig.IsLondon(blockNum) && blockHash != (common.Hash{}) {
			baseFee = header.BaseFee
		}

		txn, ok, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txnIndex)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}

		return ethapi.NewRPCTransaction(txn, blockHash, blockTime, blockNum, uint64(txnIndex), baseFee), nil
	}

	// No finalized transaction, try to retrieve it from the pool
	reply, err := api.txPool.Transactions(ctx, &txpoolproto.TransactionsRequest{Hashes: []*typesproto.H256{gointerfaces.ConvertHashToH256(txnHash)}})
	if err != nil {
		return nil, err
	}
	if len(reply.RlpTxs[0]) > 0 {
		txn, err := types.DecodeWrappedTransaction(reply.RlpTxs[0])
		if err != nil {
			return nil, err
		}

		// if no transaction was found in the txpool then we return nil and an error warning that we didn't find the transaction by the hash
		if txn == nil {
			return nil, nil
		}

		return newRPCPendingTransaction(txn), nil
	}

	// Transaction unknown, return as such
	return nil, nil
}

// GetRawTransactionByHash returns the bytes of the transaction for the given hash.
func (api *APIImpl) GetRawTransactionByHash(ctx context.Context, hash common.Hash) (hexutil.Bytes, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://www.quicknode.com/docs/ethereum/eth_getTransactionByHash
	blockNum, txNum, ok, err := api.txnLookup(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if ok {
		err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}

		txnIndex, err := api.txnIndexInBlock(ctx, tx, blockNum, txNum)
		if err != nil {
			return nil, err
		}
		txn, ok, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txnIndex)
		if err != nil {
			return nil, err
		}
		if ok {
			var buf bytes.Buffer
			err = txn.MarshalBinary(&buf)
			return buf.Bytes(), err
		}
	}

	// No finalized transaction, try to retrieve it from the pool
	reply, err := api.txPool.Transactions(ctx, &txpoolproto.TransactionsRequest{Hashes: []*typesproto.H256{gointerfaces.ConvertHashToH256(hash)}})
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
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithHash(blockHash, true), tx, api._blockReader)
	if err != nil {
		return nil, nil
	}

	err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	// https://www.quicknode.com/docs/ethereum/eth_getTransactionByBlockHashAndIndex
	return api.rpcTxnByIdxInBlock(ctx, tx, blockHash, blockNum, uint64(txIndex))
}

// GetRawTransactionByBlockHashAndIndex returns the bytes of the transaction for the given block hash and index.
func (api *APIImpl) GetRawTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index hexutil.Uint) (hexutil.Bytes, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithHash(blockHash, true), tx, api._blockReader)
	if err != nil {
		return nil, nil
	}

	err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	return api.rawTxnByIdxInBlock(ctx, tx, blockHash, blockNum, uint64(index))
}

// GetTransactionByBlockNumberAndIndex implements eth_getTransactionByBlockNumberAndIndex. Returns information about a transaction given a block number and transaction index.
func (api *APIImpl) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*ethapi.RPCTransaction, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if blockNr == rpc.PendingBlockNumber {
		b, err := api.blockByNumber(ctx, blockNr, tx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, errors.New("pending block is not available")
		}
		txs := b.Transactions()
		if uint64(txIndex) >= uint64(len(txs)) {
			return nil, nil
		}
		return ethapi.NewRPCTransaction(txs[txIndex], common.Hash{}, b.Time(), 0, uint64(txIndex), b.BaseFee()), nil
	}

	// https://www.quicknode.com/docs/ethereum/eth_getTransactionByBlockNumberAndIndex
	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNr), tx, api._blockReader)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
		}
		return nil, err
	}

	err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	return api.rpcTxnByIdxInBlock(ctx, tx, hash, blockNum, uint64(txIndex))
}

// GetRawTransactionByBlockNumberAndIndex returns the bytes of the transaction for the given block number and index.
func (api *APIImpl) GetRawTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (hexutil.Bytes, error) {
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if blockNr == rpc.PendingBlockNumber {
		b, err := api.blockByNumber(ctx, blockNr, tx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, errors.New("pending block is not available")
		}
		return newRPCRawTransactionFromBlockIndex(b, uint64(index))
	}

	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNr), tx, api._blockReader)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
		}
		return nil, err
	}

	err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	return api.rawTxnByIdxInBlock(ctx, tx, hash, blockNum, uint64(index))
}

// txnByIdxInBlock returns the i-th transaction of a canonical block with its header, from the
// block cache when the whole block is there, else reading only that transaction. ok is false when
// the block or that transaction is missing - not an error, see https://github.com/erigontech/erigon/issues/1645
func (api *APIImpl) txnByIdxInBlock(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNum, txIdxInBlock uint64) (types.Transaction, *types.Header, bool, error) {
	if txIdxInBlock > math.MaxInt { // the readers take an int index
		return nil, nil, false, nil
	}
	if api.blocksLRU != nil {
		if b, ok := api.blocksLRU.Get(blockHash); ok && b != nil {
			txs := b.Transactions()
			if txIdxInBlock >= uint64(len(txs)) {
				return nil, nil, false, nil
			}
			return txs[txIdxInBlock], b.HeaderNoCopy(), true, nil
		}
	}
	header, err := api.headerByHashAndNumber(ctx, tx, blockHash, blockNum)
	if err != nil || header == nil {
		return nil, nil, false, err
	}
	txn, ok, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, int(txIdxInBlock))
	if err != nil || !ok {
		return nil, nil, false, err
	}
	return txn, header, true, nil
}

func (api *APIImpl) rpcTxnByIdxInBlock(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNum, txIdxInBlock uint64) (*ethapi.RPCTransaction, error) {
	txn, header, ok, err := api.txnByIdxInBlock(ctx, tx, blockHash, blockNum, txIdxInBlock)
	if err != nil || !ok {
		return nil, err
	}
	return ethapi.NewRPCTransaction(txn, blockHash, header.Time, blockNum, txIdxInBlock, header.BaseFee), nil
}

// rawTxnByIdxInBlock returns the binary encoding of the i-th transaction of a canonical block.
func (api *APIImpl) rawTxnByIdxInBlock(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNum, txIdxInBlock uint64) (hexutil.Bytes, error) {
	txn, _, ok, err := api.txnByIdxInBlock(ctx, tx, blockHash, blockNum, txIdxInBlock)
	if err != nil || !ok {
		return nil, err
	}
	var buf bytes.Buffer
	if err := txn.MarshalBinary(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
