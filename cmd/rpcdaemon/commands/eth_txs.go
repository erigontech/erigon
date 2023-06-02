package commands

import (
	"bytes"
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	types2 "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

// GetTransactionByHash implements eth_getTransactionByHash. Returns information about a transaction given the transaction's hash.
func (api *APIImpl) GetTransactionByHash(ctx context.Context, txnHash common.Hash) (*RPCTransaction, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByHash
	blockNum, ok, err := api.txnLookup(ctx, tx, txnHash)
	if err != nil {
		return nil, err
	}
	// Private API returns 0 if transaction is not found.
	if blockNum == 0 && chainConfig.Bor != nil {
		blockNumPtr, err := rawdb.ReadBorTxLookupEntry(tx, txnHash)
		if err != nil {
			return nil, err
		}

		ok = blockNumPtr != nil
		if ok {
			blockNum = *blockNumPtr
		}
	}
	if ok {
		block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, nil
		}
		blockHash := block.Hash()
		var txnIndex uint64
		var txn types2.Transaction
		for i, transaction := range block.Transactions() {
			if transaction.Hash() == txnHash {
				txn = transaction
				txnIndex = uint64(i)
				break
			}
		}

		// Add GasPrice for the DynamicFeeTransaction
		var baseFee *big.Int
		if chainConfig.IsLondon(blockNum) && blockHash != (common.Hash{}) {
			baseFee = block.BaseFee()
		}

		// if no transaction was found then we return nil
		if txn == nil {
			if chainConfig.Bor == nil {
				return nil, nil
			}
			borTx, _, _, _ := rawdb.ReadBorTransactionForBlock(tx, block)
			if borTx == nil {
				return nil, nil
			}
			return newRPCBorTransaction(borTx, txnHash, blockHash, blockNum, uint64(len(block.Transactions())), baseFee, chainConfig.ChainID), nil
		}

		return newRPCTransaction(txn, blockHash, blockNum, txnIndex, baseFee), nil
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
		txn, err := types2.DecodeTransaction(reply.RlpTxs[0])
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
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByHash
	blockNum, ok, err := api.txnLookup(ctx, tx, hash)
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
func (api *APIImpl) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (*RPCTransaction, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockHashAndIndex
	block, err := api.blockByHashWithSenders(ctx, tx, blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	txs := block.Transactions()
	if uint64(txIndex) > uint64(len(txs)) {
		return nil, nil // not error
	} else if uint64(txIndex) == uint64(len(txs)) {
		if chainConfig.Bor == nil {
			return nil, nil // not error
		}
		borTx, _, _, _ := rawdb.ReadBorTransactionForBlock(tx, block)
		if borTx == nil {
			return nil, nil // not error
		}
		derivedBorTxHash := types2.ComputeBorTxHash(block.NumberU64(), block.Hash())
		return newRPCBorTransaction(borTx, derivedBorTxHash, block.Hash(), block.NumberU64(), uint64(txIndex), block.BaseFee(), chainConfig.ChainID), nil
	}

	return newRPCTransaction(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex), block.BaseFee()), nil
}

// GetRawTransactionByBlockHashAndIndex returns the bytes of the transaction for the given block hash and index.
func (api *APIImpl) GetRawTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index hexutil.Uint) (hexutility.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
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
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	return newRPCRawTransactionFromBlockIndex(block, uint64(index))
}

// GetTransactionByBlockNumberAndIndex implements eth_getTransactionByBlockNumberAndIndex. Returns information about a transaction given a block number and transaction index.
func (api *APIImpl) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*RPCTransaction, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockNumberAndIndex
	blockNum, hash, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(blockNr), tx, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	txs := block.Transactions()
	if uint64(txIndex) > uint64(len(txs)) {
		return nil, nil // not error
	} else if uint64(txIndex) == uint64(len(txs)) {
		if chainConfig.Bor == nil {
			return nil, nil // not error
		}
		borTx, _, _, _ := rawdb.ReadBorTransactionForBlock(tx, block)
		if borTx == nil {
			return nil, nil
		}
		derivedBorTxHash := types2.ComputeBorTxHash(block.NumberU64(), block.Hash())
		return newRPCBorTransaction(borTx, derivedBorTxHash, block.Hash(), block.NumberU64(), uint64(txIndex), block.BaseFee(), chainConfig.ChainID), nil
	}

	return newRPCTransaction(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex), block.BaseFee()), nil
}

// GetRawTransactionByBlockNumberAndIndex returns the bytes of the transaction for the given block number and index.
func (api *APIImpl) GetRawTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (hexutility.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
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
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	return newRPCRawTransactionFromBlockIndex(block, uint64(index))
}
