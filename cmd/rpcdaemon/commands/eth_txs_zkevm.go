package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces/txpool"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	types2 "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
)

func (api *APIImpl) forwardGetTransactionByHash(rpcUrl string, txnHash common.Hash, includeExtraInfo *bool) (json.RawMessage, error) {
	asString := txnHash.String()
	res, err := client.JSONRPCCall(rpcUrl, "eth_getTransactionByHash", asString, includeExtraInfo)
	if err != nil {
		return nil, err
	}

	if res.Error != nil {
		return nil, fmt.Errorf("RPC error response is: %s", res.Error.Message)
	}

	return res.Result, nil
}

// GetTransactionByHash implements eth_getTransactionByHash. Returns information about a transaction given the transaction's hash.
func (api *APIImpl) GetTransactionByHash(ctx context.Context, txnHash common.Hash, includeExtraInfo *bool) (interface{}, error) {
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

	// l2txhash is only after etrog
	isForkId7 := chainConfig.IsForkID7Etrog(blockNum)
	includel2TxHash := includeExtraInfo != nil && *includeExtraInfo && isForkId7

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
		block, err := api.blockByNumberWithSenders(tx, blockNum)
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

		return newRPCTransaction_zkevm(txn, blockHash, blockNum, txnIndex, baseFee, includel2TxHash), nil
	}

	if !sequencer.IsSequencer() {
		// forward the request on to the sequencer at this point as it is the only node with an active txpool
		return api.forwardGetTransactionByHash(api.l2RpcUrl, txnHash, includeExtraInfo)
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
		s := rlp.NewStream(bytes.NewReader(reply.RlpTxs[0]), uint64(len(reply.RlpTxs[0])))
		txn, err := types2.DecodeTransaction(s)
		if err != nil {
			return nil, err
		}

		// if no transaction was found in the txpool then we return nil and an error warning that we didn't find the transaction by the hash
		if txn == nil {
			return nil, nil
		}

		return newRPCPendingTransaction_zkevm(txn, curHeader, chainConfig, includel2TxHash), nil
	}

	// Transaction unknown, return as such
	return nil, nil
}

// GetTransactionByBlockHashAndIndex implements eth_getTransactionByBlockHashAndIndex. Returns information about a transaction given the block's hash and a transaction index.
func (api *APIImpl) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64, includeExtraInfo *bool) (*RPCTransaction, error) {
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
	block, err := api.blockByHashWithSenders(tx, blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	// l2txhash is only after etrog
	isForkId7 := chainConfig.IsForkID7Etrog(block.NumberU64())
	includel2TxHash := includeExtraInfo != nil && *includeExtraInfo && isForkId7

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

	return newRPCTransaction_zkevm(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex), block.BaseFee(), includel2TxHash), nil
}

// GetTransactionByBlockNumberAndIndex implements eth_getTransactionByBlockNumberAndIndex. Returns information about a transaction given a block number and transaction index.
func (api *APIImpl) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint, includeExtraInfo *bool) (*RPCTransaction, error) {
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
	blockNum, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(blockNr), tx, api.filters)
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

	// l2txhash is only after etrog
	isForkId7 := chainConfig.IsForkID7Etrog(block.NumberU64())
	includel2TxHash := includeExtraInfo != nil && *includeExtraInfo && isForkId7

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

	return newRPCTransaction_zkevm(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex), block.BaseFee(), includel2TxHash), nil
}
