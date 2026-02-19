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
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
)

func (api *APIImpl) CallBundle(ctx context.Context, txHashes []common.Hash, stateBlockNumberOrHash rpc.BlockNumberOrHash, timeoutMilliSecondsPtr *int64) (map[string]any, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	if len(txHashes) == 0 {
		return nil, nil
	}

	var txs types.Transactions

	for _, txHash := range txHashes {
		blockNumber, _, ok, err := api.txnLookup(ctx, tx, txHash)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}

		err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber)
		if err != nil {
			return nil, err
		}

		block, err := api.blockByNumberWithSenders(ctx, tx, blockNumber)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, fmt.Errorf("block not found %d", blockNumber)
		}
		var txn types.Transaction
		for _, transaction := range block.Transactions() {
			if transaction.Hash() == txHash {
				txn = transaction
				break
			}
		}
		if txn == nil {
			return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
		}
		txs = append(txs, txn)
	}
	defer func(start time.Time) { log.Trace("Executing EVM call finished", "runtime", time.Since(start)) }(time.Now())

	stateBlockNumber, hash, latest, err := rpchelper.GetBlockNumber(ctx, stateBlockNumberOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if latest {
		cacheView, err := api.stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateReader = rpchelper.CreateLatestCachedStateReader(cacheView, tx)
	} else {
		stateReader, err = rpchelper.CreateHistoryStateReader(ctx, tx, stateBlockNumber+1, 0, api._txNumReader)
		if err != nil {
			return nil, err
		}
	}
	ibs := state.New(stateReader)

	parent, _ := api.headerByNumber(ctx, rpc.BlockNumber(stateBlockNumber), tx)
	if parent == nil {
		return nil, fmt.Errorf("block %d(%x) not found", stateBlockNumber, hash)
	}

	blockNumber := stateBlockNumber + 1

	timestamp := parent.Time + chainConfig.SecondsPerSlot()

	coinbase := parent.Coinbase
	header := &types.Header{
		ParentHash: parent.Hash(),
		GasLimit:   parent.GasLimit,
		Time:       timestamp,
		Difficulty: parent.Difficulty,
		Coinbase:   coinbase,
	}
	header.Number.SetUint64(blockNumber)

	signer := types.MakeSigner(chainConfig, blockNumber, timestamp)
	blockCtx := transactions.NewEVMBlockContext(engine, header, stateBlockNumberOrHash.RequireCanonical, tx, api._blockReader, chainConfig)
	rules := blockCtx.Rules(chainConfig)
	firstMsg, err := txs[0].AsMessage(*signer, nil, rules)
	if err != nil {
		return nil, err
	}

	txCtx := protocol.NewEVMTxContext(firstMsg)
	// Get a new instance of the EVM
	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})

	timeoutMilliSeconds := int64(5000)
	if timeoutMilliSecondsPtr != nil {
		timeoutMilliSeconds = *timeoutMilliSecondsPtr
	}
	timeout := time.Millisecond * time.Duration(timeoutMilliSeconds)
	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	// Setup the gas pool (also for unmetered requests)
	// and apply the message.
	gp := new(protocol.GasPool).AddGas(math.MaxUint64).AddBlobGas(math.MaxUint64)

	bundleHash := crypto.NewKeccakState()
	defer crypto.ReturnToPool(bundleHash)

	results := make([]map[string]any, 0, len(txs))
	for _, txn := range txs {
		msg, err := txn.AsMessage(*signer, nil, rules)
		msg.SetCheckNonce(false)
		msg.SetCheckGas(false)
		if err != nil {
			return nil, err
		}
		// Execute the transaction message
		result, err := protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
		if err != nil {
			return nil, err
		}
		// If the timer caused an abort, return an appropriate error message
		if evm.Cancelled() {
			return nil, fmt.Errorf("execution aborted (timeout = %v)", timeout)
		}

		txHash := txn.Hash().String()
		jsonResult := map[string]any{
			"txHash":  txHash,
			"gasUsed": result.ReceiptGasUsed,
		}
		bundleHash.Write(txn.Hash().Bytes())
		if result.Err != nil {
			jsonResult["error"] = result.Err.Error()
		} else {
			jsonResult["value"] = common.BytesToHash(result.Return())
		}

		results = append(results, jsonResult)
	}

	ret := map[string]any{}
	ret["results"] = results
	ret["bundleHash"] = hexutil.Encode(bundleHash.Sum(nil))
	return ret, nil
}

// GetBlockByNumber implements eth_getBlockByNumber. Returns information about a block given the block's number.
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]any, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	err = api.BaseAPI.checkPruneHistory(ctx, tx, number.Uint64())
	if err != nil {
		return nil, err
	}

	b, err := api.blockByNumber(ctx, number, tx)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
		}
		return nil, err
	}
	if b == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}
	additionalFields := make(map[string]any)

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	var borTx types.Transaction
	var borTxHash common.Hash
	if chainConfig.Bor != nil {
		possibleBorTxnHash := bortypes.ComputeBorTxHash(b.NumberU64(), b.Hash())
		_, ok, err := api.bridgeReader.EventTxnLookup(ctx, possibleBorTxnHash)
		if err != nil {
			return nil, err
		}
		if ok {
			borTx = bortypes.NewBorTransaction()
			borTxHash = possibleBorTxnHash
		}
	}

	response, err := ethapi.RPCMarshalBlockEx(b, true, fullTx, borTx, borTxHash, additionalFields)
	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	return response, err
}

// GetBlockByHash implements eth_getBlockByHash. Returns information about a block given the block's hash.
func (api *APIImpl) GetBlockByHash(ctx context.Context, numberOrHash rpc.BlockNumberOrHash, fullTx bool) (map[string]any, error) {
	if numberOrHash.BlockHash == nil {
		// some web3.js based apps (like ethstats client) for some reason call
		// eth_getBlockByHash with a block number as a parameter
		// so no matter how weird that is, we would love to support that.
		if numberOrHash.BlockNumber == nil {
			return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
		}
		return api.GetBlockByNumber(ctx, *numberOrHash.BlockNumber, fullTx)
	}

	hash := *numberOrHash.BlockHash
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	additionalFields := make(map[string]any)

	blockNumber, _, _, err := rpchelper.GetBlockNumber(ctx, numberOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, nil
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}

	block, err := api.blockByHashWithSenders(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}
	number := block.NumberU64()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	var borTx types.Transaction
	var borTxHash common.Hash
	if chainConfig.Bor != nil {
		possibleBorTxnHash := bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash())
		_, ok, err := api.bridgeReader.EventTxnLookup(ctx, possibleBorTxnHash)
		if err != nil {
			return nil, err
		}
		if ok {
			borTx = bortypes.NewBorTransaction()
			borTxHash = possibleBorTxnHash
		}
	}

	response, err := ethapi.RPCMarshalBlockEx(block, true, fullTx, borTx, borTxHash, additionalFields)
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
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if blockNr == rpc.PendingBlockNumber {
		b, err := api.blockByNumberWithSenders(ctx, tx, blockNr.Uint64())
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, nil
		}
		n := hexutil.Uint(len(b.Transactions()))
		return &n, nil
	}

	blockNum, blockHash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNr), tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
		}
		return nil, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	latestBlockNumber, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return nil, err
	}
	if blockNum > latestBlockNumber {
		// (Compatibility) Every other node just returns `null` for when the block does not exist.
		return nil, nil
	}

	body, txCount, err := api._blockReader.Body(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	if chainConfig.Bor != nil {
		borStateSyncTxHash := bortypes.ComputeBorTxHash(blockNum, blockHash)

		_, ok, err := api.bridgeReader.EventTxnLookup(ctx, borStateSyncTxHash)
		if err != nil {
			return nil, err
		}
		if ok {
			txCount++
		}
	}

	numOfTx := hexutil.Uint(txCount)

	return &numOfTx, nil
}

// GetBlockTransactionCountByHash implements eth_getBlockTransactionCountByHash. Returns the number of transactions in a block given the block's block hash.
func (api *APIImpl) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHash{BlockHash: &blockHash}, tx, api._blockReader, nil)
	if err != nil {
		// (Compatibility) Every other node just return `null` for when the block does not exist.
		log.Debug("eth_getBlockTransactionCountByHash GetBlockNumber failed", "err", err)
		return nil, nil
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	_, txCount, err := api._blockReader.Body(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	if chainConfig.Bor != nil {
		borStateSyncTxHash := bortypes.ComputeBorTxHash(blockNum, blockHash)

		_, ok, err := api.bridgeReader.EventTxnLookup(ctx, borStateSyncTxHash)
		if err != nil {
			return nil, err
		}
		if ok {
			txCount++
		}
	}

	numOfTx := hexutil.Uint(txCount)

	return &numOfTx, nil
}

func (api *APIImpl) blockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, tx kv.Tx) (*types.Block, error) {
	if blockNumber != rpc.PendingBlockNumber {
		return api.blockByNumberWithSenders(ctx, tx, blockNumber.Uint64())
	}

	if block := api.pendingBlock(); block != nil {
		return block, nil
	}

	block, err := api.ethBackend.PendingBlock(ctx)
	if err != nil {
		return nil, err
	}
	if block != nil {
		return block, nil
	}

	return api.blockByNumberWithSenders(ctx, tx, blockNumber.Uint64())
}
