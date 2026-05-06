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
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
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
		blockNumber, txNum, ok, err := api.txnLookup(ctx, tx, txHash)
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

		txNumMin, err := api._txNumReader.Min(ctx, tx, blockNumber)
		if err != nil {
			return nil, err
		}
		if txNumMin+1 > txNum {
			return nil, fmt.Errorf("uint underflow txnums error txNum: %d, txNumMin: %d, blockNum: %d", txNum, txNumMin, blockNumber)
		}
		txnIndex := int(txNum - txNumMin - 1)
		txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNumber, txnIndex)
		if err != nil {
			return nil, err
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
	if chainConfig.IsLondon(blockNumber) {
		header.BaseFee = misc.CalcBaseFee(chainConfig, parent)
	}

	signer := types.MakeSigner(chainConfig, blockNumber, timestamp)
	blockCtx := transactions.NewEVMBlockContext(engine, header, stateBlockNumberOrHash.RequireCanonical, tx, api._blockReader, chainConfig)
	rules := blockCtx.Rules(chainConfig)
	firstMsg, err := txs[0].AsMessage(*signer, header.BaseFee, rules)
	if err != nil {
		return nil, err
	}

	txCtx := protocol.NewEVMTxContext(firstMsg)
	// Get a new instance of the EVM
	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})

	// evmPtr is updated atomically each time evm is recreated in the loop,
	// so the AfterFunc callback always cancels the current instance.
	var evmPtr atomic.Pointer[vm.EVM]
	evmPtr.Store(evm)

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

	stop := context.AfterFunc(ctx, func() { evmPtr.Load().Cancel() })
	defer stop()

	// Setup the gas pool (also for unmetered requests)
	// and apply the message.
	gp := new(protocol.GasPool).AddGas(math.MaxUint64).AddBlobGas(math.MaxUint64)

	bundleHash := crypto.NewKeccakState()
	defer crypto.ReturnToPool(bundleHash)

	results := make([]map[string]any, 0, len(txs))
	for _, txn := range txs {
		msg, err := txn.AsMessage(*signer, header.BaseFee, rules)
		if err != nil {
			return nil, err
		}
		msg.SetCheckNonce(false)
		msg.SetCheckGas(false)
		// Recreate EVM with the correct txCtx for this transaction
		evm = vm.NewEVM(blockCtx, protocol.NewEVMTxContext(msg), ibs, chainConfig, vm.Config{})
		evmPtr.Store(evm)
		// Execute the transaction message
		result, err := protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
		if err != nil {
			return nil, err
		}
		if evm.Cancelled() {
			return nil, fmt.Errorf("execution aborted (timeout = %v)", timeout)
		}
		if err = ibs.FinalizeTx(rules, state.NewNoopWriter()); err != nil {
			return nil, err
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
	borTx, borTxHash, err := api.lookupBorTx(ctx, chainConfig, b.NumberU64(), b.Hash())
	if err != nil {
		return nil, err
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
	borTx, borTxHash, err := api.lookupBorTx(ctx, chainConfig, block.NumberU64(), block.Hash())
	if err != nil {
		return nil, err
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
		b, err := api.blockByNumber(ctx, blockNr, tx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			// No pending block available: return 0x0
			n := hexutil.Uint(0)
			return &n, nil
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

	borTx, _, err := api.lookupBorTx(ctx, chainConfig, blockNum, blockHash)
	if err != nil {
		return nil, err
	}
	if borTx != nil {
		txCount++
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

	borTx, _, err := api.lookupBorTx(ctx, chainConfig, blockNum, blockHash)
	if err != nil {
		return nil, err
	}
	if borTx != nil {
		txCount++
	}

	numOfTx := hexutil.Uint(txCount)

	return &numOfTx, nil
}

// lookupBorTx checks whether the given block has a Bor state-sync transaction.
// Returns the synthetic transaction and its hash, or (nil, Hash{}, nil) if none.
func (api *APIImpl) lookupBorTx(ctx context.Context, chainConfig *chain.Config, blockNum uint64, blockHash common.Hash) (types.Transaction, common.Hash, error) {
	if chainConfig.Bor == nil {
		return nil, common.Hash{}, nil
	}
	borTxHash := bortypes.ComputeBorTxHash(blockNum, blockHash)
	_, ok, err := api.bridgeReader.EventTxnLookup(ctx, borTxHash)
	if err != nil {
		return nil, common.Hash{}, err
	}
	if !ok {
		return nil, common.Hash{}, nil
	}
	return bortypes.NewBorTransaction(), borTxHash, nil
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

	// No pending block available: return nil
	return nil, nil
}
