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
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
)

type Bundle struct {
	Transactions  []ethapi.CallArgs
	BlockOverride ethapi.BlockOverrides
}

type StateContext struct {
	BlockNumber      rpc.BlockNumberOrHash
	TransactionIndex *int
}

func validateBundles(bundles []Bundle, chainID *uint256.Int) error {
	empty := true
	for b, bundle := range bundles {
		for i := range bundle.Transactions {
			if err := ethapi.CheckChainID(bundle.Transactions[i].ChainID, chainID); err != nil {
				return fmt.Errorf("bundle %d, transaction %d: %w", b, i, err)
			}
			empty = false
		}
	}
	if empty {
		return errors.New("empty bundles")
	}
	return nil
}

// capEVMTimeout lets a client lower the server EVM timeout, never raise or remove it.
func capEVMTimeout(serverTimeout time.Duration, clientMilliSeconds int64) time.Duration {
	if clientMilliSeconds <= 0 {
		return serverTimeout
	}
	clientTimeout := time.Duration(min(clientMilliSeconds, math.MaxInt64/int64(time.Millisecond))) * time.Millisecond
	if serverTimeout > 0 {
		return min(clientTimeout, serverTimeout)
	}
	return clientTimeout
}

// setupEVMTimeout cancels the EVM registered via the returned store func once timeout
// elapses (or ctx is cancelled), and returns the deadline-carrying context. Callers must
// store the EVM once created — and again whenever it is recreated — so cancellation
// always hits the current instance, even when it fires before the EVM exists. The
// returned cleanup must be deferred.
func setupEVMTimeout(ctx context.Context, timeout time.Duration) (context.Context, func(*vm.EVM), func()) {
	evmPtr := &atomic.Pointer[vm.EVM]{}

	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	stop := context.AfterFunc(ctx, func() {
		if evm := evmPtr.Load(); evm != nil {
			evm.Cancel()
		}
	})
	store := func(evm *vm.EVM) {
		evmPtr.Store(evm)
		if ctx.Err() != nil {
			evm.Cancel()
		}
	}
	return ctx, store, func() {
		stop()
		cancel()
	}
}

func (api *APIImpl) CallMany(ctx context.Context, bundles []Bundle, simulateContext StateContext, stateOverride *ethapi.StateOverrides, timeoutMilliSecondsPtr *int64) ([][]map[string]any, error) {
	if err := requireBlockSelector(simulateContext.BlockNumber); err != nil {
		return nil, err
	}
	if err := rejectPendingState(simulateContext.BlockNumber); err != nil {
		return nil, err
	}
	var (
		evm               *vm.EVM
		blockCtx          evmtypes.BlockContext
		txCtx             evmtypes.TxContext
		overrideBlockHash map[uint64]common.Hash
	)

	overrideBlockHash = make(map[uint64]common.Hash)
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := validateBundles(bundles, chainConfig.ChainID); err != nil {
		return nil, err
	}

	defer func(start time.Time) { log.Trace("Executing EVM callMany finished", "runtime", time.Since(start)) }(time.Now())

	blockNum, hash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, simulateContext.BlockNumber, tx, api._blockReader)
	if err != nil {
		return nil, err
	}

	err = api.checkBlockHistoryAvailable(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	err = rpchelper.CheckBlockExecuted(tx, blockNum)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNum, hash)
	}

	// -1, the default, runs the bundles after the whole block; an index, also
	// the transaction count, before the block's closing operations.
	transactionIndex := -1
	if simulateContext.TransactionIndex != nil {
		transactionIndex = *simulateContext.TransactionIndex
	}
	afterBlock := transactionIndex == -1
	if afterBlock {
		transactionIndex = len(block.Transactions())
	}
	if transactionIndex < 0 || transactionIndex > len(block.Transactions()) {
		return nil, fmt.Errorf("transaction index %d out of range: block %d has %d transactions", transactionIndex, blockNum, len(block.Transactions()))
	}

	// History holds the state block execution left before the transaction, or
	// after the block with its closing system calls: replaying the block's
	// transactions alone misses what the block and its engine do around them.
	var stateReader state.StateReader
	if afterBlock {
		stateReader, err = rpchelper.CreateStateReaderFromBlockNumber(ctx, tx, blockNum, isLatest, -1, api.stateCache, api._txNumReader)
	} else {
		cacheView, viewErr := api.stateCache.View(ctx, tx)
		if viewErr != nil {
			return nil, viewErr
		}
		stateReader, err = rpchelper.CreateHistoryCachedStateReader(ctx, cacheView, tx, blockNum, transactionIndex, api._txNumReader)
	}
	if err != nil {
		return nil, err
	}

	st := state.New(stateReader)
	defer st.Close()
	transactions.VersionedCallState(st)

	header := block.HeaderNoCopy()

	var timeoutMilliSeconds int64
	if timeoutMilliSecondsPtr != nil {
		timeoutMilliSeconds = *timeoutMilliSecondsPtr
	}
	timeout := capEVMTimeout(api.evmCallTimeout, timeoutMilliSeconds)

	ctx, storeEVM, cleanup := setupEVMTimeout(ctx, timeout)
	defer cleanup()

	getHash := transactions.MakeBlockHashProvider(ctx, tx, api._blockReader, overrideBlockHash)

	blockCtx = protocol.NewEVMBlockContext(header, getHash, api.engine(), accounts.NilAddress /* author */, chainConfig)

	// Get a new instance of the EVM
	evm = vm.NewEVM(blockCtx, txCtx, st, chainConfig, vm.Config{})
	storeEVM(evm)
	rules := evm.ChainRules()

	// Setup the gas pool (also for unmetered requests)
	gp := new(protocol.GasPool).AddGas(math.MaxUint64).AddBlobGas(math.MaxUint64)

	st.SetTxContext(blockNum, transactionIndex)
	var activePrecompiles vm.PrecompiledContracts
	if stateOverride != nil {
		activePrecompiles = vm.ActivePrecompiledContracts(rules)
		err = stateOverride.Override(evm.IntraBlockState(), activePrecompiles, rules)
		if err != nil {
			return nil, err
		}
	}

	ret := make([][]map[string]any, 0)

	// Every transaction of the request gets the next index, so each sees the ones before it.
	txIndex := st.TxnIndex()
	for _, bundle := range bundles {
		// first change blockContext
		bundle.BlockOverride.OverrideBlockContext(&blockCtx, ethapi.BlockHashOverrides(overrideBlockHash))
		results := []map[string]any{}
		for i := range bundle.Transactions {
			txn := &bundle.Transactions[i]
			st.SetTxContext(blockNum, txIndex)
			txIndex++
			if txn.Gas == nil || *txn.Gas == 0 {
				txn.Gas = (*hexutil.Uint64)(&api.GasCap)
			}
			msg, err := txn.ToMessage(api.GasCap, &blockCtx.BaseFee)
			if err != nil {
				return nil, err
			}
			msg.SetSkipExecutionGasCap(true)
			txCtx = protocol.NewEVMTxContext(msg)
			evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{})
			evm.SetPrecompiles(activePrecompiles)
			storeEVM(evm)
			result, err := protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, api.engine())
			if err != nil {
				return nil, err
			}

			if err := ethapi.FinalizeCallTx(st, rules); err != nil {
				return nil, err
			}

			// If the timer caused an abort, return an appropriate error message
			if evm.Cancelled() {
				return nil, fmt.Errorf("execution aborted (timeout = %v)", timeout)
			}
			jsonResult := make(map[string]any)
			if result.Err != nil {
				if len(result.Revert()) > 0 {
					revertErr := ethapi.NewRevertError(result)
					jsonResult["error"] = map[string]any{
						"message": revertErr.Error(),
						"data":    revertErr.ErrorData(),
					}
				} else {
					jsonResult["error"] = result.Err.Error()
				}
			} else {
				jsonResult["value"] = hex.EncodeToString(result.Return())
			}

			results = append(results, jsonResult)
		}

		blockCtx.BlockNumber++
		blockCtx.Time++
		ret = append(ret, results)
	}

	return ret, err
}
