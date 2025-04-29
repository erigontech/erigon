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
	"math/big"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

type BlockOverrides struct {
	BlockNumber *hexutil.Uint64
	Coinbase    *common.Address
	Timestamp   *hexutil.Uint64
	GasLimit    *hexutil.Uint
	Difficulty  *hexutil.Uint
	BaseFee     *uint256.Int
	BlockHash   *map[uint64]common.Hash
}

type Bundle struct {
	Transactions  []ethapi.CallArgs
	BlockOverride BlockOverrides
}

type StateContext struct {
	BlockNumber      rpc.BlockNumberOrHash
	TransactionIndex *int
}

func blockHeaderOverride(blockCtx *evmtypes.BlockContext, blockOverride BlockOverrides, overrideBlockHash map[uint64]common.Hash) {
	if blockOverride.BlockNumber != nil {
		blockCtx.BlockNumber = uint64(*blockOverride.BlockNumber)
	}
	if blockOverride.BaseFee != nil {
		blockCtx.BaseFee = blockOverride.BaseFee
	}
	if blockOverride.Coinbase != nil {
		blockCtx.Coinbase = *blockOverride.Coinbase
	}
	if blockOverride.Difficulty != nil {
		blockCtx.Difficulty = new(big.Int).SetUint64(uint64(*blockOverride.Difficulty))
	}
	if blockOverride.Timestamp != nil {
		blockCtx.Time = uint64(*blockOverride.Timestamp)
	}
	if blockOverride.GasLimit != nil {
		blockCtx.GasLimit = uint64(*blockOverride.GasLimit)
	}
	if blockOverride.BlockHash != nil {
		for blockNum, hash := range *blockOverride.BlockHash {
			overrideBlockHash[blockNum] = hash
		}
	}
}

func (api *APIImpl) CallMany(ctx context.Context, bundles []Bundle, simulateContext StateContext, stateOverride *ethapi.StateOverrides, timeoutMilliSecondsPtr *int64) ([][]map[string]interface{}, error) {
	var (
		hash               common.Hash
		replayTransactions types.Transactions
		evm                *vm.EVM
		blockCtx           evmtypes.BlockContext
		txCtx              evmtypes.TxContext
		overrideBlockHash  map[uint64]common.Hash
	)

	overrideBlockHash = make(map[uint64]common.Hash)
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if len(bundles) == 0 {
		return nil, errors.New("empty bundles")
	}
	empty := true
	for _, bundle := range bundles {
		if len(bundle.Transactions) != 0 {
			empty = false
		}
	}

	if empty {
		return nil, errors.New("empty bundles")
	}

	defer func(start time.Time) { log.Trace("Executing EVM callMany finished", "runtime", time.Since(start)) }(time.Now())

	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, simulateContext.BlockNumber, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, err
	}

	// -1 is a default value for transaction index.
	// If it's -1, we will try to replay every single transaction in that block
	transactionIndex := -1

	if simulateContext.TransactionIndex != nil {
		transactionIndex = *simulateContext.TransactionIndex
	}

	if transactionIndex == -1 {
		transactionIndex = len(block.Transactions())
	}

	replayTransactions = block.Transactions()[:transactionIndex]

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum-1)), 0, api.filters, api.stateCache, chainConfig.ChainName)

	if err != nil {
		return nil, err
	}

	st := state.New(stateReader)

	header := block.Header()

	if header == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNum, hash)
	}

	getHash := func(i uint64) common.Hash {
		if hash, ok := overrideBlockHash[i]; ok {
			return hash
		}
		hash, ok, err := api._blockReader.CanonicalHash(ctx, tx, i)
		if err != nil || !ok {
			log.Debug("Can't get block hash by number", "number", i, "only-canonical", true, "err", err, "ok", ok)
		}
		return hash
	}

	blockCtx = core.NewEVMBlockContext(header, getHash, api.engine(), nil /* author */, chainConfig)

	// Get a new instance of the EVM
	evm = vm.NewEVM(blockCtx, txCtx, st, chainConfig, vm.Config{})
	signer := types.MakeSigner(chainConfig, blockNum, blockCtx.Time)
	rules := chainConfig.Rules(blockNum, blockCtx.Time)

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
	gp := new(core.GasPool).AddGas(math.MaxUint64).AddBlobGas(math.MaxUint64)
	for idx, txn := range replayTransactions {
		st.SetTxContext(idx)
		msg, err := txn.AsMessage(*signer, block.BaseFee(), rules)
		if err != nil {
			return nil, err
		}
		txCtx = core.NewEVMTxContext(msg)
		evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{})
		// Execute the transaction message
		_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, api.engine())
		if err != nil {
			return nil, err
		}

		_ = st.FinalizeTx(rules, state.NewNoopWriter())

		// If the timer caused an abort, return an appropriate error message
		if evm.Cancelled() {
			return nil, fmt.Errorf("execution aborted (timeout = %v)", timeout)
		}
	}

	// after replaying the txns, we want to overload the state
	// overload state
	if stateOverride != nil {
		err = stateOverride.Override((evm.IntraBlockState()).(*state.IntraBlockState))
		if err != nil {
			return nil, err
		}
	}

	ret := make([][]map[string]interface{}, 0)

	for _, bundle := range bundles {
		// first change blockContext
		if bundle.BlockOverride.BlockNumber != nil {
			blockCtx.BlockNumber = uint64(*bundle.BlockOverride.BlockNumber)
		}
		if bundle.BlockOverride.BaseFee != nil {
			blockCtx.BaseFee = bundle.BlockOverride.BaseFee
		}
		if bundle.BlockOverride.Coinbase != nil {
			blockCtx.Coinbase = *bundle.BlockOverride.Coinbase
		}
		if bundle.BlockOverride.Difficulty != nil {
			blockCtx.Difficulty = new(big.Int).SetUint64(uint64(*bundle.BlockOverride.Difficulty))
		}
		if bundle.BlockOverride.Timestamp != nil {
			blockCtx.Time = uint64(*bundle.BlockOverride.Timestamp)
		}
		if bundle.BlockOverride.GasLimit != nil {
			blockCtx.GasLimit = uint64(*bundle.BlockOverride.GasLimit)
		}
		if bundle.BlockOverride.BlockHash != nil {
			for blockNum, hash := range *bundle.BlockOverride.BlockHash {
				overrideBlockHash[blockNum] = hash
			}
		}
		results := []map[string]interface{}{}
		for _, txn := range bundle.Transactions {
			if txn.Gas == nil || *(txn.Gas) == 0 {
				txn.Gas = (*hexutil.Uint64)(&api.GasCap)
			}
			msg, err := txn.ToMessage(api.GasCap, blockCtx.BaseFee)
			if err != nil {
				return nil, err
			}
			txCtx = core.NewEVMTxContext(msg)
			evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{})
			result, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, api.engine())
			if err != nil {
				return nil, err
			}

			_ = st.FinalizeTx(rules, state.NewNoopWriter())

			// If the timer caused an abort, return an appropriate error message
			if evm.Cancelled() {
				return nil, fmt.Errorf("execution aborted (timeout = %v)", timeout)
			}
			jsonResult := make(map[string]interface{})
			if result.Err != nil {
				if len(result.Revert()) > 0 {
					revertErr := ethapi.NewRevertError(result)
					jsonResult["error"] = map[string]interface{}{
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
