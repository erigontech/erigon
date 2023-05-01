package commands

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
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
		blockCtx.Difficulty = big.NewInt(int64(*blockOverride.Difficulty))
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
		baseFee            uint256.Int
	)

	overrideBlockHash = make(map[uint64]common.Hash)
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	if len(bundles) == 0 {
		return nil, fmt.Errorf("empty bundles")
	}
	empty := true
	for _, bundle := range bundles {
		if len(bundle.Transactions) != 0 {
			empty = false
		}
	}

	if empty {
		return nil, fmt.Errorf("empty bundles")
	}

	defer func(start time.Time) { log.Trace("Executing EVM callMany finished", "runtime", time.Since(start)) }(time.Now())

	blockNum, hash, _, err := rpchelper.GetBlockNumber(simulateContext.BlockNumber, tx, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockByNumberWithSenders(tx, blockNum)
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

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum-1)), 0, api.filters, api.stateCache, api.historyV3(tx), chainConfig.ChainName)

	if err != nil {
		return nil, err
	}

	st := state.New(stateReader)

	parent := block.Header()

	if parent == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNum, hash)
	}

	getHash := func(i uint64) common.Hash {
		if hash, ok := overrideBlockHash[i]; ok {
			return hash
		}
		hash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			log.Debug("Can't get block hash by number", "number", i, "only-canonical", true)
		}
		return hash
	}

	if parent.BaseFee != nil {
		baseFee.SetFromBig(parent.BaseFee)
	}

	blockCtx = evmtypes.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     getHash,
		Coinbase:    parent.Coinbase,
		BlockNumber: parent.Number.Uint64(),
		Time:        parent.Time,
		Difficulty:  new(big.Int).Set(parent.Difficulty),
		GasLimit:    parent.GasLimit,
		BaseFee:     &baseFee,
	}

	// Get a new instance of the EVM
	evm = vm.NewEVM(blockCtx, txCtx, st, chainConfig, vm.Config{Debug: false})
	signer := types.MakeSigner(chainConfig, blockNum)
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
	gp := new(core.GasPool).AddGas(math.MaxUint64)
	for idx, txn := range replayTransactions {
		st.SetTxContext(txn.Hash(), block.Hash(), idx)
		msg, err := txn.AsMessage(*signer, block.BaseFee(), rules)
		if err != nil {
			return nil, err
		}
		txCtx = core.NewEVMTxContext(msg)
		evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{Debug: false})
		// Execute the transaction message
		_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
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
			blockCtx.Difficulty = big.NewInt(int64(*bundle.BlockOverride.Difficulty))
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
			evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{Debug: false})
			result, err := core.ApplyMessage(evm, msg, gp, true, false)
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
					jsonResult["error"] = ethapi.NewRevertError(result)
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
