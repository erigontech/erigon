package arbitrum

import (
	"context"
	"fmt"
	"runtime"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

type TransactionArgs = ethapi.SendTxArgs

// func (args *TransactionArgs) ToTransaction() types.Transaction {
// 	return args.ToTransaction()
// }

type TransactionArgs2 = ethapi.CallArgs

func EstimateGas(
	ctx context.Context,
	b *APIBackend,
	argsOrNil *TransactionArgs2,
	blockNrOrHash *rpc.BlockNumberOrHash,
	overrides *ethapi.StateOverrides,
	gasCap uint64,
) (hexutil.Uint64, error) {

	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("EstimateGas called from %s:%d\n", file, line)
	return 0, nil

	// 	EstimateGas(ctx context.Context, call CallMsg) (uint64, error)
	// 	return ethapi.DoEstimateGas(ctx, b, args, blockNrOrHash, overrides, gasCap)
	// }
	// //
	// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
	// func (api *APIImpl) EstimateGas(ctx context.Context, argsOrNil *ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutil.Uint64, error) {
	// var args TransactionArgs2
	// // if we actually get CallArgs here, we use them
	// if argsOrNil != nil {
	// 	args = *argsOrNil
	// }

	// dbtx, err := api.ibs.BeginTemporalRo(ctx)
	// if err != nil {
	// 	return 0, err
	// }
	// defer dbtx.Rollback()

	// // Binary search the gas requirement, as it may be higher than the amount used
	// var (
	// 	lo = params.TxGas - 1
	// 	hi uint64
	// 	// gasCap uint64
	// )
	// // Use zero address if sender unspecified.
	// if args.From == nil {
	// 	args.From = new(common.Address)
	// }

	// bNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	// if blockNrOrHash != nil {
	// 	bNrOrHash = *blockNrOrHash
	// }

	// // Determine the highest gas limit can be used during the estimation.
	// if args.Gas != nil && uint64(*args.Gas) >= params.TxGas {
	// 	hi = uint64(*args.Gas)
	// } else {
	// 	// Retrieve the block to act as the gas ceiling
	// 	h, err := headerByNumberOrHash(ctx, dbtx, bNrOrHash, api)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	if h == nil {
	// 		// if a block number was supplied and there is no header return 0
	// 		if blockNrOrHash != nil {
	// 			return 0, nil
	// 		}

	// 		// block number not supplied, so we haven't found a pending block, read the latest block instead
	// 		h, err = headerByNumberOrHash(ctx, dbtx, latestNumOrHash, api)
	// 		if err != nil {
	// 			return 0, err
	// 		}
	// 		if h == nil {
	// 			return 0, nil
	// 		}
	// 		// Run the gas estimation andwrap any revertals into a custom return
	// 		// Arbitrum: this also appropriately recursively calls another args.ToMessage with increased gasCap by posterCostInL2Gas amount
	// 		// call, err := args.ToMessage(gasCap, header, state, core.MessageGasEstimationMode)
	// 		// if err != nil {
	// 		// 	return 0, err
	// 		// }

	// 		// Arbitrum: raise the gas cap to ignore L1 costs so that it's compute-only
	// 		{
	// 			gasCap, err = args.L2OnlyGasCap(gasCap, h)
	// 			if err != nil {
	// 				return 0, err
	// 			}
	// 		}

	// 	}
	// 	hi = h.GasLimit
	// }

	// var feeCap *big.Int
	// if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
	// 	return 0, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
	// } else if args.GasPrice != nil {
	// 	feeCap = args.GasPrice.ToInt()
	// } else if args.MaxFeePerGas != nil {
	// 	feeCap = args.MaxFeePerGas.ToInt()
	// } else {
	// 	feeCap = libcommon.Big0
	// }
	// // Recap the highest gas limit with account's available balance.
	// if feeCap.Sign() != 0 {
	// 	cacheView, err := api.stateCache.View(ctx, dbtx)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	stateReader := rpchelper.CreateLatestCachedStateReader(cacheView, dbtx)
	// 	state := state.New(stateReader)
	// 	if state == nil {
	// 		return 0, errors.New("can't get the current state")
	// 	}

	// 	balance, err := state.GetBalance(*args.From) // from can't be nil
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	available := balance.ToBig()
	// 	if args.Value != nil {
	// 		if args.Value.ToInt().Cmp(available) >= 0 {
	// 			return 0, errors.New("insufficient funds for transfer")
	// 		}
	// 		available.Sub(available, args.Value.ToInt())
	// 	}
	// 	allowance := new(big.Int).Div(available, feeCap)

	// 	// If the allowance is larger than maximum uint64, skip checking
	// 	if allowance.IsUint64() && hi > allowance.Uint64() {
	// 		transfer := args.Value
	// 		if transfer == nil {
	// 			transfer = new(hexutil.Big)
	// 		}
	// 		log.Warn("Gas estimation capped by limited funds", "original", hi, "balance", balance,
	// 			"sent", transfer.ToInt(), "maxFeePerGas", feeCap, "fundable", allowance)
	// 		hi = allowance.Uint64()
	// 	}
	// }

	// // Recap the highest gas allowance with specified gascap.
	// if hi > api.GasCap {
	// 	log.Warn("Caller gas above allowance, capping", "requested", hi, "cap", api.GasCap)
	// 	hi = api.GasCap
	// }
	// gasCap = hi

	// chainConfig, err := api.chainConfig(ctx, dbtx)
	// if err != nil {
	// 	return 0, err
	// }
	// engine := api.engine()

	// latestCanBlockNumber, latestCanHash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, latestNumOrHash, dbtx, api._blockReader, api.filters) // DoCall cannot be executed on non-canonical blocks
	// if err != nil {
	// 	return 0, err
	// }

	// // try and get the block from the lru cache first then try DB before failing
	// block := api.tryBlockFromLru(latestCanHash)
	// if block == nil {
	// 	block, err = api.blockWithSenders(ctx, dbtx, latestCanHash, latestCanBlockNumber)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// }
	// if block == nil {
	// 	return 0, errors.New("could not find latest block in cache or ibs")
	// }

	// txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))
	// stateReader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, dbtx, txNumsReader, latestCanBlockNumber, isLatest, 0, api.stateCache, chainConfig.ChainName)
	// if err != nil {
	// 	return 0, err
	// }
	// header := block.HeaderNoCopy()

	// caller, err := transactions.NewReusableCaller(engine, stateReader, overrides, header, args, api.GasCap, latestNumOrHash, dbtx, api._blockReader, chainConfig, api.evmCallTimeout)
	// if err != nil {
	// 	return 0, err
	// }

	// // Create a helper to check if a gas allowance results in an executable transaction
	// executable := func(gas uint64) (bool, *evmtypes.ExecutionResult, error) {
	// 	result, err := caller.DoCallWithNewGas(ctx, gas)
	// 	if err != nil {
	// 		if errors.Is(err, core.ErrIntrinsicGas) {
	// 			// Special case, raise gas limit
	// 			return true, nil, nil
	// 		}

	// 		// Bail out
	// 		return true, nil, err
	// 	}
	// 	return result.Failed(), result, nil
	// }

	// // Execute the binary search and hone in on an executable gas limit
	// for lo+1 < hi {
	// 	mid := (hi + lo) / 2
	// 	failed, _, err := executable(mid)
	// 	// If the error is not nil(consensus error), it means the provided message
	// 	// call or transaction will never be accepted no matter how much gas it is
	// 	// assigened. Return the error directly, don't struggle any more.
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	if failed {
	// 		lo = mid
	// 	} else {
	// 		hi = mid
	// 	}
	// }

	// // Reject the transaction as invalid if it still fails at the highest allowance
	// if hi == gasCap {
	// 	failed, result, err := executable(hi)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	if failed {
	// 		if result != nil && !errors.Is(result.Err, vm.ErrOutOfGas) {
	// 			if len(result.Revert()) > 0 {
	// 				return 0, ethapi2.NewRevertError(result)
	// 			}
	// 			return 0, result.Err
	// 		}
	// 		// Otherwise, the specified gas cap is too low
	// 		return 0, fmt.Errorf("gas required exceeds allowance (%d)", gasCap)
	// 	}
	// }
	// return hexutil.Uint64(hi), nil
	// ====== arbitrum estimation code

	// // Retrieve the base state and mutate it with any overrides
	// state, header, err := b.StateAndHeaderByNumberOrHash(ctx, blockNrOrHash)
	// if state == nil || err != nil {
	// 	return 0, err
	// }
	// if err = overrides.Apply(state); err != nil {
	// 	return 0, err
	// }
	// header = updateHeaderForPendingBlocks(blockNrOrHash, header)

	// // Construct the gas estimator option from the user input
	// opts := &gasestimator.Options{
	// 	Config:           b.ChainConfig(),
	// 	Chain:            NewChainContext(ctx, b),
	// 	Header:           header,
	// 	State:            state,
	// 	Backend:          b,
	// 	ErrorRatio:       gasestimator.EstimateGasErrorRatio,
	// 	RunScheduledTxes: runScheduledTxes,
	// }
	// // Run the gas estimation andwrap any revertals into a custom return
	// // Arbitrum: this also appropriately recursively calls another args.ToMessage with increased gasCap by posterCostInL2Gas amount
	// call, err := args.ToMessage(gasCap, header, state, core.MessageGasEstimationMode)
	// if err != nil {
	// 	return 0, err
	// }

	// // Arbitrum: raise the gas cap to ignore L1 costs so that it's compute-only
	// {
	// 	gasCap, err = args.L2OnlyGasCap(gasCap, header, state, core.MessageGasEstimationMode)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// }

	// estimate, revert, err := gasestimator.Estimate(ctx, call, opts, gasCap)
	// if err != nil {
	// 	if len(revert) > 0 {
	// 		return 0, newRevertError(revert)
	// 	}
	// 	return 0, err
	// }
	// return hexutil.Uint64(estimate), nil
}

// todo can use following?
// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
// func (api *APIImpl) EstimateGas(ctx context.Context, argsOrNil *ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutil.Uint64, error) {

func NewRevertReason(result *evmtypes.ExecutionResult) error {
	return ethapi.NewRevertError(result)
}
