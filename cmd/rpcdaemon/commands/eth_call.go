package commands

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// Call executes a new message call immediately without creating a transaction on the block chain.
func (api *APIImpl) Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account) (hexutil.Bytes, error) {
	tx, err1 := api.db.Begin(ctx, nil, false)
	if err1 != nil {
		return nil, fmt.Errorf("call cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	result, err := transactions.DoCall(ctx, args, tx, api.dbReader, blockNrOrHash, overrides, api.GasCap)
	if err != nil {
		return nil, err
	}

	// If the result contains a revert reason, try to unpack and return it.
	if len(result.Revert()) > 0 {
		return nil, ethapi.NewRevertError(result)
	}

	return result.Return(), result.Err
}

// EstimateGas returns an estimate of the amount of gas needed to execute the
// given transaction against the current pending block.
func (api *APIImpl) EstimateGas(ctx context.Context, args ethapi.CallArgs) (hexutil.Uint64, error) {
	//fixme: blockNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	hash := rawdb.ReadHeadBlockHash(api.dbReader)

	return api.DoEstimateGas(ctx, args, rpc.BlockNumberOrHash{BlockHash: &hash}, big.NewInt(0).SetUint64(api.GasCap))
}

func (api *APIImpl) DoEstimateGas(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, gasCap *big.Int) (hexutil.Uint64, error) {
	tx, err1 := api.db.Begin(ctx, nil, false)
	if err1 != nil {
		return 0, fmt.Errorf("estimateGas cannot open tx: %v", err1)
	}
	defer tx.Rollback()
	// Binary search the gas requirement, as it may be higher than the amount used
	var (
		lo  uint64 = params.TxGas - 1
		hi  uint64
		cap uint64
	)
	// Use zero address if sender unspecified.
	if args.From == nil {
		args.From = new(common.Address)
	}

	blockNumber, hash, err := rpchelper.GetBlockNumber(blockNrOrHash, api.dbReader)
	if err != nil {
		return 0, err
	}

	// Determine the highest gas limit can be used during the estimation.
	if args.Gas != nil && uint64(*args.Gas) >= params.TxGas {
		hi = uint64(*args.Gas)
	} else {
		// Retrieve the block to act as the gas ceiling
		header := rawdb.ReadHeader(api.dbReader, hash, blockNumber)
		hi = header.GasLimit
	}
	// Recap the highest gas limit with account's available balance.
	if args.GasPrice != nil && args.GasPrice.ToInt().Uint64() != 0 {
		ds := state.NewPlainDBState(tx, blockNumber)
		state := state.New(ds)
		if state == nil {
			return 0, fmt.Errorf("can't get the state for %d", blockNumber)
		}

		balance := state.GetBalance(*args.From) // from can't be nil
		available := balance.ToBig()
		if args.Value != nil {
			if args.Value.ToInt().Cmp(available) >= 0 {
				return 0, errors.New("insufficient funds for transfer")
			}
			available.Sub(available, args.Value.ToInt())
		}
		allowance := new(big.Int).Div(available, args.GasPrice.ToInt())
		if hi > allowance.Uint64() {
			transfer := args.Value
			if transfer == nil {
				transfer = new(hexutil.Big)
			}
			log.Warn("Gas estimation capped by limited funds", "original", hi, "balance", balance,
				"sent", transfer.ToInt(), "gasprice", args.GasPrice.ToInt(), "fundable", allowance)
			hi = allowance.Uint64()
		}
	}
	// Recap the highest gas allowance with specified gascap.
	if gasCap != nil && hi > gasCap.Uint64() {
		log.Warn("Caller gas above allowance, capping", "requested", hi, "cap", gasCap)
		hi = gasCap.Uint64()
	}
	cap = hi

	// Create a helper to check if a gas allowance results in an executable transaction
	executable := func(gas uint64) (bool, *core.ExecutionResult, error) {
		args.Gas = (*hexutil.Uint64)(&gas)

		result, err := transactions.DoCall(ctx, args, tx, api.dbReader, blockNrOrHash, nil, api.GasCap)
		if err != nil {
			if errors.Is(err, core.ErrIntrinsicGas) {
				// Special case, raise gas limit
				return true, nil, nil
			}

			// Bail out
			return true, nil, err
		}
		return result.Failed(), result, nil
	}
	// Execute the binary search and hone in on an executable gas limit
	for lo+1 < hi {
		mid := (hi + lo) / 2
		failed, _, err := executable(mid)

		// If the error is not nil(consensus error), it means the provided message
		// call or transaction will never be accepted no matter how much gas it is
		// assigened. Return the error directly, don't struggle any more.
		if err != nil {
			return 0, err
		}
		if failed {
			lo = mid
		} else {
			hi = mid
		}
	}
	// Reject the transaction as invalid if it still fails at the highest allowance
	if hi == cap {
		failed, result, err := executable(hi)
		if err != nil {
			return 0, err
		}
		if failed {
			if result != nil && !errors.Is(result.Err, vm.ErrOutOfGas) {
				if len(result.Revert()) > 0 {
					return 0, ethapi.NewRevertError(result)
				}
				return 0, result.Err
			}
			// Otherwise, the specified gas cap is too low
			return 0, fmt.Errorf("gas required exceeds allowance (%d)", cap)
		}
	}
	return hexutil.Uint64(hi), nil
}
