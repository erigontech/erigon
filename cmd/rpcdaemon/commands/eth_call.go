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
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// Call implements eth_call. Executes a new message call immediately without creating a transaction on the block chain.
func (api *APIImpl) Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account) (hexutil.Bytes, error) {
	dbtx, err := api.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	result, err := transactions.DoCall(ctx, args, dbtx, blockNrOrHash, overrides, api.GasCap, chainConfig, api.pending)
	if err != nil {
		return nil, err
	}

	// If the result contains a revert reason, try to unpack and return it.
	if len(result.Revert()) > 0 {
		return nil, ethapi.NewRevertError(result)
	}

	return result.Return(), result.Err
}

func HeaderByNumberOrHash(ctx context.Context, tx ethdb.Tx, blockNrOrHash rpc.BlockNumberOrHash) (*types.Header, error) {
	db := ethdb.NewRoTxDb(tx)
	if blockLabel, ok := blockNrOrHash.Number(); ok {
		blockNum, err := getBlockNumber(blockLabel, tx)
		if err != nil {
			return nil, err
		}
		return rawdb.ReadHeaderByNumber(db, blockNum), nil
	}
	if hash, ok := blockNrOrHash.Hash(); ok {
		header, err := rawdb.ReadHeaderByHash(db, hash)
		if err != nil {
			return nil, err
		}
		if header == nil {
			return nil, errors.New("header for hash not found")
		}

		if blockNrOrHash.RequireCanonical {
			can, err := rawdb.ReadCanonicalHash(db, header.Number.Uint64())
			if err != nil {
				return nil, err
			}
			if can != hash {
				return nil, errors.New("hash is not currently canonical")
			}
		}

		h := rawdb.ReadHeader(db, hash, header.Number.Uint64())
		if h == nil {
			return nil, errors.New("header found, but block body is missing")
		}
		return h, nil
	}
	return nil, errors.New("invalid arguments; neither block nor hash specified")
}

// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
func (api *APIImpl) EstimateGas(ctx context.Context, args ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash) (hexutil.Uint64, error) {
	bNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	if blockNrOrHash != nil {
		bNrOrHash = *blockNrOrHash
	}

	dbtx, err := api.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer dbtx.Rollback()

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

	// Determine the highest gas limit can be used during the estimation.
	if args.Gas != nil && uint64(*args.Gas) >= params.TxGas {
		hi = uint64(*args.Gas)
	} else {
		// Retrieve the block to act as the gas ceiling
		h, err := HeaderByNumberOrHash(ctx, dbtx, bNrOrHash)
		if err != nil {
			return 0, err
		}
		hi = h.GasLimit
	}

	// Recap the highest gas limit with account's available balance.
	if args.GasPrice != nil && args.GasPrice.ToInt().Uint64() != 0 {
		stateReader := state.NewPlainStateReader(ethdb.NewRoTxDb(dbtx))
		state := state.New(stateReader)
		if state == nil {
			return 0, fmt.Errorf("can't get the current state")
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
	if hi > api.GasCap {
		log.Warn("Caller gas above allowance, capping", "requested", hi, "cap", api.GasCap)
		hi = api.GasCap
	}
	cap = hi
	var lastBlockNum = rpc.LatestBlockNumber

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return 0, err
	}

	// Create a helper to check if a gas allowance results in an executable transaction
	executable := func(gas uint64) (bool, *core.ExecutionResult, error) {
		args.Gas = (*hexutil.Uint64)(&gas)

		result, err := transactions.DoCall(ctx, args, dbtx, rpc.BlockNumberOrHash{BlockNumber: &lastBlockNum}, nil, api.GasCap, chainConfig, api.pending)
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

// GetProof not implemented
func (api *APIImpl) GetProof(ctx context.Context, address common.Address, storageKeys []string, blockNr rpc.BlockNumber) (*interface{}, error) {
	var stub interface{}
	return &stub, fmt.Errorf(NotImplemented, "eth_getProof")
}
