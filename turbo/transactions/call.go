package transactions

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

const callTimeout = 5 * time.Minute

func DoCall(ctx context.Context, args ethapi.CallArgs, tx ethdb.Tx, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account, GasCap uint64, chainConfig *params.ChainConfig, filters *filters.Filters) (*core.ExecutionResult, error) {
	// todo: Pending state is only known by the miner
	/*
		if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
			block, state, _ := b.eth.miner.Pending()
			return state, block.Header(), nil
		}
	*/
	blockNrOrHash.RequireCanonical = true // DoCall cannot be executed on non-canonical blocks
	blockNumber, hash, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, filters)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(tx)
	} else {
		stateReader = state.NewPlainKvState(tx, blockNumber)
	}
	state := state.New(stateReader)

	header := rawdb.ReadHeader(tx, hash, blockNumber)
	if header == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}

	// Override the fields of specified contracts before execution.
	if overrides != nil {
		for addr, account := range *overrides {
			// Override account nonce.
			if account.Nonce != nil {
				state.SetNonce(addr, uint64(*account.Nonce))
			}
			// Override account(contract) code.
			if account.Code != nil {
				state.SetCode(addr, *account.Code)
			}
			// Override account balance.
			if account.Balance != nil {
				balance, overflow := uint256.FromBig((*big.Int)(*account.Balance))
				if overflow {
					return nil, fmt.Errorf("account.Balance higher than 2^256-1")
				}
				state.SetBalance(addr, balance)
			}
			if account.State != nil && account.StateDiff != nil {
				return nil, fmt.Errorf("account %s has both 'state' and 'stateDiff'", addr.Hex())
			}
			// Replace entire state if caller requires.
			if account.State != nil {
				state.SetStorage(addr, *account.State)
			}
			// Apply state diff into specified accounts.
			if account.StateDiff != nil {
				for key, value := range *account.StateDiff {
					key := key
					state.SetState(addr, &key, value)
				}
			}
		}
	}

	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if callTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, callTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	// Get a new instance of the EVM.
	var baseFee *uint256.Int
	if header != nil && header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return nil, fmt.Errorf("header.BaseFee uint256 overflow")
		}
	}
	msg, err := args.ToMessage(GasCap, baseFee)
	if err != nil {
		return nil, err
	}
	blockCtx, txCtx := GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, tx)

	evm := vm.NewEVM(blockCtx, txCtx, state, chainConfig, vm.Config{NoBaseFee: true})

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	gp := new(core.GasPool).AddGas(msg.Gas())
	result, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, err
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", callTimeout)
	}
	return result, nil
}

func GetEvmContext(msg core.Message, header *types.Header, requireCanonical bool, tx ethdb.Tx) (vm.BlockContext, vm.TxContext) {
	var baseFee uint256.Int
	if header.Eip1559 {
		overflow := baseFee.SetFromBig(header.BaseFee)
		if overflow {
			panic(fmt.Errorf("header.BaseFee higher than 2^256-1"))
		}
	}
	return vm.BlockContext{
			CanTransfer: core.CanTransfer,
			Transfer:    core.Transfer,
			GetHash:     getHashGetter(requireCanonical, tx),
			CheckTEVM:   func(common.Hash) (bool, error) { return false, nil },
			Coinbase:    header.Coinbase,
			BlockNumber: header.Number.Uint64(),
			Time:        header.Time,
			Difficulty:  new(big.Int).Set(header.Difficulty),
			GasLimit:    header.GasLimit,
			BaseFee:     &baseFee,
		},
		vm.TxContext{
			Origin:   msg.From(),
			GasPrice: msg.GasPrice().ToBig(),
		}
}

func getHashGetter(requireCanonical bool, tx ethdb.Tx) func(uint64) common.Hash {
	return func(n uint64) common.Hash {
		hash, err := rawdb.ReadCanonicalHash(tx, n)
		if err != nil {
			log.Debug("can't get block hash by number", "number", n, "only-canonical", requireCanonical)
		}
		return hash
	}
}
