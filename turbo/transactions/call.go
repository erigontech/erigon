package transactions

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
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
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
)

const callTimeout = 5 * time.Minute

func DoCall(ctx context.Context, args ethapi.CallArgs, tx ethdb.GetterBeginner, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account, GasCap uint64, chainConfig *params.ChainConfig, pending *rpchelper.Pending) (*core.ExecutionResult, error) {
	// todo: Pending state is only known by the miner
	/*
		if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
			block, state, _ := b.eth.miner.Pending()
			return state, block.Header(), nil
		}
	*/
	blockNumber, hash, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, pending)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(tx)
	} else {
		stateReader = state.NewPlainDBState(tx, blockNumber)
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
				balance, _ := uint256.FromBig((*big.Int)(*account.Balance))
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
	msg := args.ToMessage(GasCap)

	blockCtx, txCtx := GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, tx)

	evm := vm.NewEVM(blockCtx, txCtx, state, chainConfig, vm.Config{})

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

func GetEvmContext(msg core.Message, header *types.Header, requireCanonical bool, db ethdb.Getter) (vm.BlockContext, vm.TxContext) {
	return vm.BlockContext{
			CanTransfer: core.CanTransfer,
			Transfer:    core.Transfer,
			GetHash:     getHashGetter(requireCanonical, db),
			Coinbase:    header.Coinbase,
			BlockNumber: new(big.Int).Set(header.Number),
			Time:        new(big.Int).SetUint64(header.Time),
			Difficulty:  new(big.Int).Set(header.Difficulty),
			GasLimit:    header.GasLimit,
		},
		vm.TxContext{
			Origin:   msg.From(),
			GasPrice: msg.GasPrice().ToBig(),
		}
}

func getHashGetter(requireCanonical bool, db ethdb.Getter) func(uint64) common.Hash {
	return func(n uint64) common.Hash {
		hash, err := rawdb.ReadCanonicalHash(db, n)
		if err != nil {
			log.Debug("can't get block hash by number", "number", n, "only-canonical", requireCanonical)
		}
		return hash
	}
}
