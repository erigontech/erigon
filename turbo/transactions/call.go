package transactions

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
)

const callTimeout = 5 * time.Minute

func DoCall(
	ctx context.Context,
	args ethapi.CallArgs,
	tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash,
	block *types.Block, overrides *map[common.Address]ethapi.Account,
	gasCap uint64,
	chainConfig *params.ChainConfig,
	filters *filters.Filters,
	stateCache kvcache.Cache,
	contractHasTEVM func(hash common.Hash) (bool, error),
) (*core.ExecutionResult, error) {
	// todo: Pending state is only known by the miner
	/*
		if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
			block, state, _ := b.eth.miner.Pending()
			return state, block.Header(), nil
		}
	*/
	stateReader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, filters, stateCache)
	if err != nil {
		return nil, err
	}
	state := state.New(stateReader)

	header := block.Header()

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
	msg, err := args.ToMessage(gasCap, baseFee)
	if err != nil {
		return nil, err
	}
	blockCtx, txCtx := GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, tx, contractHasTEVM)

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

func GetEvmContext(msg core.Message, header *types.Header, requireCanonical bool, tx kv.Tx, contractHasTEVM func(address common.Hash) (bool, error)) (vm.BlockContext, vm.TxContext) {
	var baseFee uint256.Int
	if header.Eip1559 {
		overflow := baseFee.SetFromBig(header.BaseFee)
		if overflow {
			panic(fmt.Errorf("header.BaseFee higher than 2^256-1"))
		}
	}
	return vm.BlockContext{
			CanTransfer:     core.CanTransfer,
			Transfer:        core.Transfer,
			GetHash:         getHashGetter(requireCanonical, tx),
			ContractHasTEVM: contractHasTEVM,
			Coinbase:        header.Coinbase,
			BlockNumber:     header.Number.Uint64(),
			Time:            header.Time,
			Difficulty:      new(big.Int).Set(header.Difficulty),
			GasLimit:        header.GasLimit,
			BaseFee:         &baseFee,
		},
		vm.TxContext{
			Origin:   msg.From(),
			GasPrice: msg.GasPrice().ToBig(),
		}
}

func getHashGetter(requireCanonical bool, tx kv.Tx) func(uint64) common.Hash {
	return func(n uint64) common.Hash {
		hash, err := rawdb.ReadCanonicalHash(tx, n)
		if err != nil {
			log.Debug("Can't get block hash by number", "number", n, "only-canonical", requireCanonical)
		}
		return hash
	}
}
