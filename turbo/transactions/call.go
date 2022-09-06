package transactions

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

const callTimeout = 5 * time.Minute

func DoCall(
	ctx context.Context,
	args ethapi.CallArgs,
	tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash,
	block *types.Block, overrides *ethapi.StateOverrides,
	gasCap uint64,
	chainConfig *params.ChainConfig,
	stateReader state.StateReader,
	headerReader services.HeaderReader,
) (*core.ExecutionResult, error) {
	// todo: Pending state is only known by the miner
	/*
		if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
			block, state, _ := b.eth.miner.Pending()
			return state, block.Header(), nil
		}
	*/
	state := state.New(stateReader)

	header := block.Header()

	// Override the fields of specified contracts before execution.
	if overrides != nil {
		if err := overrides.Override(state); err != nil {
			return nil, err
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
	blockCtx, txCtx := GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, tx, headerReader)

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

func GetEvmContext(msg core.Message, header *types.Header, requireCanonical bool, tx kv.Tx, headerReader services.HeaderReader) (vm.BlockContext, vm.TxContext) {
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
			GetHash:     getHashGetter(requireCanonical, tx, headerReader),
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

func getHashGetter(requireCanonical bool, tx kv.Tx, headerReader services.HeaderReader) func(uint64) common.Hash {
	return func(n uint64) common.Hash {
		h, err := headerReader.HeaderByNumber(context.Background(), tx, n)
		if err != nil {
			log.Error("Can't get block hash by number", "number", n, "only-canonical", requireCanonical)
			return common.Hash{}
		}
		return h.Hash()
	}
}
