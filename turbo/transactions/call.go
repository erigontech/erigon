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

package transactions

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	ethapi2 "github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/turbo/services"
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

type BlockHashOverrides map[uint64]common.Hash

func BlockHeaderOverride(blockCtx *evmtypes.BlockContext, blockOverride BlockOverrides, overrideBlockHash BlockHashOverrides) {
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

func DoCall(
	ctx context.Context,
	engine consensus.EngineReader,
	args ethapi2.CallArgs,
	tx kv.Tx,
	blockNrOrHash rpc.BlockNumberOrHash,
	header *types.Header,
	stateOverrides *ethapi2.StateOverrides,
	blockOverrides *BlockOverrides,
	blockHashOverrides BlockHashOverrides,
	gasCap uint64,
	chainConfig *chain.Config,
	stateReader state.StateReader,
	canonicalReader services.CanonicalReader,
	callTimeout time.Duration,
) (*evmtypes.ExecutionResult, types.Logs, error) {
	// todo: Pending state is only known by the miner
	/*
		if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
			block, state, _ := b.eth.miner.Pending()
			return state, block.Header(), nil
		}
	*/

	intraBlockState := state.New(stateReader)

	// Override the fields of specified contracts before execution.
	if stateOverrides != nil {
		if err := stateOverrides.Override(intraBlockState); err != nil {
			return nil, nil, err
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

	// Create a custom block context and apply any custom block overrides
	blockCtx := NewEVMBlockContextWithOverrides(ctx, engine, header, tx, canonicalReader, chainConfig, blockOverrides, blockHashOverrides)

	// Prepare the transaction message
	var baseFee *uint256.Int
	if blockCtx.BaseFee != nil {
		baseFee = blockCtx.BaseFee
	} else if header != nil && header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return nil, nil, errors.New("header.BaseFee uint256 overflow")
		}
	}
	msg, err := args.ToMessage(gasCap, baseFee)
	if err != nil {
		return nil, nil, err
	}
	txCtx := core.NewEVMTxContext(msg)

	// Get a new instance of the EVM
	evm := vm.NewEVM(blockCtx, txCtx, intraBlockState, chainConfig, vm.Config{NoBaseFee: true})

	// Wait for the context to be done and cancel the EVM. Even if the EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	result, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
	if err != nil {
		return nil, nil, err
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, nil, fmt.Errorf("execution aborted (timeout = %v)", callTimeout)
	}
	return result, intraBlockState.Logs(), nil
}

func NewEVMBlockContextWithOverrides(ctx context.Context, engine consensus.EngineReader, header *types.Header, tx kv.Getter,
	reader services.CanonicalReader, config *chain.Config, blockOverrides *BlockOverrides, blockHashOverrides BlockHashOverrides) evmtypes.BlockContext {
	blockHashFunc := MakeBlockHashProvider(ctx, tx, reader, blockHashOverrides)
	blockContext := core.NewEVMBlockContext(header, blockHashFunc, engine, nil /* author */, config)
	if blockOverrides != nil {
		BlockHeaderOverride(&blockContext, *blockOverrides, blockHashOverrides)
	}
	return blockContext
}

func NewEVMBlockContext(ctx context.Context, engine consensus.EngineReader, header *types.Header, tx kv.Getter,
	reader services.CanonicalReader, config *chain.Config) evmtypes.BlockContext {
	blockHashFunc := MakeBlockHashProvider(ctx, tx, reader, BlockHashOverrides{})
	blockContext := core.NewEVMBlockContext(header, blockHashFunc, engine, nil /* author */, config)
	return blockContext
}

func MakeHeaderGetter(requireCanonical bool, tx kv.Getter, headerReader services.HeaderReader) func(uint64) (common.Hash, error) {
	return func(n uint64) (common.Hash, error) {
		h, err := headerReader.HeaderByNumber(context.Background(), tx, n)
		if err != nil {
			log.Error("Can't get block hash by number", "number", n, "only-canonical", requireCanonical)
			return common.Hash{}, err
		}
		if h == nil {
			log.Warn("[evm] header is nil", "blockNum", n)
			return common.Hash{}, nil
		}
		return h.Hash(), nil
	}
}

type BlockHashProvider func(blockNum uint64) (common.Hash, error)

func MakeBlockHashProvider(ctx context.Context, tx kv.Getter, reader services.CanonicalReader, overrides BlockHashOverrides) BlockHashProvider {
	return func(blockNum uint64) (common.Hash, error) {
		if blockHash, ok := overrides[blockNum]; ok {
			return blockHash, nil
		}
		blockHash, ok, err := reader.CanonicalHash(ctx, tx, blockNum)
		if err != nil || !ok {
			log.Debug("Can't get block hash by number", "blockNum", blockNum, "ok", ok, "err", err)
		}
		return blockHash, err
	}
}

type ReusableCaller struct {
	evm             *vm.EVM
	intraBlockState *state.IntraBlockState
	gasCap          uint64
	baseFee         *uint256.Int
	stateReader     state.StateReader
	callTimeout     time.Duration
	message         *types.Message
}

func (r *ReusableCaller) DoCallWithNewGas(
	ctx context.Context,
	newGas uint64,
	engine consensus.EngineReader,
	overrides *ethapi2.StateOverrides,
) (*evmtypes.ExecutionResult, error) {
	var cancel context.CancelFunc
	if r.callTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, r.callTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	r.message.ChangeGas(r.gasCap, newGas)

	// reset the EVM so that we can continue to use it with the new context
	txCtx := core.NewEVMTxContext(r.message)
	if overrides == nil {
		r.intraBlockState = state.New(r.stateReader)
	}

	r.evm.Reset(txCtx, r.intraBlockState)

	timedOut := false
	go func() {
		<-ctx.Done()
		timedOut = true
	}()

	gp := new(core.GasPool).AddGas(r.message.Gas()).AddBlobGas(r.message.BlobGas())

	result, err := core.ApplyMessage(r.evm, r.message, gp, true /* refunds */, false /* gasBailout */, engine)
	if err != nil {
		return nil, err
	}

	// If the timer caused an abort, return an appropriate error message
	if timedOut {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", r.callTimeout)
	}

	return result, nil
}

func NewReusableCaller(
	ctx context.Context,
	engine consensus.EngineReader,
	stateReader state.StateReader,
	overrides *ethapi2.StateOverrides,
	header *types.Header,
	initialArgs ethapi2.CallArgs,
	gasCap uint64,
	tx kv.Tx,
	canonicalReader services.CanonicalReader,
	chainConfig *chain.Config,
	callTimeout time.Duration,
) (*ReusableCaller, error) {
	ibs := state.New(stateReader)

	if overrides != nil {
		if err := overrides.Override(ibs); err != nil {
			return nil, err
		}
	}

	var baseFee *uint256.Int
	if header != nil && header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return nil, errors.New("header.BaseFee uint256 overflow")
		}
	}

	msg, err := initialArgs.ToMessage(gasCap, baseFee)
	if err != nil {
		return nil, err
	}

	blockCtx := NewEVMBlockContext(ctx, engine, header, tx, canonicalReader, chainConfig)
	txCtx := core.NewEVMTxContext(msg)

	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{NoBaseFee: true})

	return &ReusableCaller{
		evm:             evm,
		intraBlockState: ibs,
		baseFee:         baseFee,
		gasCap:          gasCap,
		callTimeout:     callTimeout,
		stateReader:     stateReader,
		message:         msg,
	}, nil
}
