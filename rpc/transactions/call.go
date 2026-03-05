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
	"fmt"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	ethapi2 "github.com/erigontech/erigon/rpc/ethapi"
)

func DoCall(
	ctx context.Context,
	engine rules.EngineReader,
	args ethapi2.CallArgs,
	tx kv.Tx,
	blockNrOrHash rpc.BlockNumberOrHash,
	header *types.Header,
	stateOverrides *ethapi2.StateOverrides,
	blockOverrides *ethapi2.BlockOverrides,
	gasCap uint64,
	chainConfig *chain.Config,
	stateReader state.StateReader,
	headerReader services.HeaderReader,
	callTimeout time.Duration,
) (*evmtypes.ExecutionResult, error) {
	// todo: Pending state is only known by the miner
	/*
		if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
			block, state, _ := b.eth.miner.Pending()
			return state, block.Header(), nil
		}
	*/

	state := state.New(stateReader)

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
	msg, err := args.ToMessage(gasCap, header.BaseFee)
	if err != nil {
		return nil, err
	}
	blockCtx := NewEVMBlockContext(engine, header, blockNrOrHash.RequireCanonical, tx, headerReader, chainConfig)
	if blockOverrides != nil {
		if err := blockOverrides.Override(&blockCtx); err != nil {
			return nil, err
		}
	}
	txCtx := protocol.NewEVMTxContext(msg)
	evm := vm.NewEVM(blockCtx, txCtx, state, chainConfig, vm.Config{NoBaseFee: true})
	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	// Override the fields of specified contracts before execution.
	if stateOverrides != nil {
		rules := blockCtx.Rules(chainConfig)
		precompiles := vm.ActivePrecompiledContracts(rules)
		if err := stateOverrides.Override(state, precompiles, blockCtx.Rules(chainConfig)); err != nil {
			return nil, err
		}
		evm.SetPrecompiles(precompiles)
	}

	gp := new(protocol.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	result, err := protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
	if err != nil {
		return nil, err
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", callTimeout)
	}
	return result, nil
}

func NewEVMBlockContextWithOverrides(ctx context.Context, engine rules.EngineReader, header *types.Header, tx kv.Getter,
	reader services.CanonicalReader, config *chain.Config, blockOverrides *ethapi2.BlockOverrides, blockHashOverrides ethapi2.BlockHashOverrides) evmtypes.BlockContext {
	blockHashFunc := MakeBlockHashProvider(ctx, tx, reader, blockHashOverrides)
	blockContext := protocol.NewEVMBlockContext(header, blockHashFunc, engine, accounts.NilAddress /* author */, config)
	if blockOverrides != nil {
		blockOverrides.OverrideBlockContext(&blockContext, blockHashOverrides)
	}
	return blockContext
}

func NewEVMBlockContext(engine rules.EngineReader, header *types.Header, requireCanonical bool, tx kv.Getter,
	headerReader services.HeaderReader, config *chain.Config) evmtypes.BlockContext {
	blockHashFunc := MakeHeaderGetter(requireCanonical, tx, headerReader)
	return protocol.NewEVMBlockContext(header, blockHashFunc, engine, accounts.NilAddress /* author */, config)
}

type BlockHashProvider func(blockNum uint64) (common.Hash, error)

func MakeBlockHashProvider(ctx context.Context, tx kv.Getter, reader services.CanonicalReader, overrides ethapi2.BlockHashOverrides) BlockHashProvider {
	return func(blockNum uint64) (common.Hash, error) {
		if blockHash, ok := overrides[blockNum]; ok {
			return blockHash, nil
		}
		blockHash, ok, err := reader.CanonicalHash(ctx, tx, blockNum)
		if err != nil || !ok {
			log.Error("[evm] canonical hash not found", "blockNum", blockNum, "ok", ok, "err", err)
		}
		return blockHash, err
	}
}

func MakeHeaderGetter(requireCanonical bool, tx kv.Getter, headerReader services.HeaderReader) BlockHashProvider {
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

type ReusableCaller struct {
	evm            *vm.EVM
	gasCap         uint64
	baseFee        *uint256.Int
	stateReader    state.StateReader
	stateOverrides *ethapi2.StateOverrides
	rules          *chain.Rules
	callTimeout    time.Duration
	message        *types.Message
}

func (r *ReusableCaller) DoCallWithNewGas(
	ctx context.Context,
	newGas uint64,
	engine rules.EngineReader) (*evmtypes.ExecutionResult, error) {
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
	txCtx := protocol.NewEVMTxContext(r.message)
	ibs := state.New(r.stateReader)
	if r.stateOverrides != nil {
		precompiles := vm.ActivePrecompiledContracts(r.rules)
		if err := r.stateOverrides.Override(ibs, precompiles, r.rules); err != nil {
			return nil, err
		}
		r.evm.SetPrecompiles(precompiles)
	}
	r.evm.Reset(txCtx, ibs)

	timedOut := false
	go func() {
		<-ctx.Done()
		timedOut = true
	}()

	gp := new(protocol.GasPool).AddGas(r.message.Gas()).AddBlobGas(r.message.BlobGas())

	result, err := protocol.ApplyMessage(r.evm, r.message, gp, true /* refunds */, false /* gasBailout */, engine)
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
	engine rules.EngineReader,
	stateReader state.StateReader,
	stateOverrides *ethapi2.StateOverrides,
	blockOverrides *ethapi2.BlockOverrides,
	header *types.Header,
	initialArgs ethapi2.CallArgs,
	gasCap uint64,
	blockNrOrHash rpc.BlockNumberOrHash,
	tx kv.Tx,
	headerReader services.HeaderReader,
	chainConfig *chain.Config,
	callTimeout time.Duration,
) (*ReusableCaller, error) {

	ibs := state.New(stateReader)

	baseFee := header.BaseFee

	msg, err := initialArgs.ToMessage(gasCap, baseFee)
	if err != nil {
		return nil, err
	}

	blockCtx := NewEVMBlockContext(engine, header, blockNrOrHash.RequireCanonical, tx, headerReader, chainConfig)

	if blockOverrides != nil {
		if err := blockOverrides.Override(&blockCtx); err != nil {
			return nil, err
		}
	}
	txCtx := protocol.NewEVMTxContext(msg)

	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{NoBaseFee: true})

	return &ReusableCaller{
		evm:            evm,
		baseFee:        baseFee,
		gasCap:         gasCap,
		callTimeout:    callTimeout,
		stateReader:    stateReader,
		stateOverrides: stateOverrides,
		rules:          blockCtx.Rules(chainConfig),
		message:        msg,
	}, nil
}
