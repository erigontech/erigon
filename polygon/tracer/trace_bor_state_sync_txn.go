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

package tracer

import (
	"context"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	jsonstream "github.com/erigontech/erigon-lib/json"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/tracers"
	tracersConfig "github.com/erigontech/erigon/eth/tracers/config"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/turbo/transactions"
)

func TraceBorStateSyncTxnDebugAPI(
	ctx context.Context,
	chainConfig *chain.Config,
	traceConfig *tracersConfig.TraceConfig,
	ibs *state.IntraBlockState,
	blockHash common.Hash,
	blockNum uint64,
	blockTime uint64,
	blockCtx evmtypes.BlockContext,
	stream jsonstream.Stream,
	callTimeout time.Duration,
	msgs []*types.Message,
	txIndex int,
) (gasUsed uint64, err error) {
	txCtx := initStateSyncTxContext(blockNum, blockHash)
	tracer, streaming, cancel, err := transactions.AssembleTracer(ctx, traceConfig, txCtx.TxHash, blockHash, txIndex, stream, callTimeout)
	if err != nil {
		stream.WriteNil()
		return gasUsed, err
	}

	defer cancel()
	stateReceiverContract := chainConfig.Bor.(*borcfg.BorConfig).StateReceiverContractAddress()
	tracer = NewBorStateSyncTxnTracer(tracer, stateReceiverContract)
	rules := chainConfig.Rules(blockNum, blockTime)
	stateWriter := state.NewNoopWriter()
	execCb := func(evm *vm.EVM, refunds bool) (*evmtypes.ExecutionResult, error) {
		tracer.OnTxStart(evm.GetVMContext(), bortypes.NewBorTransaction(), common.Address{})
		res, err := traceBorStateSyncTxn(ctx, ibs, stateWriter, msgs, evm, rules, txCtx, refunds)
		tracer.OnTxEnd(&types.Receipt{}, err)
		if err != nil {
			return res, err
		}
		gasUsed = res.GasUsed
		return res, nil
	}

	err = transactions.ExecuteTraceTx(blockCtx, txCtx, ibs, traceConfig, chainConfig, stream, tracer, streaming, execCb)
	return gasUsed, err
}

func TraceBorStateSyncTxnTraceAPI(
	ctx context.Context,
	vmConfig *vm.Config,
	chainConfig *chain.Config,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	blockCtx evmtypes.BlockContext,
	blockHash common.Hash,
	blockNum uint64,
	blockTime uint64,
	msgs []*types.Message,
	tracer *tracers.Tracer,
) (*evmtypes.ExecutionResult, error) {
	stateReceiverContract := chainConfig.Bor.(*borcfg.BorConfig).StateReceiverContractAddress()
	if tracer != nil {
		vmConfig.Tracer = NewBorStateSyncTxnTracer(tracer, stateReceiverContract).Hooks
	}

	txCtx := initStateSyncTxContext(blockNum, blockHash)
	rules := chainConfig.Rules(blockNum, blockTime)
	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, *vmConfig)

	return traceBorStateSyncTxn(ctx, ibs, stateWriter, msgs, evm, rules, txCtx, true)
}

func traceBorStateSyncTxn(
	ctx context.Context,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	msgs []*types.Message,
	evm *vm.EVM,
	rules *chain.Rules,
	txCtx evmtypes.TxContext,
	refunds bool,
) (*evmtypes.ExecutionResult, error) {
	for _, msg := range msgs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
		_, err := core.ApplyMessage(evm, msg, gp, refunds, false /* gasBailout */, nil /* engine */)
		if err != nil {
			return nil, err
		}

		err = ibs.FinalizeTx(rules, stateWriter)
		if err != nil {
			return nil, err
		}

		// reset to reuse
		evm.Reset(txCtx, ibs)
	}

	return &evmtypes.ExecutionResult{}, nil
}

func initStateSyncTxContext(blockNum uint64, blockHash common.Hash) evmtypes.TxContext {
	return evmtypes.TxContext{
		TxHash:   bortypes.ComputeBorTxHash(blockNum, blockHash),
		Origin:   common.Address{},
		GasPrice: uint256.NewInt(0),
	}
}
