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
	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
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
	blockHash libcommon.Hash,
	blockNum uint64,
	blockTime uint64,
	blockCtx evmtypes.BlockContext,
	stream *jsoniter.Stream,
	callTimeout time.Duration,
	msgs []*types.Message,
) error {
	txCtx := initStateSyncTxContext(blockNum, blockHash)
	tracer, streaming, cancel, err := transactions.AssembleTracer(ctx, traceConfig, txCtx.TxHash, stream, callTimeout)
	if err != nil {
		stream.WriteNil()
		return err
	}

	defer cancel()
	stateReceiverContract := libcommon.HexToAddress(chainConfig.Bor.(*borcfg.BorConfig).StateReceiverContract)
	tracer = NewBorStateSyncTxnTracer(tracer, len(msgs), stateReceiverContract)
	rules := chainConfig.Rules(blockNum, blockTime)
	stateWriter := state.NewNoopWriter()
	execCb := func(evm *vm.EVM, refunds bool) (*evmtypes.ExecutionResult, error) {
		return traceBorStateSyncTxn(ctx, ibs, stateWriter, msgs, evm, rules, txCtx, refunds)
	}

	return transactions.ExecuteTraceTx(blockCtx, txCtx, ibs, traceConfig, chainConfig, stream, tracer, streaming, execCb)
}

func TraceBorStateSyncTxnTraceAPI(
	ctx context.Context,
	vmConfig *vm.Config,
	chainConfig *chain.Config,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	blockCtx evmtypes.BlockContext,
	blockHash libcommon.Hash,
	blockNum uint64,
	blockTime uint64,
	msgs []*types.Message,
) (*evmtypes.ExecutionResult, error) {
	stateReceiverContract := libcommon.HexToAddress(chainConfig.Bor.(*borcfg.BorConfig).StateReceiverContract)
	if vmConfig.Tracer != nil {
		vmConfig.Tracer = NewBorStateSyncTxnTracer(vmConfig.Tracer, len(msgs), stateReceiverContract)
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
		_, err := core.ApplyMessage(evm, msg, gp, refunds, false /* gasBailout */)
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

func initStateSyncTxContext(blockNum uint64, blockHash libcommon.Hash) evmtypes.TxContext {
	return evmtypes.TxContext{
		TxHash:   bortypes.ComputeBorTxHash(blockNum, blockHash),
		Origin:   libcommon.Address{},
		GasPrice: uint256.NewInt(0),
	}
}
