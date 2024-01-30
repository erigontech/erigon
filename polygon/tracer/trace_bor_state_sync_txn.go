package tracer

import (
	"context"
	"time"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

func TraceBorStateSyncTxnDebugAPI(
	ctx context.Context,
	dbTx kv.Tx,
	chainConfig *chain.Config,
	traceConfig *tracers.TraceConfig,
	ibs *state.IntraBlockState,
	blockReader services.FullBlockReader,
	blockHash libcommon.Hash,
	blockNum uint64,
	blockTime uint64,
	blockCtx evmtypes.BlockContext,
	stream *jsoniter.Stream,
	callTimeout time.Duration,
) error {
	stateSyncEvents, err := blockReader.EventsByBlock(ctx, dbTx, blockHash, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}

	txCtx := initStateSyncTxContext(blockNum, blockHash)
	tracer, streaming, cancel, err := transactions.AssembleTracer(ctx, traceConfig, txCtx.TxHash, stream, callTimeout)
	if err != nil {
		stream.WriteNil()
		return err
	}

	defer cancel()
	stateReceiverContract := libcommon.HexToAddress(chainConfig.Bor.(*borcfg.BorConfig).StateReceiverContract)
	tracer = NewBorStateSyncTxnTracer(tracer, len(stateSyncEvents), stateReceiverContract)
	rules := chainConfig.Rules(blockNum, blockTime)
	stateWriter := state.NewNoopWriter()
	execCb := func(evm *vm.EVM, refunds bool) (*core.ExecutionResult, error) {
		return traceBorStateSyncTxn(ctx, ibs, stateWriter, stateReceiverContract, stateSyncEvents, evm, rules, txCtx, refunds)
	}

	return transactions.ExecuteTraceTx(blockCtx, txCtx, ibs, traceConfig, chainConfig, stream, tracer, streaming, execCb)
}

func TraceBorStateSyncTxnTraceAPI(
	ctx context.Context,
	dbTx kv.Tx,
	vmConfig *vm.Config,
	chainConfig *chain.Config,
	blockReader services.FullBlockReader,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	blockCtx evmtypes.BlockContext,
	blockHash libcommon.Hash,
	blockNum uint64,
	blockTime uint64,
) (*core.ExecutionResult, error) {
	stateSyncEvents, err := blockReader.EventsByBlock(ctx, dbTx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}

	stateReceiverContract := libcommon.HexToAddress(chainConfig.Bor.(*borcfg.BorConfig).StateReceiverContract)
	if vmConfig.Tracer != nil {
		vmConfig.Tracer = NewBorStateSyncTxnTracer(vmConfig.Tracer, len(stateSyncEvents), stateReceiverContract)
	}

	txCtx := initStateSyncTxContext(blockNum, blockHash)
	rules := chainConfig.Rules(blockNum, blockTime)
	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, *vmConfig)
	return traceBorStateSyncTxn(ctx, ibs, stateWriter, stateReceiverContract, stateSyncEvents, evm, rules, txCtx, true)
}

func traceBorStateSyncTxn(
	ctx context.Context,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	stateReceiverContract libcommon.Address,
	stateSyncEvents []rlp.RawValue,
	evm *vm.EVM,
	rules *chain.Rules,
	txCtx evmtypes.TxContext,
	refunds bool,
) (*core.ExecutionResult, error) {
	for _, eventData := range stateSyncEvents {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		msg := types.NewMessage(
			state.SystemAddress, // from
			&stateReceiverContract,
			0,         // nonce
			u256.Num0, // amount
			core.SysCallGasLimit,
			u256.Num0, // gasPrice
			nil,       // feeCap
			nil,       // tip
			eventData,
			nil,   // accessList
			false, // checkNonce
			true,  // isFree
			nil,   // maxFeePerBlobGas
		)

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

	return &core.ExecutionResult{}, nil
}

func initStateSyncTxContext(blockNum uint64, blockHash libcommon.Hash) evmtypes.TxContext {
	return evmtypes.TxContext{
		TxHash:   types.ComputeBorTxHash(blockNum, blockHash),
		Origin:   libcommon.Address{},
		GasPrice: uint256.NewInt(0),
	}
}
