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
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

func TraceBorStateSyncTxn(
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

	rules := chainConfig.Rules(blockNum, blockTime)
	stateReceiverContract := libcommon.HexToAddress(chainConfig.Bor.(*borcfg.BorConfig).StateReceiverContract)
	borStateSyncTxHash := types.ComputeBorTxHash(blockNum, blockHash)
	tracer, streaming, cancel, err := transactions.AssembleTracer(ctx, traceConfig, borStateSyncTxHash, stream, callTimeout)
	if err != nil {
		stream.WriteNil()
		return err
	}

	defer cancel()
	tracer = NewBorStateSyncTxnTracer(tracer, len(stateSyncEvents), stateReceiverContract)

	txCtx := evmtypes.TxContext{
		TxHash:   borStateSyncTxHash,
		Origin:   libcommon.Address{},
		GasPrice: uint256.NewInt(0),
	}

	execCb := func(evm *vm.EVM, refunds bool) (*core.ExecutionResult, error) {
		for _, eventData := range stateSyncEvents {
			select {
			case <-ctx.Done():
				stream.WriteArrayEnd()
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

			err = ibs.FinalizeTx(rules, state.NewNoopWriter())
			if err != nil {
				return nil, err
			}

			// reset to reuse
			evm.Reset(txCtx, ibs)
		}

		return &core.ExecutionResult{}, nil
	}

	return transactions.ExecuteTraceTx(blockCtx, txCtx, ibs, traceConfig, chainConfig, stream, tracer, streaming, execCb)
}
