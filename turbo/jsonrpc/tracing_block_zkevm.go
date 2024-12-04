package jsonrpc

import (
	"context"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

type blockTracer struct {
	ctx            context.Context
	stream         *jsoniter.Stream
	engine         consensus.EngineReader
	tx             kv.Tx
	config         *tracers.TraceConfig_ZkEvm
	chainConfig    *chain.Config
	_blockReader   services.FullBlockReader
	historyV3      bool
	evmCallTimeout time.Duration
}

func (bt *blockTracer) TraceBlock(block *types.Block) error {
	txEnv, err := transactions.ComputeTxEnv_ZkEvm(bt.ctx, bt.engine, block, bt.chainConfig, bt._blockReader, bt.tx, 0, bt.historyV3)
	if err != nil {
		bt.stream.WriteNil()
		return err
	}
	bt.stream.WriteArrayStart()

	borTx := rawdb.ReadBorTransactionForBlock(bt.tx, block.NumberU64())
	txns := block.Transactions()
	if borTx != nil && *bt.config.BorTraceEnabled {
		txns = append(txns, borTx)
	}

	txTracerEnv := txTracerEnv{
		block:         block,
		txEnv:         txEnv,
		cumulativeGas: uint64(0),
		hermezReader:  hermez_db.NewHermezDbReader(bt.tx),
		chainConfig:   bt.chainConfig,
		engine:        bt.engine,
	}

	for idx, txn := range txns {
		if err := bt.traceLastTxFlushed(txTracerEnv, txn, idx); err != nil {
			return err
		}
	}
	bt.stream.WriteArrayEnd()
	bt.stream.Flush()

	return nil
}

func (bt *blockTracer) traceLastTxFlushed(txTracerEnv txTracerEnv, txn types.Transaction, idx int) error {
	if err := bt.traceTransactionUnflushed(txTracerEnv, txn, idx); err != nil {
		return err
	}

	if idx != len(txTracerEnv.block.Transactions())-1 {
		bt.stream.WriteMore()
	}
	bt.stream.Flush()
	return nil
}

func (bt *blockTracer) traceTransactionUnflushed(txTracerEnv txTracerEnv, txn types.Transaction, idx int) error {
	txHash := txn.Hash()
	bt.stream.WriteObjectStart()
	bt.stream.WriteObjectField("txHash")
	bt.stream.WriteString(txHash.Hex())
	bt.stream.WriteMore()
	bt.stream.WriteObjectField("result")
	select {
	default:
	case <-bt.ctx.Done():
		bt.stream.WriteNil()
		return bt.ctx.Err()
	}

	txCtx, msg, err := txTracerEnv.GetTxExecuteContext(txn, idx)
	if err != nil {
		bt.stream.WriteNil()
		return err
	}

	if err = transactions.TraceTx(
		bt.ctx,
		msg,
		txTracerEnv.txEnv.BlockContext,
		txCtx,
		txTracerEnv.txEnv.Ibs,
		bt.config,
		bt.chainConfig,
		bt.stream,
		bt.evmCallTimeout,
	); err == nil {
		rules := bt.chainConfig.Rules(txTracerEnv.block.NumberU64(), txTracerEnv.block.Time())
		err = txTracerEnv.txEnv.Ibs.FinalizeTx(rules, state.NewNoopWriter())
	}
	bt.stream.WriteObjectEnd()

	// if we have an error we want to output valid json for it before continuing after clearing down potential writes to the stream
	if err != nil {
		bt.handleError(err)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bt *blockTracer) handleError(err error) {
	bt.stream.WriteMore()
	bt.stream.WriteObjectStart()
	rpc.HandleError(err, bt.stream)
	bt.stream.WriteObjectEnd()
}

type txTracerEnv struct {
	hermezReader  state.ReadOnlyHermezDb
	chainConfig   *chain.Config
	engine        consensus.EngineReader
	block         *types.Block
	cumulativeGas uint64
	txEnv         transactions.TxEnv
}

func (tt *txTracerEnv) GetTxExecuteContext(txn types.Transaction, idx int) (evmtypes.TxContext, types.Message, error) {
	txHash := txn.Hash()
	evm, effectiveGasPricePercentage, err := core.PrepareForTxExecution(
		tt.chainConfig,
		&vm.Config{},
		&tt.txEnv.BlockContext,
		tt.hermezReader,
		tt.txEnv.Ibs,
		tt.block,
		&txHash,
		idx,
	)
	if err != nil {
		return evmtypes.TxContext{}, types.Message{}, err
	}

	msg, _, err := core.GetTxContext(
		tt.chainConfig,
		tt.engine,
		tt.txEnv.Ibs,
		tt.block.Header(),
		txn,
		evm,
		effectiveGasPricePercentage,
	)
	if err != nil {
		return evmtypes.TxContext{}, types.Message{}, err
	}

	txCtx := evmtypes.TxContext{
		TxHash:            txHash,
		Origin:            msg.From(),
		GasPrice:          msg.GasPrice(),
		Txn:               txn,
		CumulativeGasUsed: &tt.cumulativeGas,
		BlockNum:          tt.block.NumberU64(),
	}

	return txCtx, msg, nil
}
