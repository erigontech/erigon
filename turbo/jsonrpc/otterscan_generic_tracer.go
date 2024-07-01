package jsonrpc

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/cmd/state/exec3"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
)

type GenericTracer interface {
	vm.EVMLogger
	SetTransaction(tx types.Transaction)
	Found() bool
}

func (api *OtterscanAPIImpl) genericTracer(dbtx kv.Tx, ctx context.Context, blockNum, txnID uint64, txIndex int, chainConfig *chain.Config, tracer GenericTracer) error {
	ttx := dbtx.(kv.TemporalTx)
	executor := exec3.NewTraceWorker(ttx, chainConfig, api.engine(), api._blockReader, tracer)

	// if block number changed, calculate all related field
	header, err := api._blockReader.HeaderByNumber(ctx, ttx, blockNum)
	if err != nil {
		return err
	}
	if header == nil {
		log.Warn("[rpc] header is nil", "blockNum", blockNum)
		return nil
	}
	executor.ChangeBlock(header)

	txn, err := api._txnReader.TxnByIdxInBlock(ctx, ttx, blockNum, txIndex)
	if err != nil {
		return err
	}
	if txn == nil {
		log.Warn("[rpc genericTracer] txn is nil", "blockNum", blockNum, "txIndex", txIndex)
		return nil
	}
	_, err = executor.ExecTxn(txnID, txIndex, txn)
	if err != nil {
		return err
	}
	return nil
}
