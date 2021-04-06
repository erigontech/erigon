package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/eth/tracers"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// TraceTransaction implements debug_traceTransaction. Returns Geth style transaction traces.
func (api *PrivateDebugAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash, config *tracers.TraceConfig) (interface{}, error) {
	tx, err := api.dbReader.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the transaction and assemble its EVM context
	txn, blockHash, _, txIndex := rawdb.ReadTransaction(ethdb.NewRoTxDb(tx), hash)
	if txn == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}
	getter := adapter.NewBlockGetter(tx)
	chainContext := adapter.NewChainContext(tx)

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, chainContext.GetHeader, chainContext.Engine(), tx, blockHash, txIndex)
	if err != nil {
		return nil, err
	}
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig)
}

func (api *PrivateDebugAPIImpl) TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig) (interface{}, error) {
	dbtx, err := api.dbReader.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	blockNumber, hash, err := rpchelper.GetBlockNumber(blockNrOrHash, dbtx, api.pending)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(dbtx)
	} else {
		stateReader = state.NewPlainDBState(ethdb.NewRoTxDb(dbtx), blockNumber)
	}
	header := rawdb.ReadHeader(ethdb.NewRoTxDb(dbtx), hash, blockNumber)
	if header == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}
	ibs := state.New(stateReader)
	msg := args.ToMessage(api.GasCap)
	blockCtx, txCtx := transactions.GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, dbtx)
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig)
}
