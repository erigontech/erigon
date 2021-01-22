package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// TraceTransaction implements debug_traceTransaction. Returns Geth style transaction traces.
func (api *PrivateDebugAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash, config *eth.TraceConfig) (interface{}, error) {
	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the transaction and assemble its EVM context
	txn, blockHash, _, txIndex := rawdb.ReadTransaction(tx, hash)
	if txn == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}
	getter := adapter.NewBlockGetter(tx)
	chainContext := adapter.NewChainContext(tx)

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	msg, vmctx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, chainContext, tx.(ethdb.HasTx).Tx(), blockHash, txIndex)
	if err != nil {
		return nil, err
	}
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, vmctx, ibs, config, chainConfig)
}

func (api *PrivateDebugAPIImpl) TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *eth.TraceConfig) (interface{}, error) {
	dbtx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	blockNumber, hash, err := rpchelper.GetBlockNumber(blockNrOrHash, dbtx)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(dbtx)
	} else {
		stateReader = state.NewPlainDBState(dbtx, blockNumber)
	}
	header := rawdb.ReadHeader(dbtx, hash, blockNumber)
	if header == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}
	ibs := state.New(stateReader)
	msg := args.ToMessage(api.GasCap)
	evmCtx := transactions.GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, dbtx)
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, evmCtx, ibs, config, chainConfig)
}
