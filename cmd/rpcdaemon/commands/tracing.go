package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// TraceTransaction returns the structured logs created during the execution of EVM
// and returns them as a JSON object.
func (api *PrivateDebugAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash, config *eth.TraceConfig) (interface{}, error) {
	tx, err := api.dbReader.Begin(ctx)
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
	msg, vmctx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, params.MainnetChainConfig, chainContext, api.db, blockHash, txIndex)
	if err != nil {
		return nil, err
	}
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, vmctx, ibs, config)
}
