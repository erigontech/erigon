package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// Block Implements trace_block
func (api *TraceAPIImpl) Block(ctx context.Context, blockNr rpc.BlockNumber) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}

// Get Implements trace_get
func (api *TraceAPIImpl) Get(ctx context.Context, txHash common.Hash, indicies []hexutil.Uint64) (interface{}, error) {
	var stub []interface{}
	return stub, nil
}

// Transaction Implements trace_transaction
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}
