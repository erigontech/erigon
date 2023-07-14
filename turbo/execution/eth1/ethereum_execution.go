package eth1

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
)

// EthereumExecutionModule describes ethereum execution logic and indexing.
type EthereumExecutionModule struct {
	// Snapshots + MDBX
	blockReader services.FullBlockReader

	// MDBX database
	db kv.RwDB

	execution.UnimplementedExecutionServer
}

// func (execution.UnimplementedExecutionServer).InsertBodies(context.Context, *execution.InsertBodiesRequest) (*execution.EmptyMessage, error)
// func (execution.UnimplementedExecutionServer).InsertHeaders(context.Context, *execution.InsertHeadersRequest) (*execution.EmptyMessage, error)

// func (execution.UnimplementedExecutionServer).AssembleBlock(context.Context, *execution.EmptyMessage) (*types.ExecutionPayload, error)
// func (execution.UnimplementedExecutionServer).UpdateForkChoice(context.Context, *types.H256) (*execution.ForkChoiceReceipt, error)
// func (execution.UnimplementedExecutionServer).ValidateChain(context.Context, *types.H256) (*execution.ValidationReceipt, error)
