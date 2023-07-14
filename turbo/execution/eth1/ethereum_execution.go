package eth1

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
)

// EthereumExecutionModule describes ethereum execution logic and indexing.
type EthereumExecutionModule struct {
	// Snapshots + MDBX
	blockReader services.FullBlockReader

	// MDBX database
	db      kv.RwDB // main database
	chainDb kv.RwDB // this is a database used to queue insert headers/bodies requests

	execution.UnimplementedExecutionServer
}

func NewEthereumExecutionModule(blockReader services.FullBlockReader, db, chainDb kv.RwDB) *EthereumExecutionModule {
	return &EthereumExecutionModule{
		blockReader: blockReader,
		db:          db,
		chainDb:     chainDb,
	}
}

func (e *EthereumExecutionModule) getHeader(ctx context.Context, tx, tx2 kv.Tx, blockHash libcommon.Hash, blockNumber uint64) (*types.Header, error) {
	header := rawdb.ReadHeader(tx, blockHash, blockNumber)
	if header != nil {
		return header, nil
	}
	if e.blockReader == nil {
		return rawdb.ReadHeader(tx2, blockHash, blockNumber), nil
	}
	return e.blockReader.Header(ctx, tx2, blockHash, blockNumber)
}

func (e *EthereumExecutionModule) getBody(ctx context.Context, tx, tx2 kv.Tx, blockHash libcommon.Hash, blockNumber uint64) (*types.Body, error) {
	body, _, _ := rawdb.ReadBody(tx, blockHash, blockNumber)
	if body != nil {
		return body, nil
	}
	if e.blockReader == nil {
		body, _, _ := rawdb.ReadBody(tx2, blockHash, blockNumber)
		return body, nil
	}
	return e.blockReader.BodyWithTransactions(ctx, tx2, blockHash, blockNumber)
}

func (e *EthereumExecutionModule) canonicalHash(ctx context.Context, tx kv.Tx, blockNumber uint64) (libcommon.Hash, error) {
	if e.blockReader == nil {
		return rawdb.ReadCanonicalHash(tx, blockNumber)
	}
	return e.blockReader.CanonicalHash(ctx, tx, blockNumber)
}

// Remaining

// func (execution.UnimplementedExecutionServer).AssembleBlock(context.Context, *execution.EmptyMessage) (*types.ExecutionPayload, error)
// func (execution.UnimplementedExecutionServer).UpdateForkChoice(context.Context, *types.H256) (*execution.ForkChoiceReceipt, error)
// func (execution.UnimplementedExecutionServer).ValidateChain(context.Context, *types.H256) (*execution.ValidationReceipt, error)
