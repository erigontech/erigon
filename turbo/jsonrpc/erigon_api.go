package jsonrpc

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/eth/filters"

	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

// ErigonAPI Erigon specific routines
type ErigonAPI interface {
	// System related (see ./erigon_system.go)
	Forks(ctx context.Context) (Forks, error)
	BlockNumber(ctx context.Context, rpcBlockNumPtr *rpc.BlockNumber) (hexutil.Uint64, error)

	// Blocks related (see ./erigon_blocks.go)
	GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
	GetHeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error)
	GetBlockByTimestamp(ctx context.Context, timeStamp rpc.Timestamp, fullTx bool) (map[string]interface{}, error)
	GetBalanceChangesInBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (map[common.Address]*hexutil.Big, error)

	// Receipt related (see ./erigon_receipts.go)
	GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error)
	//GetLogsByNumber(ctx context.Context, number rpc.BlockNumber) ([][]*types.Log, error)
	GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.ErigonLogs, error)
	GetLatestLogs(ctx context.Context, crit filters.FilterCriteria, logOptions filters.LogFilterOptions) (types.ErigonLogs, error)
	// Gets cannonical block receipt through hash. If the block is not cannonical returns error
	GetBlockReceiptsByBlockHash(ctx context.Context, cannonicalBlockHash common.Hash) ([]map[string]interface{}, error)

	// NodeInfo returns a collection of metadata known about the host.
	NodeInfo(ctx context.Context) ([]p2p.NodeInfo, error)
}

// ErigonImpl is implementation of the ErigonAPI interface
type ErigonImpl struct {
	*BaseAPI
	db         kv.RoDB
	ethBackend rpchelper.ApiBackend
}

// NewErigonAPI returns ErigonImpl instance
func NewErigonAPI(base *BaseAPI, db kv.RoDB, eth rpchelper.ApiBackend) *ErigonImpl {
	return &ErigonImpl{
		BaseAPI:    base,
		db:         db,
		ethBackend: eth,
	}
}
