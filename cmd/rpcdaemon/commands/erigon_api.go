package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/rpc"
)

// ErigonAPI Erigon specific routines
type ErigonAPI interface {
	// System related (see ./erigon_system.go)
	Forks(ctx context.Context) (Forks, error)

	// Blocks related (see ./erigon_blocks.go)
	GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
	GetHeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error)
	GetBlockByTimeStamp(ctx context.Context, timeStamp uint64) (types.Block, error)

	// Receipt related (see ./erigon_receipts.go)
	GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error)
	//GetLogsByNumber(ctx context.Context, number rpc.BlockNumber) ([][]*types.Log, error)

	// WatchTheBurn / reward related (see ./erigon_issuance.go)
	WatchTheBurn(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error)

	// CumulativeChainTraffic / related to chain traffic (see ./erigon_cumulative_index.go)
	CumulativeChainTraffic(ctx context.Context, blockNr rpc.BlockNumber) (ChainTraffic, error)

	// NodeInfo returns a collection of metadata known about the host.
	NodeInfo(ctx context.Context) ([]p2p.NodeInfo, error)
}

// ErigonImpl is implementation of the ErigonAPI interface
type ErigonImpl struct {
	*BaseAPI
	db         kv.RoDB
	ethBackend services.ApiBackend
}

// NewErigonAPI returns ErigonImpl instance
func NewErigonAPI(base *BaseAPI, db kv.RoDB, eth services.ApiBackend) *ErigonImpl {
	return &ErigonImpl{
		BaseAPI:    base,
		db:         db,
		ethBackend: eth,
	}
}
