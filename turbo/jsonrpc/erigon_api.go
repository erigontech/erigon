// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"

	"github.com/erigontech/erigon-lib/common/hexutil"

	"github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/eth/filters"

	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
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
