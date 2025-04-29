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
	"encoding/json"

	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/eth/tracers/config"
	"github.com/erigontech/erigon/rpc"
)

// TraceAPI RPC interface into tracing API
type TraceAPI interface {
	// Ad-hoc (see ./trace_adhoc.go)

	ReplayBlockTransactions(ctx context.Context, blockNr rpc.BlockNumberOrHash, traceTypes []string, gasBailOut *bool, traceConfig *config.TraceConfig) ([]*TraceCallResult, error)
	ReplayTransaction(ctx context.Context, txHash common.Hash, traceTypes []string, gasBailOut *bool, traceConfig *config.TraceConfig) (*TraceCallResult, error)
	Call(ctx context.Context, call TraceCallParam, types []string, blockNr *rpc.BlockNumberOrHash, traceConfig *config.TraceConfig) (*TraceCallResult, error)
	CallMany(ctx context.Context, calls json.RawMessage, blockNr *rpc.BlockNumberOrHash, traceConfig *config.TraceConfig) ([]*TraceCallResult, error)
	RawTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error)

	// Filtering (see ./trace_filtering.go)

	Transaction(ctx context.Context, txHash common.Hash, gasBailOut *bool, traceConfig *config.TraceConfig) (ParityTraces, error)
	Get(ctx context.Context, txHash common.Hash, txIndicies []hexutil.Uint64, gasBailOut *bool, traceConfig *config.TraceConfig) (*ParityTrace, error)
	Block(ctx context.Context, blockNr rpc.BlockNumber, gasBailOut *bool, traceConfig *config.TraceConfig) (ParityTraces, error)
	Filter(ctx context.Context, req TraceFilterRequest, gasBailOut *bool, traceConfig *config.TraceConfig, stream *jsoniter.Stream) error
}

// TraceAPIImpl is implementation of the TraceAPI interface based on remote Db access
type TraceAPIImpl struct {
	*BaseAPI
	kv            kv.TemporalRoDB
	maxTraces     uint64
	gasCap        uint64
	compatibility bool // Bug for bug compatiblity with OpenEthereum
}

// NewTraceAPI returns NewTraceAPI instance
func NewTraceAPI(base *BaseAPI, kv kv.TemporalRoDB, cfg *httpcfg.HttpCfg) *TraceAPIImpl {
	return &TraceAPIImpl{
		BaseAPI:       base,
		kv:            kv,
		maxTraces:     cfg.MaxTraces,
		gasCap:        cfg.Gascap,
		compatibility: cfg.TraceCompatibility,
	}
}
