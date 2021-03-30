package commands

import (
	"context"
	"encoding/json"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
)

// TraceAPI RPC interface into tracing API
type TraceAPI interface {
	// Ad-hoc (see ./trace_adhoc.go)
	ReplayBlockTransactions(ctx context.Context, blockNr rpc.BlockNumber, traceTypes []string) ([]interface{}, error)
	ReplayTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error)
	Call(ctx context.Context, call TraceCallParam, types []string, blockNr *rpc.BlockNumberOrHash) (*TraceCallResult, error)
	CallMany(ctx context.Context, calls json.RawMessage, blockNr *rpc.BlockNumberOrHash) ([]*TraceCallResult, error)
	RawTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error)

	// Filtering (see ./trace_filtering.go)
	Transaction(ctx context.Context, txHash common.Hash) (ParityTraces, error)
	Get(ctx context.Context, txHash common.Hash, txIndicies []hexutil.Uint64) (*ParityTrace, error)
	Block(ctx context.Context, blockNr rpc.BlockNumber) (ParityTraces, error)
	Filter(ctx context.Context, req TraceFilterRequest) (ParityTraces, error)
}

// TraceAPIImpl is implementation of the TraceAPI interface based on remote Db access
type TraceAPIImpl struct {
	*BaseAPI
	dbReader  ethdb.Database
	maxTraces uint64
	traceType string
	gasCap    uint64
	pending   *rpchelper.Pending
}

// NewTraceAPI returns NewTraceAPI instance
func NewTraceAPI(dbReader ethdb.Database, pending *rpchelper.Pending, cfg *cli.Flags) *TraceAPIImpl {
	return &TraceAPIImpl{
		BaseAPI:   &BaseAPI{},
		dbReader:  dbReader,
		maxTraces: cfg.MaxTraces,
		traceType: cfg.TraceType,
		gasCap:    cfg.Gascap,
		pending:   pending,
	}
}
