package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// TraceAPI RPC interface into tracing API
type TraceAPI interface {
	// Ad-hoc
	// Call(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	// CallMany(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	// RawTransaction(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	// ReplayBlockTransactions(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	// ReplayTransaction(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)

	// Filtering
	Transaction(ctx context.Context, txHash common.Hash) ([]interface{}, error)
	Get(ctx context.Context, txHash common.Hash, txIndicies []hexutil.Uint64) (interface{}, error)
	Block(ctx context.Context, blockNr rpc.BlockNumber) ([]interface{}, error)
	Filter(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)

	// Custom (turbo geth exclusive)
	BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error)
	UncleReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error)
	Issuance(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error)
}

// TraceAPIImpl is implementation of the TraceAPI interface based on remote Db access
type TraceAPIImpl struct {
	db        ethdb.KV
	dbReader  ethdb.Getter
	maxTraces uint64
	traceType string
}

// NewTraceAPI returns NewTraceAPI instance
func NewTraceAPI(db ethdb.KV, dbReader ethdb.Getter, cfg *cli.Flags) *TraceAPIImpl {
	return &TraceAPIImpl{
		db:        db,
		dbReader:  dbReader,
		maxTraces: cfg.MaxTraces,
		traceType: cfg.TraceType,
	}
}

func (api *TraceAPIImpl) getBlockByRPCNumber(blockNr rpc.BlockNumber) (*types.Block, error) {
	blockNum, err := getBlockNumber(blockNr, api.dbReader)
	if err != nil {
		return nil, err
	}
	return rawdb.ReadBlockByNumber(api.dbReader, blockNum), nil
}
