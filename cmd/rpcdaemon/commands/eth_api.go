package commands

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	Coinbase(ctx context.Context) (common.Address, error)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error)
	GetBalance(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error)
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
	GetLogs(ctx context.Context, hash common.Hash) ([][]*types.Log, error)
	Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account) (hexutil.Bytes, error)
	EstimateGas(ctx context.Context, args ethapi.CallArgs) (hexutil.Uint64, error)
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	db           ethdb.KV
	ethBackend   ethdb.Backend
	dbReader     ethdb.Getter
	chainContext core.ChainContext
	GasCap       uint64
}

// NewAPI returns APIImpl instance
func NewAPI(db ethdb.KV, dbReader ethdb.Getter, eth ethdb.Backend, gascap uint64) *APIImpl {
	return &APIImpl{
		db:         db,
		dbReader:   dbReader,
		ethBackend: eth,
		GasCap:     gascap,
	}
}

func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	execution, _, err := stages.GetStageProgress(api.dbReader, stages.Execution)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(execution), nil
}
