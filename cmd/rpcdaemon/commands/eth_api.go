package commands

import (
	"context"
	"errors"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	Coinbase(context.Context) (common.Address, error)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error)
	GetBalance(_ context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error)
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
	GetLogs(ctx context.Context, hash common.Hash) ([][]*types.Log, error)
	Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account) (hexutil.Bytes, error)
	EstimateGas(ctx context.Context, args ethapi.CallArgs) (hexutil.Uint64, error)
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	db           ethdb.KV
	txpool       ethdb.Backend
	dbReader     ethdb.Getter
	chainContext core.ChainContext
}

// NewAPI returns APIImpl instance
func NewAPI(db ethdb.KV, dbReader ethdb.Getter, chainContext core.ChainContext, txpool ethdb.Backend) *APIImpl {
	return &APIImpl{
		db:           db,
		dbReader:     dbReader,
		chainContext: chainContext,
		txpool:       txpool,
	}
}

func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	execution, _, err := stages.GetStageProgress(api.dbReader, stages.Execution)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(execution), nil
}

// Etherbase is the address that mining rewards will be send to
func (api *APIImpl) Coinbase(_ context.Context) (common.Address, error) {
	return common.Address{}, errors.New("not implemented")
}

type blockGetter struct {
	dbReader rawdb.DatabaseReader
}

func (g *blockGetter) GetBlockByHash(hash common.Hash) *types.Block {
	return rawdb.ReadBlockByHash(g.dbReader, hash)

}

func (g *blockGetter) GetBlock(hash common.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(g.dbReader, hash, number)
}
