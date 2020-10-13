package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// PrivateDebugAPI
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error)
	TraceTransaction(ctx context.Context, hash common.Hash, config *eth.TraceConfig) (interface{}, error)
	AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start []byte, maxResults int, nocode, nostorage, incompletes bool) (state.IteratorDump, error)
	GetModifiedAccountsByNumber(ctx context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error)
	GetModifiedAccountsByHash(_ context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error)
}

// APIImpl is implementation of the PrivateDebugAPI interface based on remote Db access
type PrivateDebugAPIImpl struct {
	db           ethdb.KV
	dbReader     ethdb.Database
	chainContext core.ChainContext
}

// NewPrivateDebugAPI returns PrivateDebugAPIImpl instance
func NewPrivateDebugAPI(db ethdb.KV, dbReader ethdb.Database) *PrivateDebugAPIImpl {
	return &PrivateDebugAPIImpl{
		db:       db,
		dbReader: dbReader,
	}
}

// StorageRangeAt re-implementation of eth/api.go:StorageRangeAt
func (api *PrivateDebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer tx.Rollback()

	bc := adapter.NewBlockGetter(tx)
	cc := adapter.NewChainContext(tx)
	genesisHash := rawdb.ReadBlockByNumber(tx, 0).Hash()
	chainConfig := rawdb.ReadChainConfig(tx, genesisHash)
	_, _, _, stateReader, err := transactions.ComputeTxEnv(ctx, bc, chainConfig, cc, tx.(ethdb.HasTx).Tx(), blockHash, txIndex)
	if err != nil {
		return StorageRangeResult{}, err
	}
	return StorageRangeAt(stateReader, contractAddress, keyStart, maxResult)
}

// AccountRange re-implementation of eth/api.go:AccountRange
func (api *PrivateDebugAPIImpl) AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start []byte, maxResults int, nocode, nostorage, incompletes bool) (state.IteratorDump, error) {
	tx, err := api.db.Begin(ctx, nil, false)
	if err != nil {
		return state.IteratorDump{}, err
	}
	defer tx.Rollback()

	var blockNumber uint64

	if number, ok := blockNrOrHash.Number(); ok {
		if number == rpc.PendingBlockNumber {
			return state.IteratorDump{}, fmt.Errorf("accountRange for pending block not supported")
		}
		if number == rpc.LatestBlockNumber {
			var err error

			blockNumber, _, err = stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return state.IteratorDump{}, fmt.Errorf("last block has not found: %w", err)
			}
		} else {
			blockNumber = uint64(number)
		}

	} else if hash, ok := blockNrOrHash.Hash(); ok {
		block := rawdb.ReadBlockByHash(tx, hash)
		if block == nil {
			return state.IteratorDump{}, fmt.Errorf("block %s not found", hash.Hex())
		}
		blockNumber = block.NumberU64()
	}

	if maxResults > eth.AccountRangeMaxResults || maxResults <= 0 {
		maxResults = eth.AccountRangeMaxResults
	}

	dumper := state.NewDumper(tx, blockNumber)
	res, err := dumper.IteratorDump(nocode, nostorage, incompletes, start, maxResults)
	if err != nil {
		return state.IteratorDump{}, err
	}

	hash, err := rawdb.ReadCanonicalHash(tx, blockNumber)
	if err != nil {
		return state.IteratorDump{}, err
	}
	if hash != (common.Hash{}) {
		header := rawdb.ReadHeader(tx, hash, blockNumber)
		if header != nil {
			res.Root = header.Root.String()
		}
	}

	return res, nil
}

func (api *PrivateDebugAPIImpl) GetModifiedAccountsByNumber(_ context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error) {
	if endNum != nil && startNum.Int64() >= endNum.Int64() {
		return nil, fmt.Errorf("start block height (%d) must be less than end block height (%d)", startNum.Int64(), endNum.Int64())
	}

	final, _, err := stages.GetStageProgress(api.dbReader, stages.Finish)
	if err != nil {
		return nil, err
	}

	lastBlockNumber := final

	if startNum.Int64() < 1 || uint64(startNum.Int64()) > lastBlockNumber {
		return nil, fmt.Errorf("start block %x not found", uint64(startNum.Int64()))
	}

	if endNum == nil {
		*endNum = startNum
	} else {
		if endNum.Int64() < 1 || uint64(endNum.Int64()) > lastBlockNumber {
			return nil, fmt.Errorf("end block %x not found", uint64(endNum.Int64()))
		}
	}

	return getModifiedAccounts(api.dbReader, uint64(startNum.Int64()), uint64(endNum.Int64()))
}

func (api *PrivateDebugAPIImpl) GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	startBlock := rawdb.ReadBlockByHash(tx, startHash)
	if startBlock == nil {
		return nil, fmt.Errorf("start block %x not found", startHash)
	}

	var endBlock *types.Block
	if endHash == nil {
		endBlock = startBlock
	} else {
		endBlock = rawdb.ReadBlockByHash(tx, *endHash)
		if endBlock == nil {
			return nil, fmt.Errorf("end block %x not found", *endHash)
		}
	}
	return getModifiedAccounts(tx, startBlock.NumberU64(), endBlock.NumberU64())
}

func getModifiedAccounts(db ethdb.Getter, startNum, endNum uint64) ([]common.Address, error) {
	if startNum >= endNum {
		return nil, fmt.Errorf("start block height (%d) must be less than end block height (%d)", startNum, endNum)
	}

	return ethdb.GetModifiedAccounts(db, startNum, endNum)
}
