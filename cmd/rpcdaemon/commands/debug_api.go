package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// PrivateDebugAPI Exposed RPC endpoints for debugging use
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error)
	TraceTransaction(ctx context.Context, hash common.Hash, config *eth.TraceConfig) (interface{}, error)
	AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start []byte, maxResults int, nocode, nostorage, incompletes bool) (state.IteratorDump, error)
	GetModifiedAccountsByNumber(ctx context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error)
	GetModifiedAccountsByHash(_ context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error)
}

// PrivateDebugAPIImpl is implementation of the PrivateDebugAPI interface based on remote Db access
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
	genesis, err := rawdb.ReadBlockByNumber(tx, 0)
	if err != nil {
		return StorageRangeResult{}, err
	}
	genesisHash := genesis.Hash()
	chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return StorageRangeResult{}, err
	}
	_, _, _, stateReader, err := transactions.ComputeTxEnv(ctx, bc, chainConfig, cc, tx.(ethdb.HasTx).Tx(), blockHash, txIndex)
	if err != nil {
		return StorageRangeResult{}, err
	}
	return StorageRangeAt(stateReader, contractAddress, keyStart, maxResult)
}

// AccountRange enumerates all accounts in the given block and start point in paging request
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

// GetModifiedAccountsByNumber returns a list of accounts found in the change sets
// startNum - first block from which to include results
// endNum - if present, last block from which to include results (inclusive). If not present, startNum.
func (api *PrivateDebugAPIImpl) GetModifiedAccountsByNumber(ctx context.Context, startNumber rpc.BlockNumber, endNumber *rpc.BlockNumber) ([]common.Address, error) {
	tx, err := api.db.Begin(ctx, nil, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	latestBlock, _, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return nil, err
	}

	// forces negative numbers to fail (too large) but allows zero
	startNum := uint64(startNumber.Int64())
	if startNum > latestBlock {
		return nil, fmt.Errorf("start block (%d) is later than the latest block (%d)", startNum, latestBlock)
	}

	endNum := startNum // allows for single param calls
	if endNumber != nil {
		// forces negative numbers to fail (too large) but allows zero
		endNum = uint64(endNumber.Int64())
	}

	// is endNum too big?
	if endNum > latestBlock {
		return nil, fmt.Errorf("end block (%d) is later than the latest block (%d)", endNum, latestBlock)
	}

	if startNum > endNum {
		return nil, fmt.Errorf("start block (%d) must be less than or equal to end block (%d)", startNum, endNum)
	}

	return ethdb.GetModifiedAccounts(tx, startNum, endNum)
}

// GetModifiedAccountsByHash returns a list of accounts found in the change sets
// startHash - first block to include in results
// endHash - if present, last block to include in results (inclusive)
func (api *PrivateDebugAPIImpl) GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error) {
	tx, err := api.db.Begin(ctx, nil, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	startBlock := rawdb.ReadBlockByHash(tx, startHash)
	if startBlock == nil {
		return nil, fmt.Errorf("start block %x not found", startHash)
	}
	startNum := startBlock.NumberU64()
	endNum := startNum // allows for single parameter calls

	if endHash != nil {
		endBlock := rawdb.ReadBlockByHash(tx, *endHash)
		if endBlock == nil {
			return nil, fmt.Errorf("end block %x not found", *endHash)
		}
		endNum = endBlock.NumberU64()
	}

	if startNum > endNum {
		return nil, fmt.Errorf("start block (%d) must be less than or equal to end block (%d)", startNum, endNum)
	}

	return ethdb.GetModifiedAccounts(tx, startNum, endNum)
}
