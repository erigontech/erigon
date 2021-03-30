package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/eth/tracers"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// PrivateDebugAPI Exposed RPC endpoints for debugging use
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error)
	TraceTransaction(ctx context.Context, hash common.Hash, config *tracers.TraceConfig) (interface{}, error)
	AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start []byte, maxResults int, nocode, nostorage, incompletes bool) (state.IteratorDump, error)
	GetModifiedAccountsByNumber(ctx context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error)
	GetModifiedAccountsByHash(_ context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error)
	TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig) (interface{}, error)
	AccountAt(ctx context.Context, blockHash common.Hash, txIndex uint64, account common.Address) (*AccountResult, error)
}

// PrivateDebugAPIImpl is implementation of the PrivateDebugAPI interface based on remote Db access
type PrivateDebugAPIImpl struct {
	*BaseAPI
	dbReader ethdb.KV
	GasCap   uint64
	pending  *rpchelper.Pending
}

// NewPrivateDebugAPI returns PrivateDebugAPIImpl instance
func NewPrivateDebugAPI(dbReader ethdb.KV, gascap uint64, pending *rpchelper.Pending) *PrivateDebugAPIImpl {
	return &PrivateDebugAPIImpl{
		BaseAPI:  &BaseAPI{},
		dbReader: dbReader,
		GasCap:   gascap,
		pending:  pending,
	}
}

// StorageRangeAt implements debug_storageRangeAt. Returns information about a range of storage locations (if any) for the given address.
func (api *PrivateDebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer tx.Rollback()

	bc := adapter.NewBlockGetter(tx)
	cc := adapter.NewChainContext(ethdb.NewRoTxDb(tx))
	genesis, err := rawdb.ReadBlockByNumber(ethdb.NewRoTxDb(tx), 0)
	if err != nil {
		return StorageRangeResult{}, err
	}
	genesisHash := genesis.Hash()
	chainConfig, err := rawdb.ReadChainConfig(ethdb.NewRoTxDb(tx), genesisHash)
	if err != nil {
		return StorageRangeResult{}, err
	}
	_, _, _, _, stateReader, err := transactions.ComputeTxEnv(ctx, bc, chainConfig, cc, tx, blockHash, txIndex)
	if err != nil {
		return StorageRangeResult{}, err
	}
	return StorageRangeAt(stateReader, contractAddress, keyStart, maxResult)
}

// AccountRange implements debug_accountRange. Returns a range of accounts involved in the given block range
func (api *PrivateDebugAPIImpl) AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, startKey []byte, maxResults int, excludeCode, excludeStorage, excludeMissingPreimages bool) (state.IteratorDump, error) {
	tx, err := api.dbReader.Begin(ctx)
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

			blockNumber, err = stages.GetStageProgress(ethdb.NewRoTxDb(tx), stages.Execution)
			if err != nil {
				return state.IteratorDump{}, fmt.Errorf("last block has not found: %w", err)
			}
		} else {
			blockNumber = uint64(number)
		}

	} else if hash, ok := blockNrOrHash.Hash(); ok {
		block, err1 := rawdb.ReadBlockByHash(ethdb.NewRoTxDb(tx), hash)
		if err1 != nil {
			return state.IteratorDump{}, err1
		}
		if block == nil {
			return state.IteratorDump{}, fmt.Errorf("block %s not found", hash.Hex())
		}
		blockNumber = block.NumberU64()
	}

	if maxResults > eth.AccountRangeMaxResults || maxResults <= 0 {
		maxResults = eth.AccountRangeMaxResults
	}

	dumper := state.NewDumper(tx, blockNumber)
	res, err := dumper.IteratorDump(excludeCode, excludeStorage, excludeMissingPreimages, common.BytesToAddress(startKey), maxResults)
	if err != nil {
		return state.IteratorDump{}, err
	}

	hash, err := rawdb.ReadCanonicalHash(ethdb.NewRoTxDb(tx), blockNumber)
	if err != nil {
		return state.IteratorDump{}, err
	}
	if hash != (common.Hash{}) {
		header := rawdb.ReadHeader(ethdb.NewRoTxDb(tx), hash, blockNumber)
		if header != nil {
			res.Root = header.Root.String()
		}
	}

	return res, nil
}

// GetModifiedAccountsByNumber implements debug_getModifiedAccountsByNumber. Returns a list of accounts modified in the given block.
func (api *PrivateDebugAPIImpl) GetModifiedAccountsByNumber(ctx context.Context, startNumber rpc.BlockNumber, endNumber *rpc.BlockNumber) ([]common.Address, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	latestBlock, err := stages.GetStageProgress(ethdb.NewRoTxDb(tx), stages.Finish)
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

	return changeset.GetModifiedAccounts(ethdb.NewRoTxDb(tx), startNum, endNum)
}

// GetModifiedAccountsByHash implements debug_getModifiedAccountsByHash. Returns a list of accounts modified in the given block.
func (api *PrivateDebugAPIImpl) GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	startBlock, err := rawdb.ReadBlockByHash(ethdb.NewRoTxDb(tx), startHash)
	if err != nil {
		return nil, err
	}
	if startBlock == nil {
		return nil, fmt.Errorf("start block %x not found", startHash)
	}
	startNum := startBlock.NumberU64()
	endNum := startNum // allows for single parameter calls

	if endHash != nil {
		endBlock, err := rawdb.ReadBlockByHash(ethdb.NewRoTxDb(tx), *endHash)
		if err != nil {
			return nil, err
		}
		if endBlock == nil {
			return nil, fmt.Errorf("end block %x not found", *endHash)
		}
		endNum = endBlock.NumberU64()
	}

	if startNum > endNum {
		return nil, fmt.Errorf("start block (%d) must be less than or equal to end block (%d)", startNum, endNum)
	}

	return changeset.GetModifiedAccounts(ethdb.NewRoTxDb(tx), startNum, endNum)
}

func (api *PrivateDebugAPIImpl) AccountAt(ctx context.Context, blockHash common.Hash, txIndex uint64, address common.Address) (*AccountResult, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bc := adapter.NewBlockGetter(tx)
	cc := adapter.NewChainContext(ethdb.NewRoTxDb(tx))
	genesis, err := rawdb.ReadBlockByNumber(ethdb.NewRoTxDb(tx), 0)
	if err != nil {
		return nil, err
	}
	genesisHash := genesis.Hash()
	chainConfig, err := rawdb.ReadChainConfig(ethdb.NewRoTxDb(tx), genesisHash)
	if err != nil {
		return nil, err
	}
	_, _, _, ibs, _, err := transactions.ComputeTxEnv(ctx, bc, chainConfig, cc, tx, blockHash, txIndex)
	if err != nil {
		return nil, err
	}
	result := &AccountResult{}
	result.Balance.ToInt().Set(ibs.GetBalance(address).ToBig())
	result.Nonce = hexutil.Uint64(ibs.GetNonce(address))
	result.Code = ibs.GetCode(address)
	result.CodeHash = ibs.GetCodeHash(address)
	return result, nil
}

type AccountResult struct {
	Balance  hexutil.Big    `json:"balance"`
	Nonce    hexutil.Uint64 `json:"nonce"`
	Code     hexutil.Bytes  `json:"code"`
	CodeHash common.Hash    `json:"codeHash"`
}
