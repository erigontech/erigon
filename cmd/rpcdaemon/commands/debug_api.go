package commands

import (
	"context"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

// AccountRangeMaxResults is the maximum number of results to be returned per call
const AccountRangeMaxResults = 256

// PrivateDebugAPI Exposed RPC endpoints for debugging use
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error)
	TraceTransaction(ctx context.Context, hash common.Hash, config *tracers.TraceConfig, stream *jsoniter.Stream) error
	TraceBlockByHash(ctx context.Context, hash common.Hash, config *tracers.TraceConfig, stream *jsoniter.Stream) error
	TraceBlockByNumber(ctx context.Context, number rpc.BlockNumber, config *tracers.TraceConfig, stream *jsoniter.Stream) error
	AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start []byte, maxResults int, nocode, nostorage bool) (state.IteratorDump, error)
	GetModifiedAccountsByNumber(ctx context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error)
	GetModifiedAccountsByHash(_ context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error)
	TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig, stream *jsoniter.Stream) error
	AccountAt(ctx context.Context, blockHash common.Hash, txIndex uint64, account common.Address) (*AccountResult, error)
}

// PrivateDebugAPIImpl is implementation of the PrivateDebugAPI interface based on remote Db access
type PrivateDebugAPIImpl struct {
	*BaseAPI
	db     kv.RoDB
	GasCap uint64
}

// NewPrivateDebugAPI returns PrivateDebugAPIImpl instance
func NewPrivateDebugAPI(base *BaseAPI, db kv.RoDB, gascap uint64) *PrivateDebugAPIImpl {
	return &PrivateDebugAPIImpl{
		BaseAPI: base,
		db:      db,
		GasCap:  gascap,
	}
}

// storageRangeAt implements debug_storageRangeAt. Returns information about a range of storage locations (if any) for the given address.
func (api *PrivateDebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return StorageRangeResult{}, err
	}
	engine := api.engine()

	if api.historyV3(tx) {
		number := rawdb.ReadHeaderNumber(tx, blockHash)
		minTxNum, err := rawdbv3.TxNums.Min(tx, *number)
		if err != nil {
			return StorageRangeResult{}, err
		}
		return storageRangeAtV3(tx.(kv.TemporalTx), contractAddress, keyStart, minTxNum+txIndex, maxResult)
	}

	block, err := api.blockByHashWithSenders(tx, blockHash)
	if err != nil {
		return StorageRangeResult{}, err
	}
	if block == nil {
		return StorageRangeResult{}, nil
	}

	_, _, _, _, stateReader, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, int(txIndex), api.historyV3(tx))
	if err != nil {
		return StorageRangeResult{}, err
	}
	return storageRangeAt(stateReader.(*state.PlainState), contractAddress, keyStart, maxResult)
}

// AccountRange implements debug_accountRange. Returns a range of accounts involved in the given block rangeb
func (api *PrivateDebugAPIImpl) AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, startKey []byte, maxResults int, excludeCode, excludeStorage bool) (state.IteratorDump, error) {
	tx, err := api.db.BeginRo(ctx)
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

			blockNumber, err = stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return state.IteratorDump{}, fmt.Errorf("last block has not found: %w", err)
			}
		} else {
			blockNumber = uint64(number)
		}

	} else if hash, ok := blockNrOrHash.Hash(); ok {
		block, err1 := api.blockByHashWithSenders(tx, hash)
		if err1 != nil {
			return state.IteratorDump{}, err1
		}
		if block == nil {
			return state.IteratorDump{}, fmt.Errorf("block %s not found", hash.Hex())
		}
		blockNumber = block.NumberU64()
	}

	if maxResults > AccountRangeMaxResults || maxResults <= 0 {
		maxResults = AccountRangeMaxResults
	}

	dumper := state.NewDumper(tx, blockNumber)
	res, err := dumper.IteratorDump(excludeCode, excludeStorage, common.BytesToAddress(startKey), maxResults)
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

// GetModifiedAccountsByNumber implements debug_getModifiedAccountsByNumber. Returns a list of accounts modified in the given block.
func (api *PrivateDebugAPIImpl) GetModifiedAccountsByNumber(ctx context.Context, startNumber rpc.BlockNumber, endNumber *rpc.BlockNumber) ([]common.Address, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	latestBlock, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return nil, err
	}

	// forces negative numbers to fail (too large) but allows zero
	startNum := uint64(startNumber.Int64())
	if startNum > latestBlock {
		return nil, fmt.Errorf("start block (%d) is later than the latest block (%d)", startNum, latestBlock)
	}

	endNum := startNum + 1 // allows for single param calls
	if endNumber != nil {
		// forces negative numbers to fail (too large) but allows zero
		endNum = uint64(endNumber.Int64()) + 1
	}

	// is endNum too big?
	if endNum > latestBlock {
		return nil, fmt.Errorf("end block (%d) is later than the latest block (%d)", endNum, latestBlock)
	}

	if startNum > endNum {
		return nil, fmt.Errorf("start block (%d) must be less than or equal to end block (%d)", startNum, endNum)
	}

	return changeset.GetModifiedAccounts(tx, startNum, endNum)
}

// GetModifiedAccountsByHash implements debug_getModifiedAccountsByHash. Returns a list of accounts modified in the given block.
func (api *PrivateDebugAPIImpl) GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	startBlock, err := api.blockByHashWithSenders(tx, startHash)
	if err != nil {
		return nil, err
	}
	if startBlock == nil {
		return nil, fmt.Errorf("start block %x not found", startHash)
	}
	startNum := startBlock.NumberU64()
	endNum := startNum + 1 // allows for single parameter calls

	if endHash != nil {
		endBlock, err := api.blockByHashWithSenders(tx, *endHash)
		if err != nil {
			return nil, err
		}
		if endBlock == nil {
			return nil, fmt.Errorf("end block %x not found", *endHash)
		}
		endNum = endBlock.NumberU64() + 1
	}

	if startNum > endNum {
		return nil, fmt.Errorf("start block (%d) must be less than or equal to end block (%d)", startNum, endNum)
	}

	return changeset.GetModifiedAccounts(tx, startNum, endNum)
}

func (api *PrivateDebugAPIImpl) AccountAt(ctx context.Context, blockHash common.Hash, txIndex uint64, address common.Address) (*AccountResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if api.historyV3(tx) {
		number := rawdb.ReadHeaderNumber(tx, blockHash)
		if number == nil {
			return nil, nil
		}
		canonicalHash, _ := rawdb.ReadCanonicalHash(tx, *number)
		isCanonical := canonicalHash == blockHash
		if !isCanonical {
			return nil, fmt.Errorf("block hash is not canonical")
		}

		minTxNum, err := rawdbv3.TxNums.Min(tx, *number)
		if err != nil {
			return nil, err
		}
		ttx := tx.(kv.TemporalTx)
		v, ok, err := ttx.DomainGet(temporal.AccountsDomain, address[:], nil, minTxNum+txIndex+1)
		if err != nil {
			return nil, err
		}
		if !ok || len(v) == 0 {
			return &AccountResult{}, nil
		}

		var a accounts.Account
		if err := a.DecodeForStorage(v); err != nil {
			return nil, err
		}
		result := &AccountResult{}
		result.Balance.ToInt().Set(a.Balance.ToBig())
		result.Nonce = hexutil.Uint64(a.Nonce)
		result.CodeHash = a.CodeHash

		code, _, err := ttx.DomainGet(temporal.CodeDomain, address[:], a.CodeHash[:], minTxNum+txIndex)
		if err != nil {
			return nil, err
		}
		result.Code = code
		return result, nil
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	block, err := api.blockByHashWithSenders(tx, blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	_, _, _, ibs, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, int(txIndex), api.historyV3(tx))
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
