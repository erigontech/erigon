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
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/trie"
	witnesstypes "github.com/erigontech/erigon/execution/commitment/witness"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
)

// AccountRangeMaxResults is the maximum number of results to be returned
const AccountRangeMaxResults = 8192

// AccountRangeMaxResultsWithStorage is the maximum number of results to be returned
// if storage is asked to be enclosed. Contract storage is usually huge and we should
// be careful not overwhelming our clients or being stuck in db.
const AccountRangeMaxResultsWithStorage = 256

// PrivateDebugAPI Exposed RPC endpoints for debugging use
type PrivateDebugAPI interface {
	StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error)
	TraceTransaction(ctx context.Context, hash common.Hash, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error
	TraceBlockByHash(ctx context.Context, hash common.Hash, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error
	TraceBlockByNumber(ctx context.Context, number rpc.BlockNumber, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error
	AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start any, maxResults int, nocode, nostorage bool, incompletes *bool) (state.IteratorDump, error)
	GetModifiedAccountsByNumber(ctx context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error)
	GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error)
	TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error
	AccountAt(ctx context.Context, blockHash common.Hash, txIndex uint64, account common.Address) (*AccountResult, error)
	GetRawHeader(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)
	GetRawBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)
	GetRawReceipts(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) ([]hexutil.Bytes, error)
	GetBadBlocks(ctx context.Context) ([]map[string]any, error)
	GetRawTransaction(ctx context.Context, hash common.Hash) (hexutil.Bytes, error)
	ExecutionWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ExecutionWitnessResult, error)
	FreeOSMemory()
	SetGCPercent(v int) int
	SetMemoryLimit(limit int64) int64
	GcStats() *debug.GCStats
	MemStats() *runtime.MemStats
}

// PrivateDebugAPIImpl is implementation of the PrivateDebugAPI interface based on remote Db access
type DebugAPIImpl struct {
	*BaseAPI
	db     kv.TemporalRoDB
	GasCap uint64
}

// NewPrivateDebugAPI returns PrivateDebugAPIImpl instance
func NewPrivateDebugAPI(base *BaseAPI, db kv.TemporalRoDB, gascap uint64) *DebugAPIImpl {
	return &DebugAPIImpl{
		BaseAPI: base,
		db:      db,
		GasCap:  gascap,
	}
}

// storageRangeAt implements debug_storageRangeAt. Returns information about a range of storage locations (if any) for the given address.
func (api *DebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer tx.Rollback()

	number, err := api._blockReader.HeaderNumber(ctx, tx, blockHash)
	if err != nil {
		return StorageRangeResult{}, err
	}
	if number == nil {
		return StorageRangeResult{}, nil
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, *number)
	if err != nil {
		return StorageRangeResult{}, err
	}

	minTxNum, err := api._txNumReader.Min(ctx, tx, *number)
	if err != nil {
		return StorageRangeResult{}, err
	}

	fromTxNum := minTxNum + txIndex + 1 //+1 for system txn in the beginning of block
	return storageRangeAt(tx, contractAddress, keyStart, fromTxNum, maxResult)
}

// AccountRange implements debug_accountRange. Returns a range of accounts involved in the given block rangeb
// To ensure compatibility, we've temporarily added support for the start parameter in two formats:
// - string (e.g., "0x..."), which is used by Geth and other APIs (i.e debug_storageRangeAt).
// - []byte, which was used in Erigon.
// Deprecation of []byte format: The []byte format is now deprecated and will be removed in a future release.
//
// New optional parameter incompletes: This parameter has been added for compatibility with Geth. It is currently not supported when set to true(as its functionality is specific to the Geth protocol).
func (api *DebugAPIImpl) AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start any, maxResults int, excludeCode, excludeStorage bool, optional_incompletes *bool) (state.IteratorDump, error) {
	var startBytes []byte

	switch v := start.(type) {
	case string:
		var err error
		startBytes, err = hexutil.Decode(v)
		if err != nil {
			return state.IteratorDump{}, fmt.Errorf("invalid hex string for start parameter: %v", err)
		}

	case []byte:
		startBytes = v

	case []any:
		for _, val := range v {
			if b, ok := val.(float64); ok {
				startBytes = append(startBytes, byte(b))
			} else {
				return state.IteratorDump{}, fmt.Errorf("invalid byte value in array: %T", val)
			}
		}
	default:
		return state.IteratorDump{}, fmt.Errorf("invalid type for start parameter: %T", v)
	}

	var incompletes bool

	if optional_incompletes == nil {
		incompletes = false
	} else {
		incompletes = *optional_incompletes
	}

	if incompletes {
		return state.IteratorDump{}, fmt.Errorf("not supported incompletes = true")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return state.IteratorDump{}, err
	}
	defer tx.Rollback()

	var blockNumber uint64

	if number, ok := blockNrOrHash.Number(); ok {
		if number == rpc.PendingBlockNumber {
			return state.IteratorDump{}, errors.New("accountRange for pending block not supported")
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
		header, err1 := api.headerByHash(ctx, hash, tx)
		if err1 != nil {
			return state.IteratorDump{}, err1
		}
		if header == nil {
			return state.IteratorDump{}, fmt.Errorf("header %s not found", hash.Hex())
		}
		blockNumber = header.Number.Uint64()
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber)
	if err != nil {
		return state.IteratorDump{}, err
	}

	// Determine how many results we will dump
	if excludeStorage {
		// Plain addresses
		if maxResults > AccountRangeMaxResults || maxResults <= 0 {
			maxResults = AccountRangeMaxResults
		}
	} else {
		// With storage
		if maxResults > AccountRangeMaxResultsWithStorage || maxResults <= 0 {
			maxResults = AccountRangeMaxResultsWithStorage
		}
	}

	dumper := state.NewDumper(tx, api._blockReader.TxnumReader(), blockNumber)
	res, err := dumper.IteratorDump(excludeCode, excludeStorage, common.BytesToAddress(startBytes), maxResults)
	if err != nil {
		return state.IteratorDump{}, err
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNumber)
	if err != nil {
		return state.IteratorDump{}, err
	}
	if header != nil {
		res.Root = header.Root.String()
	}

	return res, nil
}

// GetModifiedAccountsByNumber implements debug_getModifiedAccountsByNumber. Returns a list of accounts modified in the given block.
// [from, to)
func (api *DebugAPIImpl) GetModifiedAccountsByNumber(ctx context.Context, startNumber rpc.BlockNumber, endNumber *rpc.BlockNumber) ([]common.Address, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	latestBlock, err := stages.GetStageProgress(tx, stages.Execution)
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
		endNum = uint64(endNumber.Int64()) // [startNum,endNum) from user
	}

	// is endNum too big?
	if endNum > latestBlock+1 { // [startNum,endNum)
		return nil, fmt.Errorf("end block (%d) is later than the latest block (%d)", endNum, latestBlock)
	}

	if startNum >= endNum {
		return nil, fmt.Errorf("start block (%d) must be less than end block (%d)", startNum, endNum)
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, startNum)
	if err != nil {
		return nil, err
	}

	//[from, to)
	startTxNum, err := api._txNumReader.Min(ctx, tx, startNum)
	if err != nil {
		return nil, err
	}
	endTxNum, err := api._txNumReader.Min(ctx, tx, endNum)
	if err != nil {
		return nil, err
	}
	return getModifiedAccounts(tx, startTxNum, endTxNum-1)
}

// getModifiedAccounts returns a list of addresses that were modified in the block range
// [startNum:endNum)
func getModifiedAccounts(tx kv.TemporalTx, startTxNum, endTxNum uint64) ([]common.Address, error) {
	it, err := tx.HistoryRange(kv.AccountsDomain, int(startTxNum), int(endTxNum), order.Asc, kv.Unlim)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var result []common.Address
	saw := make(map[common.Address]struct{})
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		//TODO: data is sorted, enough to compare with prevKey
		if _, ok := saw[common.BytesToAddress(k)]; !ok {
			saw[common.BytesToAddress(k)] = struct{}{}
			result = append(result, common.BytesToAddress(k))
		}
	}
	return result, nil
}

// GetModifiedAccountsByHash implements debug_getModifiedAccountsByHash. Returns a list of accounts modified in the given block.
func (api *DebugAPIImpl) GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	startNum, err := api.headerNumberByHash(ctx, tx, startHash)
	if err != nil {
		return nil, fmt.Errorf("start block %x not found", startHash)
	}
	endNum := startNum + 1 // allows for single parameter calls

	if endHash != nil {
		var err error
		endNum, err = api.headerNumberByHash(ctx, tx, *endHash) // [startNum,endNum) from user
		if err != nil {
			return nil, fmt.Errorf("end block %x not found", *endHash)
		}
	}

	if startNum >= endNum {
		return nil, fmt.Errorf("start block (%d) must be less than end block (%d)", startNum, endNum)
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, startNum)
	if err != nil {
		return nil, err
	}

	//[from, to)
	startTxNum, err := api._txNumReader.Min(ctx, tx, startNum)
	if err != nil {
		return nil, err
	}
	endTxNum, err := api._txNumReader.Min(ctx, tx, endNum)
	if err != nil {
		return nil, err
	}
	return getModifiedAccounts(tx, startTxNum, endTxNum-1)
}

func (api *DebugAPIImpl) AccountAt(ctx context.Context, blockHash common.Hash, txIndex uint64, address common.Address) (*AccountResult, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header, err := api.headerByHash(ctx, blockHash, tx)
	if err != nil {
		return &AccountResult{}, err
	}
	if header == nil || header.Number == nil {
		return nil, nil // not error, see https://github.com/erigontech/erigon/issues/1645
	}
	canonicalHash, ok, err := api._blockReader.CanonicalHash(ctx, tx, header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("canonical hash not found %d", header.Number.Uint64())
	}
	isCanonical := canonicalHash == blockHash
	if !isCanonical {
		return nil, errors.New("block hash is not canonical")
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, header.Number.Uint64())
	if err != nil {
		return nil, err
	}

	minTxNum, err := api._txNumReader.Min(ctx, tx, header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	ttx := tx
	v, ok, err := ttx.GetAsOf(kv.AccountsDomain, address[:], minTxNum+txIndex+1)
	if err != nil {
		return nil, err
	}
	if !ok || len(v) == 0 {
		return &AccountResult{}, nil
	}

	var a accounts.Account
	if err := accounts.DeserialiseV3(&a, v); err != nil {
		return nil, err
	}
	result := &AccountResult{}
	result.Balance.ToInt().Set(a.Balance.ToBig())
	result.Nonce = hexutil.Uint64(a.Nonce)
	result.CodeHash = a.CodeHash.Value()

	code, _, err := ttx.GetAsOf(kv.CodeDomain, address[:], minTxNum+txIndex)
	if err != nil {
		return nil, err
	}
	result.Code = code
	return result, nil
}

type AccountResult struct {
	Balance  hexutil.Big    `json:"balance"`
	Nonce    hexutil.Uint64 `json:"nonce"`
	Code     hexutil.Bytes  `json:"code"`
	CodeHash common.Hash    `json:"codeHash"`
}

// GetRawHeader implements debug_getRawHeader - returns a an RLP-encoded header, given a block number or hash
func (api *DebugAPIImpl) GetRawHeader(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	n, h, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil // waiting for spec: not error, see Geth and https://github.com/erigontech/erigon/issues/1645
		}
		return nil, err
	}
	header, err := api._blockReader.Header(ctx, tx, h, n)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	return rlp.EncodeToBytes(header)
}

// Implements debug_getRawBlock - Returns an RLP-encoded block
func (api *DebugAPIImpl) GetRawBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	n, h, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil // waiting for spec: not error, see Geth and https://github.com/erigontech/erigon/issues/1645
		}
		return nil, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, n)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, h, n)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return rlp.EncodeToBytes(block)
}

// GetRawReceipts implements debug_getRawReceipts - retrieves and returns an array of EIP-2718 binary-encoded receipts of a single block
func (api *DebugAPIImpl) GetRawReceipts(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) ([]hexutil.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, blockHash, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil // waiting for spec: not error, see Geth and https://github.com/erigontech/erigon/issues/1645
		}
		return nil, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	receipts, err := api.getReceipts(ctx, tx, block)
	if err != nil {
		return nil, err
	}
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if chainConfig.Bor != nil {
		events, err := api.bridgeReader.Events(ctx, block.Hash(), blockNum)
		if err != nil {
			return nil, err
		}
		if len(events) != 0 {
			borReceipt, err := api.borReceiptGenerator.GenerateBorReceipt(ctx, tx, block, events, chainConfig)
			if err != nil {
				return nil, err
			}
			receipts = append(receipts, borReceipt)
		}
	}

	result := make([]hexutil.Bytes, len(receipts))
	for i, receipt := range receipts {
		b, err := receipt.MarshalBinary()
		if err != nil {
			return nil, err
		}
		result[i] = b
	}
	return result, nil
}

// GetBadBlocks implements debug_getBadBlocks - Returns an array of recent bad blocks that the client has seen on the network
func (api *DebugAPIImpl) GetBadBlocks(ctx context.Context) ([]map[string]any, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blocks, err := rawdb.GetLatestBadBlocks(tx)
	if err != nil || len(blocks) == 0 {
		// Return empty array if no bad blocks found to align with other clients and spec
		return []map[string]any{}, err
	}

	results := make([]map[string]any, 0, len(blocks))
	for _, block := range blocks {
		var blockRlp string
		if rlpBytes, err := rlp.EncodeToBytes(block); err != nil {
			blockRlp = err.Error() // hack
		} else {
			blockRlp = fmt.Sprintf("%#x", rlpBytes)
		}

		blockJson, err := ethapi.RPCMarshalBlock(block, true, true, nil)
		if err != nil {
			log.Error("Failed to marshal block", "err", err)
			blockJson = map[string]any{}
		}
		results = append(results, map[string]any{
			"hash":  block.Hash(),
			"block": blockRlp,
			"rlp":   blockJson,
		})
	}

	return results, nil
}

// GetRawTransaction implements debug_getRawTransaction - Returns an array of EIP-2718 binary-encoded transactions
func (api *DebugAPIImpl) GetRawTransaction(ctx context.Context, txnHash common.Hash) (hexutil.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	blockNum, txNum, ok, err := api.txnLookup(ctx, tx, txnHash)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	// Private API returns 0 if transaction is not found.
	isBorStateSyncTx := blockNum == 0 && chainConfig.Bor != nil
	if isBorStateSyncTx {
		blockNum, ok, err = api.bridgeReader.EventTxnLookup(ctx, txnHash)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
	}

	txNumMin, err := api._txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	if txNumMin+1 > txNum && !isBorStateSyncTx {
		return nil, fmt.Errorf("uint underflow txnums error txNum: %d, txNumMin: %d, blockNum: %d", txNum, txNumMin, blockNum)
	}

	var txnIndex = txNum - txNumMin - 1

	txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, int(txnIndex))
	if err != nil {
		return nil, err
	}

	if txn != nil {
		var buf bytes.Buffer
		err = txn.MarshalBinary(&buf)
		return buf.Bytes(), err
	}

	return nil, nil
}

// RecordingState combines a StateReader and StateWriter with an in-memory overlay.
// Reads check the overlay first (accounting for deletes and modifications), then
// fall back to the inner reader. Writes go to the overlay. All accesses and
// modifications are recorded for witness generation.
type RecordingState struct {
	inner  state.StateReader
	trace  bool
	prefix string

	// Read tracking (all accessed keys, including reads that hit the overlay)
	AccessedAccounts map[common.Address]struct{}
	AccessedStorage  map[common.Address]map[common.Hash]struct{}
	AccessedCode     map[common.Address][]byte

	// In-memory state overlay (writes)
	accountOverlay map[common.Address]*accounts.Account // non-nil = updated, entry present with nil value = deleted
	storageOverlay map[common.Address]map[common.Hash]uint256.Int
	codeOverlay    map[common.Address][]byte

	// Write tracking
	ModifiedAccounts map[common.Address]struct{}
	ModifiedStorage  map[common.Address]map[common.Hash]struct{}
	ModifiedCode     map[common.Address][]byte
	DeletedAccounts  map[common.Address]struct{}
	CreatedContracts map[common.Address]struct{}

	// Debug: addresses to trace operations on
	accountsToTrace map[common.Address]struct{}
}

// NewRecordingState creates a new RecordingState wrapping the given inner reader.
func NewRecordingState(inner state.StateReader) *RecordingState {
	return &RecordingState{
		inner:            inner,
		AccessedAccounts: make(map[common.Address]struct{}),
		AccessedStorage:  make(map[common.Address]map[common.Hash]struct{}),
		AccessedCode:     make(map[common.Address][]byte),
		accountOverlay:   make(map[common.Address]*accounts.Account),
		storageOverlay:   make(map[common.Address]map[common.Hash]uint256.Int),
		codeOverlay:      make(map[common.Address][]byte),
		ModifiedAccounts: make(map[common.Address]struct{}),
		ModifiedStorage:  make(map[common.Address]map[common.Hash]struct{}),
		ModifiedCode:     make(map[common.Address][]byte),
		DeletedAccounts:  make(map[common.Address]struct{}),
		CreatedContracts: make(map[common.Address]struct{}),
	}
}

func (s *RecordingState) SetAccountsToTrace(addrs []common.Address) {
	s.accountsToTrace = make(map[common.Address]struct{}, len(addrs))
	for _, a := range addrs {
		s.accountsToTrace[a] = struct{}{}
	}
}

func (s *RecordingState) tracing(addr common.Address) bool {
	if s.accountsToTrace == nil {
		return false
	}
	_, ok := s.accountsToTrace[addr]
	return ok
}

// --- StateReader implementation ---

func (s *RecordingState) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	// Check overlay: deleted accounts return nil
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountData %s -> deleted\n", addr.Hex())
		}
		return nil, nil
	}
	if acc, ok := s.accountOverlay[addr]; ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountData %s -> overlay nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		}
		return acc, nil
	}
	acc, err := s.inner.ReadAccountData(address)
	if s.tracing(addr) {
		if acc != nil {
			fmt.Printf("[TRACE] ReadAccountData %s -> inner nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		} else {
			fmt.Printf("[TRACE] ReadAccountData %s -> inner nil (err=%v)\n", addr.Hex(), err)
		}
	}
	return acc, err
}

func (s *RecordingState) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountDataForDebug %s -> deleted\n", addr.Hex())
		}
		return nil, nil
	}
	if acc, ok := s.accountOverlay[addr]; ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountDataForDebug %s -> overlay nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		}
		return acc, nil
	}
	acc, err := s.inner.ReadAccountDataForDebug(address)
	if s.tracing(addr) {
		if acc != nil {
			fmt.Printf("[TRACE] ReadAccountDataForDebug %s -> inner nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		} else {
			fmt.Printf("[TRACE] ReadAccountDataForDebug %s -> inner nil (err=%v)\n", addr.Hex(), err)
		}
	}
	return acc, err
}

func (s *RecordingState) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	if s.AccessedStorage[addr] == nil {
		s.AccessedStorage[addr] = make(map[common.Hash]struct{})
	}
	s.AccessedStorage[addr][key.Value()] = struct{}{}
	// Deleted accounts have no storage
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountStorage %s key=%s -> deleted\n", addr.Hex(), key.Value().Hex())
		}
		return uint256.Int{}, false, nil
	}
	// Check if this storage slot has been written in the overlay
	if mods, ok := s.ModifiedStorage[addr]; ok {
		if _, modified := mods[key.Value()]; modified {
			val := s.storageOverlay[addr][key.Value()]
			if s.tracing(addr) {
				fmt.Printf("[TRACE] ReadAccountStorage %s key=%s -> overlay val=%d\n", addr.Hex(), key.Value().Hex(), &val)
			}
			return val, !val.IsZero(), nil
		}
	}
	val, ok, err := s.inner.ReadAccountStorage(address, key)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] ReadAccountStorage %s key=%s -> inner val=%d ok=%v err=%v\n", addr.Hex(), key.Value().Hex(), &val, ok, err)
	}
	return val, ok, err
}

func (s *RecordingState) HasStorage(address accounts.Address) (bool, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	// Check overlay for any non-zero storage
	if mods, ok := s.storageOverlay[addr]; ok {
		for _, val := range mods {
			if !val.IsZero() {
				if s.tracing(addr) {
					fmt.Printf("[TRACE] HasStorage %s -> overlay true\n", addr.Hex())
				}
				return true, nil
			}
		}
	}
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] HasStorage %s -> deleted false\n", addr.Hex())
		}
		return false, nil
	}
	has, err := s.inner.HasStorage(address)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] HasStorage %s -> inner %v (err=%v)\n", addr.Hex(), has, err)
	}
	return has, err
}

func (s *RecordingState) ReadAccountCode(address accounts.Address) ([]byte, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountCode %s -> deleted\n", addr.Hex())
		}
		return nil, nil
	}
	if code, ok := s.codeOverlay[addr]; ok {
		if len(code) > 0 {
			s.AccessedCode[addr] = code
		}
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountCode %s -> overlay len=%d\n", addr.Hex(), len(code))
		}
		return code, nil
	}
	code, err := s.inner.ReadAccountCode(address)
	if err != nil {
		return nil, err
	}
	if len(code) > 0 {
		s.AccessedCode[addr] = code
	}
	if s.tracing(addr) {
		fmt.Printf("[TRACE] ReadAccountCode %s -> inner len=%d\n", addr.Hex(), len(code))
	}
	return code, nil
}

func (s *RecordingState) ReadAccountCodeSize(address accounts.Address) (int, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountCodeSize %s -> deleted 0\n", addr.Hex())
		}
		return 0, nil
	}
	if code, ok := s.codeOverlay[addr]; ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountCodeSize %s -> overlay %d\n", addr.Hex(), len(code))
		}
		return len(code), nil
	}
	_, err := s.ReadAccountCode(address) // need to read code here because witness has no way of knowing code size without reading the code first
	if err != nil {
		return 0, err
	}
	size, err := s.inner.ReadAccountCodeSize(address)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] ReadAccountCodeSize %s -> inner %d (err=%v)\n", addr.Hex(), size, err)
	}
	return size, err
}

func (s *RecordingState) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	inc, err := s.inner.ReadAccountIncarnation(address)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] ReadAccountIncarnation %s -> %d (err=%v)\n", addr.Hex(), inc, err)
	}
	return inc, err
}

func (s *RecordingState) SetTrace(trace bool, tracePrefix string) {
	s.trace = trace
	s.prefix = tracePrefix
}

func (s *RecordingState) Trace() bool {
	return s.trace
}

func (s *RecordingState) TracePrefix() string {
	return s.prefix
}

// --- StateWriter implementation ---

func (s *RecordingState) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	// Store a copy in the overlay
	acctCopy := *account
	s.accountOverlay[addr] = &acctCopy
	delete(s.DeletedAccounts, addr)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] UpdateAccountData %s nonce=%d balance=%d codeHash=%x\n", addr.Hex(), account.Nonce, &account.Balance, account.CodeHash)
	}
	return nil
}

func (s *RecordingState) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	s.codeOverlay[addr] = common.Copy(code)
	s.ModifiedCode[addr] = common.Copy(code)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] UpdateAccountCode %s codeHash=%x len=%d\n", addr.Hex(), codeHash, len(code))
	}
	return nil
}

func (s *RecordingState) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	s.DeletedAccounts[addr] = struct{}{}
	delete(s.accountOverlay, addr)
	// Clear storage overlay for this account
	delete(s.storageOverlay, addr)
	delete(s.codeOverlay, addr)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] DeleteAccount %s\n", addr.Hex())
	}
	return nil
}

func (s *RecordingState) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	if s.ModifiedStorage[addr] == nil {
		s.ModifiedStorage[addr] = make(map[common.Hash]struct{})
	}
	s.ModifiedStorage[addr][key.Value()] = struct{}{}
	// Store in overlay
	if s.storageOverlay[addr] == nil {
		s.storageOverlay[addr] = make(map[common.Hash]uint256.Int)
	}
	s.storageOverlay[addr][key.Value()] = value
	if s.tracing(addr) {
		fmt.Printf("[TRACE] WriteAccountStorage %s key=%s val=%d\n", addr.Hex(), key.Value().Hex(), &value)
	}
	return nil
}

func (s *RecordingState) CreateContract(address accounts.Address) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	s.CreatedContracts[addr] = struct{}{}
	delete(s.DeletedAccounts, addr)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] CreateContract %s\n", addr.Hex())
	}
	return nil
}

// --- Query methods ---

// GetAccessedKeys returns all accessed account addresses and storage keys (reads + writes)
func (s *RecordingState) GetAccessedKeys() ([]common.Address, map[common.Address][]common.Hash) {
	addresses := make([]common.Address, 0, len(s.AccessedAccounts))
	for addr := range s.AccessedAccounts {
		addresses = append(addresses, addr)
	}

	storageKeys := make(map[common.Address][]common.Hash)
	for addr, keys := range s.AccessedStorage {
		keySlice := make([]common.Hash, 0, len(keys))
		for key := range keys {
			keySlice = append(keySlice, key)
		}
		storageKeys[addr] = keySlice
	}

	return addresses, storageKeys
}

// GetModifiedKeys returns all modified account addresses and storage keys
func (s *RecordingState) GetModifiedKeys() ([]common.Address, map[common.Address][]common.Hash) {
	addresses := make([]common.Address, 0, len(s.ModifiedAccounts))
	for addr := range s.ModifiedAccounts {
		addresses = append(addresses, addr)
	}

	storageKeys := make(map[common.Address][]common.Hash)
	for addr, keys := range s.ModifiedStorage {
		keySlice := make([]common.Hash, 0, len(keys))
		for key := range keys {
			keySlice = append(keySlice, key)
		}
		storageKeys[addr] = keySlice
	}

	return addresses, storageKeys
}

// GetAccessedCode returns all accessed contract code
func (s *RecordingState) GetAccessedCode() map[common.Address][]byte {
	result := make(map[common.Address][]byte, len(s.AccessedCode))
	for addr, code := range s.AccessedCode {
		result[addr] = common.Copy(code)
	}
	return result
}

// GetModifiedCode returns all modified contract code
func (s *RecordingState) GetModifiedCode() map[common.Address][]byte {
	result := make(map[common.Address][]byte, len(s.ModifiedCode))
	for addr, code := range s.ModifiedCode {
		result[addr] = common.Copy(code)
	}
	return result
}

// ExecutionWitnessResult is the response format for debug_executionWitness
// Compatible with Geth/Reth format
type ExecutionWitnessResult struct {
	// State contains the list of RLP-encoded trie nodes in the witness trie
	State []hexutil.Bytes `json:"state"`
	// Codes is the list of accessed/created bytecodes during block execution
	Codes []hexutil.Bytes `json:"codes"`
	// Keys is the list of account and storage keys accessed/created during execution
	Keys []hexutil.Bytes `json:"keys"`
	// Headers is a list of RLP-encoded block headers needed for BLOCKHASH opcode support
	Headers []hexutil.Bytes `json:"headers,omitempty"`
}

// debugCompareRecordedVsGroundTruth compares account and storage writes recorded
// by the RecordingState against the ground truth from HistoryRange (which only
// contains writes). Prints any discrepancies.
func debugCompareRecordedVsGroundTruth(
	tx kv.TemporalTx,
	recordingState *RecordingState,
	firstTxNumInBlock, lastTxNumInBlock uint64,
) error {
	// Collect ground truth account writes from history
	gtAccounts := make(map[common.Address]struct{})
	accStream, err := tx.HistoryRange(kv.AccountsDomain, int(firstTxNumInBlock), int(lastTxNumInBlock+1), order.Asc, -1)
	if err != nil {
		return fmt.Errorf("ground truth account stream: %w", err)
	}
	for accStream.HasNext() {
		k, _, err := accStream.Next()
		if err != nil {
			accStream.Close()
			return err
		}
		var addr common.Address
		copy(addr[:], k)
		gtAccounts[addr] = struct{}{}
	}
	accStream.Close()

	// Collect ground truth storage writes from history
	gtStorage := make(map[common.Address]map[common.Hash]struct{})
	storStream, err := tx.HistoryRange(kv.StorageDomain, int(firstTxNumInBlock), int(lastTxNumInBlock+1), order.Asc, -1)
	if err != nil {
		return fmt.Errorf("ground truth storage stream: %w", err)
	}
	for storStream.HasNext() {
		k, _, err := storStream.Next()
		if err != nil {
			storStream.Close()
			return err
		}
		addr := common.BytesToAddress(k[:20])
		var key common.Hash
		copy(key[:], k[20:])
		if gtStorage[addr] == nil {
			gtStorage[addr] = make(map[common.Hash]struct{})
		}
		gtStorage[addr][key] = struct{}{}
	}
	storStream.Close()

	// Build our write sets from the RecordingState
	// Account writes = ModifiedAccounts + DeletedAccounts
	ourAccounts := make(map[common.Address]struct{})
	for addr := range recordingState.ModifiedAccounts {
		ourAccounts[addr] = struct{}{}
	}
	for addr := range recordingState.DeletedAccounts {
		ourAccounts[addr] = struct{}{}
	}

	// Storage writes = ModifiedStorage
	ourStorage := recordingState.ModifiedStorage

	// Count totals
	gtStorageTotal := 0
	for _, keys := range gtStorage {
		gtStorageTotal += len(keys)
	}
	ourStorageTotal := 0
	for _, keys := range ourStorage {
		ourStorageTotal += len(keys)
	}

	fmt.Printf("[debug] Ground truth writes: %d accounts, %d storage | Recorded writes: %d accounts, %d storage\n",
		len(gtAccounts), gtStorageTotal, len(ourAccounts), ourStorageTotal)

	// Find accounts in ground truth but missing from our writes
	missingAccounts := 0
	for addr := range gtAccounts {
		if _, ok := ourAccounts[addr]; !ok {
			missingAccounts++
			fmt.Printf("[debug] MISSING ACCOUNT WRITE: %s (in ground truth but not in recorded writes)\n", addr.Hex())
		}
	}

	// Find accounts in our writes but not in ground truth
	extraAccounts := 0
	for addr := range ourAccounts {
		if _, ok := gtAccounts[addr]; !ok {
			extraAccounts++
			fmt.Printf("[debug] EXTRA ACCOUNT WRITE: %s (in recorded writes but not in ground truth)\n", addr.Hex())
		}
	}

	// Find storage keys in ground truth but missing from our writes
	missingStorage := 0
	for addr, gtKeys := range gtStorage {
		ourKeys := ourStorage[addr]
		for key := range gtKeys {
			if ourKeys == nil {
				missingStorage++
				fmt.Printf("[debug] MISSING STORAGE WRITE: account=%s key=%s (account has no recorded storage writes)\n", addr.Hex(), key.Hex())
			} else if _, ok := ourKeys[key]; !ok {
				missingStorage++
				fmt.Printf("[debug] MISSING STORAGE WRITE: account=%s key=%s\n", addr.Hex(), key.Hex())
			}
		}
	}

	// Find storage keys in our writes but not in ground truth
	extraStorage := 0
	for addr, ourKeys := range ourStorage {
		gtKeys := gtStorage[addr]
		for key := range ourKeys {
			if gtKeys == nil {
				extraStorage++
				fmt.Printf("[debug] EXTRA STORAGE WRITE: account=%s key=%s (account has no ground truth storage writes)\n", addr.Hex(), key.Hex())
			} else if _, ok := gtKeys[key]; !ok {
				extraStorage++
				fmt.Printf("[debug] EXTRA STORAGE WRITE: account=%s key=%s\n", addr.Hex(), key.Hex())
			}
		}
	}

	fmt.Printf("[debug] Account writes: %d missing, %d extra | Storage writes: %d missing, %d extra\n",
		missingAccounts, extraAccounts, missingStorage, extraStorage)

	// For missing accounts, check if they were accessed (read) but not written
	for addr := range gtAccounts {
		if _, ok := ourAccounts[addr]; !ok {
			if _, accessed := recordingState.AccessedAccounts[addr]; accessed {
				fmt.Printf("[debug]   -> MISSING ACCOUNT WRITE %s was READ but not written\n", addr.Hex())
			} else {
				fmt.Printf("[debug]   -> MISSING ACCOUNT WRITE %s was NEVER TOUCHED\n", addr.Hex())
			}
		}
	}

	// For missing storage keys, check if the account was deleted or if the key was read
	for addr, gtKeys := range gtStorage {
		ourKeys := ourStorage[addr]
		for key := range gtKeys {
			isMissing := ourKeys == nil
			if !isMissing {
				_, found := ourKeys[key]
				isMissing = !found
			}
			if isMissing {
				if _, deleted := recordingState.DeletedAccounts[addr]; deleted {
					fmt.Printf("[debug]   -> MISSING STORAGE WRITE %s/%s belongs to DELETED account\n", addr.Hex(), key.Hex())
				} else if accessedKeys, ok := recordingState.AccessedStorage[addr]; ok {
					if _, accessed := accessedKeys[key]; accessed {
						fmt.Printf("[debug]   -> MISSING STORAGE WRITE %s/%s was READ but not written\n", addr.Hex(), key.Hex())
					} else {
						fmt.Printf("[debug]   -> MISSING STORAGE WRITE %s/%s was NEVER TOUCHED\n", addr.Hex(), key.Hex())
					}
				} else {
					fmt.Printf("[debug]   -> MISSING STORAGE WRITE %s/%s account NEVER TOUCHED\n", addr.Hex(), key.Hex())
				}
			}
		}
	}

	return nil
}

func touchHistoricalKeys(sd *execctx.SharedDomains, tx kv.TemporalTx, d kv.Domain, fromTxNum uint64, toTxNum uint64, visitor func(k []byte)) (uint64, error) {
	// toTxNum is exclusive per kv.TemporalTx.HistoryRange contract [from,to)
	stream, err := tx.HistoryRange(d, int(fromTxNum), int(toTxNum), order.Asc, -1)
	if err != nil {
		return 0, err
	}
	defer stream.Close()
	var touches uint64
	for stream.HasNext() {
		k, _, err := stream.Next()
		if err != nil {
			return 0, err
		}
		if visitor != nil {
			visitor(k)
		}
		sd.GetCommitmentCtx().TouchKey(d, string(k), nil)
		touches++
	}
	return touches, nil
}

func touchGroundTruthHistoricalKeys(domains *execctx.SharedDomains, tx kv.TemporalTx, fromTxNum, toTxNum uint64) error {
	accTouches, err := touchHistoricalKeys(domains, tx, kv.AccountsDomain, fromTxNum, toTxNum, nil)
	if err != nil {
		return err
	}
	storageTouches, err := touchHistoricalKeys(domains, tx, kv.StorageDomain, fromTxNum, toTxNum, nil)
	if err != nil {
		return err
	}
	codeTouches, err := touchHistoricalKeys(domains, tx, kv.CodeDomain, fromTxNum, toTxNum, nil)
	if err != nil {
		return err
	}
	log.Info("commitment touched keys", "accTouches", accTouches, "storageTouches", storageTouches, "codeTouches", codeTouches)
	return nil
}

// ExecutionWitness implements debug_executionWitness.
// It executes a block using a historical state reader, records all state accesses
// (accounts, storage, code), and builds merkle proofs for the accessed keys.
// This is compatible with the Geth/Reth format for execution witnesses.
func (api *DebugAPIImpl) ExecutionWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ExecutionWitnessResult, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	engine := api.engine()

	// Get first txnum of blockNum — this is the exact txnum of the parent block's
	// final state (before any system txns in this block have been applied).
	firstTxNumInBlock, err := api._txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	// last txnum in block used for commitment calculation and collapsed paths tracing
	lastTxNumInBlock, err := api._txNumReader.Max(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	// Create a state reader at the parent block state using the exact txnum
	var stateReader state.StateReader
	var parentNum uint64
	if blockNum == 0 {
		// For genesis block, use empty state as parent since there's no block before it
		// The genesis allocations are what get accessed during block 0 execution
		stateReader = state.NewNoopReader()
		parentNum = 0
	} else {
		parentNum = blockNum - 1
		stateReader = state.NewHistoryReaderV3(tx, firstTxNumInBlock)
	}

	// Create a combined recording state (reader + writer with in-memory overlay)
	recordingState := NewRecordingState(stateReader)
	recordingState.SetAccountsToTrace([]common.Address{
		// Add addresses to trace here, e.g.:
		// common.HexToAddress("0x8863786beBE8eB9659DF00b49f8f1eeEc7e2C8c1"),
	})

	// Create the in-block state with the recording state as reader
	ibs := state.New(recordingState)

	// Get header for block context
	header := block.Header()

	// Create EVM block context
	blockCtx := transactions.NewEVMBlockContext(engine, header, true /* requireCanonical */, tx, api._blockReader, chainConfig)
	blockRules := blockCtx.Rules(chainConfig)
	signer := types.MakeSigner(chainConfig, blockNum, header.Time)

	// Track accessed block hashes for BLOCKHASH opcode
	var accessedBlockHashes []uint64
	originalGetHash := blockCtx.GetHash
	blockCtx.GetHash = func(n uint64) (common.Hash, error) {
		accessedBlockHashes = append(accessedBlockHashes, n)
		return originalGetHash(n)
	}

	// Run block initialization (e.g. EIP-2935 blockhash contract, EIP-4788 beacon root)
	fullEngine, ok := engine.(rules.Engine)
	if !ok {
		return nil, fmt.Errorf("engine does not support full rules.Engine interface")
	}
	chainReader := consensuschain.NewReader(chainConfig, tx, api._blockReader, log.Root())
	systemCallCustom := func(contract accounts.Address, data []byte, ibState *state.IntraBlockState, hdr *types.Header, constCall bool) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibState, hdr, fullEngine, constCall, vm.Config{})
	}
	if err = fullEngine.Initialize(chainConfig, chainReader, header, ibs, systemCallCustom, log.Root(), nil); err != nil {
		return nil, fmt.Errorf("failed to initialize block: %w", err)
	}
	if err = ibs.FinalizeTx(blockRules, recordingState); err != nil {
		return nil, fmt.Errorf("failed to finalize engine.Initialize tx: %w", err)
	}

	// Execute all transactions in the block
	for txIndex, txn := range block.Transactions() {
		msg, err := txn.AsMessage(*signer, header.BaseFee, blockRules)
		if err != nil {
			return nil, fmt.Errorf("failed to convert tx %d to message: %w", txIndex, err)
		}

		txCtx := protocol.NewEVMTxContext(msg)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})

		gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time))
		ibs.SetTxContext(blockNum, txIndex)

		_, err = protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
		if err != nil {
			return nil, fmt.Errorf("failed to apply tx %d: %w", txIndex, err)
		}

		if err = ibs.FinalizeTx(blockRules, recordingState); err != nil {
			return nil, fmt.Errorf("failed to finalize tx %d: %w", txIndex, err)
		}
	}

	syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibs, header, fullEngine, false /* constCall */, vm.Config{})
	}

	if _, err = fullEngine.Finalize(chainConfig, types.CopyHeader(header), ibs, block.Uncles(), nil /* receipts */, block.Withdrawals(), chainReader, syscall, true /* skipReceiptsEval */, log.Root()); err != nil {
		return nil, fmt.Errorf("failed to finalize block: %w", err)
	}

	if err = ibs.CommitBlock(blockRules, recordingState); err != nil {
		return nil, fmt.Errorf("failed to commit block: %w", err)
	}

	// Build the execution witness result
	result := &ExecutionWitnessResult{
		State: []hexutil.Bytes{},
		Codes: []hexutil.Bytes{},
		Keys:  []hexutil.Bytes{},
	}

	// Collect all accessed keys (reads) for the Keys field
	readAddresses, readStorageKeys := recordingState.GetAccessedKeys()

	// Collect all modified keys (writes) for the Keys field
	writeAddresses, writeStorageKeys := recordingState.GetModifiedKeys()

	// Merge read and write addresses into a deduplicated set
	allAddresses := make(map[common.Address]struct{})
	for _, addr := range readAddresses {
		allAddresses[addr] = struct{}{}
	}
	for _, addr := range writeAddresses {
		allAddresses[addr] = struct{}{}
	}

	// Merge read and write storage keys into a deduplicated set
	allStorageKeys := make(map[common.Address]map[common.Hash]struct{})
	for addr, keys := range readStorageKeys {
		if allStorageKeys[addr] == nil {
			allStorageKeys[addr] = make(map[common.Hash]struct{})
		}
		for _, key := range keys {
			allStorageKeys[addr][key] = struct{}{}
		}
	}
	for addr, keys := range writeStorageKeys {
		if allStorageKeys[addr] == nil {
			allStorageKeys[addr] = make(map[common.Hash]struct{})
		}
		for _, key := range keys {
			allStorageKeys[addr][key] = struct{}{}
		}
	}

	// Add account keys
	for addr := range allAddresses {
		result.Keys = append(result.Keys, addr.Bytes())
	}

	// Add storage keys
	for addr, keys := range allStorageKeys {
		for key := range keys {
			// Storage keys are represented as composite keys (address + key)
			compositeKey := append(addr.Bytes(), key.Bytes()...)
			result.Keys = append(result.Keys, compositeKey)
		}
	}

	// Collect codes from both reads and writes
	accessedCode := recordingState.GetAccessedCode()
	modifiedCode := recordingState.GetModifiedCode()

	// Merge codes (deduplicated by address, prefer modified if both exist)
	allCode := make(map[common.Address][]byte)
	for addr, code := range accessedCode {
		allCode[addr] = code
	}
	for addr, code := range modifiedCode {
		allCode[addr] = code // Modified code takes precedence
	}

	// Build codeReads map for witness generation (keyed by code hash)
	codeReads := make(map[common.Hash]witnesstypes.CodeWithHash)
	for _, code := range allCode {
		if len(code) > 0 {
			result.Codes = append(result.Codes, code)
			codeHash := crypto.Keccak256Hash(code)
			codeReads[codeHash] = witnesstypes.CodeWithHash{
				Code:     code,
				CodeHash: accounts.InternCodeHash(codeHash),
			}
		}
	}

	// Build merkle proofs for all accessed accounts
	// Use the proof infrastructure from the commitment context
	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	if err != nil {
		return nil, err
	}
	defer domains.Close()
	sdCtx := domains.GetCommitmentContext()
	sdCtx.SetDeferBranchUpdates(false)

	// Get the expected parent state root for verification
	var expectedParentRoot common.Hash

	// Get the parent header for state root verification
	parentHeader, err := api._blockReader.HeaderByNumber(ctx, tx, parentNum)
	if err != nil {
		return nil, err
	}
	if parentHeader == nil {
		return nil, fmt.Errorf("parent header %d not found", parentNum)
	}
	expectedParentRoot = parentHeader.Root
	fmt.Printf("EXPECTED PARENT ROOT = %x\n", expectedParentRoot)

	commitmentStartingTxNum := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
	if firstTxNumInBlock < commitmentStartingTxNum {
		return nil, fmt.Errorf("commitment history pruned: start %d, last tx: %d", commitmentStartingTxNum, firstTxNumInBlock)
	}

	// if err := debugCompareRecordedVsGroundTruth(tx, recordingState, firstTxNumInBlock, lastTxNumInBlock); err != nil {
	// 	return nil, err
	// }

	if len(allAddresses)+len(allStorageKeys)+len(allCode) == 0 { // nothing touched, return empty witness
		return result, nil
	}

	// Helper to touch all accessed/modified accounts, storage keys, and code keys
	touchAllKeys := func() {
		for addr := range allAddresses {
			sdCtx.TouchKey(kv.AccountsDomain, string(addr.Bytes()), nil)
		}
		for addr, keys := range allStorageKeys {
			for key := range keys {
				storageKey := string(common.FromHex(addr.Hex()[2:] + key.Hex()[2:]))
				sdCtx.TouchKey(kv.StorageDomain, storageKey, nil)
			}
		}
		for addr := range allCode {
			sdCtx.TouchKey(kv.CodeDomain, string(addr.Bytes()), nil)
		}
	}

	// Helper to reset commitment to parent block state and re-seek
	resetToParentState := func() error {
		sdCtx.SetHistoryStateReader(tx, firstTxNumInBlock)
		return domains.SeekCommitment(context.Background(), tx)
	}

	// === STEP 1: Collapse Detection via ComputeCommitment ===
	// Detect trie node collapses by running the full commitment calculation for this block.
	// When a FullNode is reduced to a single child (e.g., due to storage deletes),
	// the remaining child's data must be included in the witness for correct
	// state root computation during stateless execution.
	//
	// We only record sibling paths  (not build witness tries) in this first step, because the grid
	// is mutated during ComputeCommitment and would produce incorrect root hashes.
	var collapseSiblingPaths [][]byte

	// Set up split reader: branch data from parent state, plain state from end of block
	branchDataReader := commitmentdb.NewHistoryStateReader(tx, firstTxNumInBlock)
	plainStateReader := commitmentdb.NewHistoryStateReader(tx, lastTxNumInBlock+1)
	splitStateReader := rpchelper.NewCommitmentSplitStateReader(branchDataReader, plainStateReader /* withHistory */, true)
	sdCtx.SetCustomHistoryStateReader(splitStateReader)
	if err := domains.SeekCommitment(context.Background(), tx); err != nil {
		return nil, fmt.Errorf("failed to re-seek commitment for collapse detection: %w", err)
	}

	touchAllKeys()
	//  for debugging purposes the historical keys can be used to compute commitment, but the query is extremely slow
	// err = touchGroundTruthHistoricalKeys(firstTxNumInBlock, lastTxNumInBlock+1)
	if err != nil {
		return nil, err
	}

	sdCtx.SetCollapseTracer(func(hashedKeyPath []byte) {
		fmt.Printf("[debug_executionWitness] node collapse detected at path %s (len=%d)\n", commitment.NibblesToString(hashedKeyPath), len(hashedKeyPath))
		collapseSiblingPaths = append(collapseSiblingPaths, common.Copy(hashedKeyPath))
	})

	computedRootHash, err := sdCtx.ComputeCommitment(ctx, tx, false, blockNum, firstTxNumInBlock, "debug_executionWitness_collapse_detection", nil)
	if err != nil {
		return nil, fmt.Errorf("[debug_executionWitness] collapse detection via ComputeCommitment failed: %v\n", err)
	}

	if common.Hash(computedRootHash) != block.Root() {
		return nil, fmt.Errorf("[debug_executionWitness] computedRootHash(%x)!= expectedRootHash(%x)", computedRootHash, block.Root())
	}

	sdCtx.SetCollapseTracer(nil)

	// === STEP 2: Generate witness for regular keys + siblings from collapses
	if err := resetToParentState(); err != nil {
		return nil, fmt.Errorf("failed to reset commitment for regular witness: %w", err)
	}
	touchAllKeys()

	if len(collapseSiblingPaths) > 0 {
		fmt.Printf("[debug_executionWitness] detected %d sibling paths\n", len(collapseSiblingPaths))

		for _, siblingPath := range collapseSiblingPaths {
			compactSiblingPath := commitment.NibblesToString(siblingPath)
			if err != nil {
				return nil, err
			}
			fmt.Printf("[debug_executionWitness] touching  sibling hashed key: %s (len=%d)\n", compactSiblingPath, len(siblingPath))
			sdCtx.TouchHashedKey(siblingPath)
		}
	}

	witnessTrie, witnessRoot, err := sdCtx.Witness(ctx, codeReads, "debug_executionWitness_collapses")
	if err != nil {
		return nil, fmt.Errorf("failed to generate witness: %w", err)
	}

	// printPreStateCheck(witnessTrie, readAddresses, writeAddresses, readStorageKeys, writeStorageKeys)

	// pre-state root verification
	if !bytes.Equal(witnessRoot, expectedParentRoot[:]) {
		return nil, fmt.Errorf("collapse witness root mismatch: calculated=%x, expected=%x", common.BytesToHash(witnessRoot), expectedParentRoot)
	}

	// Collect all unique RLP-encoded trie nodes by traversing from root to leaves
	// This avoids duplicates that would occur when calling Prove() separately for each key
	allNodes, err := witnessTrie.RLPEncode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode trie nodes: %w", err)
	}
	for _, node := range allNodes {
		result.State = append(result.State, common.Copy(node))
	}

	// // check the RLP decoded trie has the same root hash
	// rlpDecodedTrie, err := trie.RLPDecode(allNodes)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to rlp decode state nodes : %w", err)
	// }

	// decodedHash := rlpDecodedTrie.Hash()
	// if decodedHash != common.Hash(witnessRoot) {
	// 	return nil, fmt.Errorf("root hash of decoded trie is incorrect")
	// }

	// Collect headers for BLOCKHASH opcode support
	// Include headers from accessed block numbers
	seenBlockNums := make(map[uint64]struct{})
	for _, bn := range accessedBlockHashes {
		if _, seen := seenBlockNums[bn]; seen {
			continue
		}
		seenBlockNums[bn] = struct{}{}

		blockHeader, err := api._blockReader.HeaderByNumber(ctx, tx, bn)
		if err != nil || blockHeader == nil {
			continue
		}

		headerRLP, err := rlp.EncodeToBytes(blockHeader)
		if err != nil {
			continue
		}
		result.Headers = append(result.Headers, headerRLP)
	}

	// Verify the execution witness result by re-executing the block statelessly
	chainCfg, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain config: %w", err)
	}

	newStateRoot, stateless, err := execBlockStatelessly(result, block, chainCfg, fullEngine)
	if err != nil {
		return nil, fmt.Errorf("[debug_executionWitness] stateless block execution failed: %w", err)
	}
	_ = stateless

	// Query the expected state for all modified accounts from the actual state DB
	expectedState, expectedStorage, err := api.buildExpectedPostState(ctx, tx, blockNum, block,
		readAddresses, writeAddresses, readStorageKeys, writeStorageKeys)
	if err != nil {
		return nil, err
	}

	//  Compare computed state vs expected state for debugging
	compareComputedVsExpectedState(stateless, expectedState, expectedStorage, stateless.storageDeletes)

	// Verify the root matches the block's state root
	expectedRoot := block.Root()
	if newStateRoot != expectedRoot {
		return nil, fmt.Errorf("[debug_executionWitness] state root mismatch after stateless execution : got %x, expected %x", newStateRoot, expectedRoot)
	}

	log.Info("Witness successfully verified 🚀\n", "blockNum", blockNum)
	return result, nil
}

// buildExpectedPostState queries the actual state DB to build expected post-state for verification.
// It uses the commitment context to get correct storage roots (not stored in DB to save space).
func (api *DebugAPIImpl) buildExpectedPostState(
	ctx context.Context,
	tx kv.TemporalTx,
	blockNum uint64,
	block *types.Block,
	readAddresses, writeAddresses []common.Address,
	readStorageKeys, writeStorageKeys map[common.Address][]common.Hash,
) (map[common.Address]*accounts.Account, map[common.Address]map[common.Hash]uint256.Int, error) {
	expectedState := make(map[common.Address]*accounts.Account)
	expectedStorage := make(map[common.Address]map[common.Hash]uint256.Int)

	// Create commitment context for accurate storage roots
	postDomains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create post-state domains: %w", err)
	}
	defer postDomains.Close()
	postSdCtx := postDomains.GetCommitmentContext()
	postSdCtx.SetDeferBranchUpdates(false)

	// Set up to read state at current block (after execution)
	latestBlock, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get latest block: %w", err)
	}
	if blockNum < latestBlock {
		// Get first txnum of blockNum+1 to ensure correct state root
		lastTxnInBlock, err := api._txNumReader.Min(ctx, tx, blockNum+1)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get last txn in block: %w", err)
		}
		postSdCtx.SetHistoryStateReader(tx, lastTxnInBlock)
		if err := postDomains.SeekCommitment(context.Background(), tx); err != nil {
			return nil, nil, fmt.Errorf("failed to seek commitment: %w", err)
		}
	}

	// Touch all modified accounts and storage keys for the post-state trie
	for _, addr := range writeAddresses {
		postSdCtx.TouchKey(kv.AccountsDomain, string(addr.Bytes()), nil)
	}
	for addr, keys := range writeStorageKeys {
		for _, key := range keys {
			storageKey := string(common.FromHex(addr.Hex()[2:] + key.Hex()[2:]))
			postSdCtx.TouchKey(kv.StorageDomain, storageKey, nil)
		}
	}

	// Generate the trie with correct storage roots
	postTrie, postRoot, err := postSdCtx.Witness(ctx, nil, "debug_executionWitness_postState")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate post-state trie: %w", err)
	}

	// Verify the post-state root matches the block's state root
	if !bytes.Equal(postRoot, block.Root().Bytes()) {
		fmt.Printf("Warning: post-state trie root %x doesn't match block root %x\n", postRoot, block.Root())
	}

	// Read account data from the post-state trie (with correct storage roots)
	// Include both read and write addresses
	for _, addr := range readAddresses {
		addrHash := crypto.Keccak256(addr.Bytes())
		acc, _ := postTrie.GetAccount(addrHash)
		expectedState[addr] = acc
	}
	for _, addr := range writeAddresses {
		addrHash := crypto.Keccak256(addr.Bytes())
		acc, _ := postTrie.GetAccount(addrHash)
		expectedState[addr] = acc
	}

	// Read storage values from the state reader
	currentBlockNum := rpc.BlockNumber(blockNum)
	currentNrOrHash := rpc.BlockNumberOrHash{BlockNumber: &currentBlockNum}
	postStateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, currentNrOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create postStateReader: %w", err)
	}
	// Include both read and write storage keys
	for addr, keys := range readStorageKeys {
		if expectedStorage[addr] == nil {
			expectedStorage[addr] = make(map[common.Hash]uint256.Int)
		}
		for _, key := range keys {
			storageKey := accounts.InternKey(key)
			val, _, err := postStateReader.ReadAccountStorage(accounts.InternAddress(addr), storageKey)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read expected storage in post state: %x key %x: %w", addr, key, err)
			}
			expectedStorage[addr][key] = val
		}
	}
	for addr, keys := range writeStorageKeys {
		if expectedStorage[addr] == nil {
			expectedStorage[addr] = make(map[common.Hash]uint256.Int)
		}
		for _, key := range keys {
			storageKey := accounts.InternKey(key)
			val, _, err := postStateReader.ReadAccountStorage(accounts.InternAddress(addr), storageKey)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read expected storage in post state: %x key %x: %w", addr, key, err)
			}
			expectedStorage[addr][key] = val
		}
	}

	return expectedState, expectedStorage, nil
}

// printExpectedPostState prints the expected post-state in a human-readable format
func printExpectedPostState(blockNum uint64, expectedStateRoot common.Hash, expectedState map[common.Address]*accounts.Account, expectedStorage map[common.Address]map[common.Hash]uint256.Int) {
	fmt.Printf("\n")
	fmt.Printf("╔══════════════════════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║              EXPECTED POST-STATE FOR BLOCK %-5d                             ║\n", blockNum)
	fmt.Printf("╠══════════════════════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ Expected State Root: %x ║\n", expectedStateRoot)
	fmt.Printf("╚══════════════════════════════════════════════════════════════════════════════╝\n")
	fmt.Printf("\n")

	// Sort addresses for consistent output
	addrs := make([]common.Address, 0, len(expectedState))
	for addr := range expectedState {
		addrs = append(addrs, addr)
	}

	for _, addr := range addrs {
		acc := expectedState[addr]
		addrHash, _ := common.HashData(addr[:])

		fmt.Printf("┌─────────────────────────────────────────────────────────────────────────────┐\n")
		fmt.Printf("│ Account: %s\n", addr.Hex())
		fmt.Printf("│ Hash:    %x\n", addrHash)
		fmt.Printf("├─────────────────────────────────────────────────────────────────────────────┤\n")

		if acc == nil {
			fmt.Printf("│ Status: DELETED\n")
		} else {
			fmt.Printf("│ Nonce:        %d\n", acc.Nonce)
			fmt.Printf("│ Balance:      %s wei\n", acc.Balance.String())

			// Storage Root
			if acc.Root == trie.EmptyRoot {
				fmt.Printf("│ Storage Root: %x (empty)\n", acc.Root)
			} else {
				fmt.Printf("│ Storage Root: %x\n", acc.Root)
			}

			// Code Hash
			if acc.CodeHash == accounts.EmptyCodeHash {
				fmt.Printf("│ Code Hash:    %x (empty - EOA)\n", acc.CodeHash)
			} else {
				fmt.Printf("│ Code Hash:    %x (contract)\n", acc.CodeHash)
			}
		}

		// Print storage for this account
		if storage, ok := expectedStorage[addr]; ok && len(storage) > 0 {
			fmt.Printf("├─────────────────────────────────────────────────────────────────────────────┤\n")
			fmt.Printf("│ Storage (%d slots):\n", len(storage))
			for key, val := range storage {
				keyHash, _ := common.HashData(key[:])
				fmt.Printf("│   Slot %x... (hash %x...):\n", key[:8], keyHash[:8])
				fmt.Printf("│     Value: %s\n", val.String())
				fmt.Printf("│     Hex:   0x%x\n", val.Bytes())
			}
		}

		fmt.Printf("└─────────────────────────────────────────────────────────────────────────────┘\n")
		fmt.Printf("\n")
	}
}

// printPreStateCheck prints the pre-state status of all touched accounts and storage.
// Missing items aren't necessarily errors - they could be created during block execution.
func printPreStateCheck(witnessTrie *trie.Trie, readAddresses, writeAddresses []common.Address, readStorageKeys, writeStorageKeys map[common.Address][]common.Hash) {
	// Merge all addresses into a deduplicated map with their access type
	type accessInfo struct {
		read  bool
		write bool
	}
	allAddrs := make(map[common.Address]*accessInfo)
	for _, addr := range readAddresses {
		if allAddrs[addr] == nil {
			allAddrs[addr] = &accessInfo{}
		}
		allAddrs[addr].read = true
	}
	for _, addr := range writeAddresses {
		if allAddrs[addr] == nil {
			allAddrs[addr] = &accessInfo{}
		}
		allAddrs[addr].write = true
	}

	fmt.Printf("\n--- Pre-state check: %d touched accounts ---\n", len(allAddrs))
	for addr, info := range allAddrs {
		addrHash, _ := common.HashData(addr[:])
		acc, found := witnessTrie.GetAccount(addrHash[:])

		accessType := ""
		if info.read && info.write {
			accessType = "R/W"
		} else if info.read {
			accessType = "R"
		} else {
			accessType = "W"
		}

		if found && acc != nil {
			fmt.Printf("  [%s] %s (hash %x): Nonce=%d, Balance=%s, Root=%x\n",
				accessType, addr.Hex(), addrHash[:8], acc.Nonce, acc.Balance.String(), acc.Root[:8])
		} else {
			fmt.Printf("  [%s] %s (hash %x): not in pre-state (may be created)\n",
				accessType, addr.Hex(), addrHash[:8])
		}
	}

	// Merge all storage keys into a deduplicated map
	type storageAccessInfo struct {
		read  bool
		write bool
	}
	allStorage := make(map[common.Address]map[common.Hash]*storageAccessInfo)
	for addr, keys := range readStorageKeys {
		if allStorage[addr] == nil {
			allStorage[addr] = make(map[common.Hash]*storageAccessInfo)
		}
		for _, key := range keys {
			if allStorage[addr][key] == nil {
				allStorage[addr][key] = &storageAccessInfo{}
			}
			allStorage[addr][key].read = true
		}
	}
	for addr, keys := range writeStorageKeys {
		if allStorage[addr] == nil {
			allStorage[addr] = make(map[common.Hash]*storageAccessInfo)
		}
		for _, key := range keys {
			if allStorage[addr][key] == nil {
				allStorage[addr][key] = &storageAccessInfo{}
			}
			allStorage[addr][key].write = true
		}
	}

	// Count total storage keys
	totalStorageKeys := 0
	for _, keys := range allStorage {
		totalStorageKeys += len(keys)
	}

	if totalStorageKeys > 0 {
		fmt.Printf("\n--- Pre-state check: %d touched storage slots ---\n", totalStorageKeys)
		for addr, keys := range allStorage {
			addrHash, _ := common.HashData(addr[:])
			fmt.Printf("  Account %s (hash %x):\n", addr.Hex(), addrHash[:8])
			for key, info := range keys {
				keyHash, _ := common.HashData(key[:])
				cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)

				accessType := ""
				if info.read && info.write {
					accessType = "R/W"
				} else if info.read {
					accessType = "R"
				} else {
					accessType = "W"
				}

				if val, found := witnessTrie.Get(cKey); found {
					var valInt uint256.Int
					valInt.SetBytes(val)
					fmt.Printf("    [%s] Key %x (hash %x): value=%s\n",
						accessType, key, keyHash[:8], valInt.String())
				} else {
					fmt.Printf("    [%s] Key %x (hash %x): not in pre-state (may be created)\n",
						accessType, key, keyHash[:8])
				}
			}
		}
	}
}

// compareComputedVsExpectedState compares the state computed by witnessStateless against the expected state.
func compareComputedVsExpectedState(stateless *witnessStateless, expectedState map[common.Address]*accounts.Account, expectedStorage map[common.Address]map[common.Hash]uint256.Int, storageDeletes map[common.Address]map[common.Hash]struct{}) {
	fmt.Printf("\n=== Comparing computed vs expected state ===\n")
	for addr, expectedAcc := range expectedState {
		addrHash, _ := common.HashData(addr[:])
		computedAcc, found := stateless.t.GetAccount(addrHash[:])

		fmt.Printf("\nAccount %s (hash %x):\n", addr.Hex(), addrHash[:8])
		if expectedAcc != nil {
			fmt.Printf("  EXPECTED: Nonce=%d, Balance=%s, Root=%x, CodeHash=%x\n",
				expectedAcc.Nonce, expectedAcc.Balance.String(), expectedAcc.Root, expectedAcc.CodeHash)
		} else {
			fmt.Printf("  EXPECTED: nil (deleted)\n")
		}
		if found && computedAcc != nil {
			fmt.Printf("  COMPUTED: Nonce=%d, Balance=%s, Root=%x, CodeHash=%x\n",
				computedAcc.Nonce, computedAcc.Balance.String(), computedAcc.Root, computedAcc.CodeHash)
			// Check for differences - only print mismatches or a single tick if all match
			if expectedAcc != nil {
				allMatch := true
				if _, ok := storageDeletes[addr]; ok {
					fmt.Printf("   ⛔️ STORAGE deletes on this account!\n")
				}
				if expectedAcc.Nonce != computedAcc.Nonce {
					fmt.Printf("    ❌ NONCE MISMATCH!\n")
					allMatch = false
				}
				if !expectedAcc.Balance.Eq(&computedAcc.Balance) {
					fmt.Printf("    ❌ BALANCE MISMATCH! (diff: %d wei)\n", new(uint256.Int).Sub(&expectedAcc.Balance, &computedAcc.Balance).Uint64())
					allMatch = false
				}
				if expectedAcc.Root != computedAcc.Root {
					fmt.Printf("    ❌ STORAGE ROOT MISMATCH!\n")
					allMatch = false
				}
				if expectedAcc.CodeHash != computedAcc.CodeHash {
					fmt.Printf("    ❌ CODE HASH MISMATCH!\n")
					allMatch = false
				}
				if allMatch {
					fmt.Printf("    ✅ All fields match\n")
				}
			}
		} else {
			fmt.Printf("  COMPUTED: NOT FOUND or nil\n")
		}
	}

	// Compare storage values
	for addr, expectedKeys := range expectedStorage {
		addrHash, _ := common.HashData(addr[:])
		fmt.Printf("\nStorage for %s (hash %x):\n", addr.Hex(), addrHash[:8])
		for key, expectedVal := range expectedKeys {
			keyHash, _ := common.HashData(key[:])
			cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)
			computedBytes, found := stateless.t.Get(cKey)
			var computedVal uint256.Int
			if found && len(computedBytes) > 0 {
				computedVal.SetBytes(computedBytes)
			}
			fmt.Printf("  Key %x (hash %x):\n", key, keyHash[:8])
			fmt.Printf("    EXPECTED: %s (hex: %x)\n", expectedVal.String(), expectedVal.Bytes())
			fmt.Printf("    COMPUTED: %s (hex: %x)\n", computedVal.String(), computedVal.Bytes())
			if !expectedVal.Eq(&computedVal) {
				fmt.Printf("    ❌ STORAGE VALUE MISMATCH!\n")
			} else {
				fmt.Printf("    ✅\n")
			}
		}
	}
}

// witnessStateless is a StateReader/StateWriter implementation that operates on a witness trie.
// It's used for stateless block verification.
type witnessStateless struct {
	t              *trie.Trie                                     // Witness trie decoded from ExecutionWitnessResult.State
	codeMap        map[common.Hash][]byte                         // Code hash -> bytecode
	codeUpdates    map[common.Hash][]byte                         // Code updates during execution
	storageWrites  map[common.Address]map[common.Hash]uint256.Int // addr -> key -> value
	storageDeletes map[common.Address]map[common.Hash]struct{}    // addr -> key
	accountUpdates map[common.Address]*accounts.Account           // addr -> account
	deleted        map[common.Address]struct{}                    // deleted accounts
	created        map[common.Address]struct{}                    // created contracts
	trace          bool
}

// Ensure witnessStateless implements both interfaces
var _ state.StateReader = (*witnessStateless)(nil)
var _ state.StateWriter = (*witnessStateless)(nil)

// newWitnessStateless creates a new witnessStateless from ExecutionWitnessResult
func newWitnessStateless(result *ExecutionWitnessResult) (*witnessStateless, error) {
	// Decode the witness trie from RLP-encoded nodes
	encodedNodes := make([][]byte, len(result.State))
	for i, node := range result.State {
		encodedNodes[i] = node
	}

	witnessTrie, err := trie.RLPDecode(encodedNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode witness trie: %w", err)
	}

	targetAddress := common.HexToAddress("0x881D40237659C251811CEC9c364ef91dC08D300C")
	addrHash, _ := common.HashData(targetAddress[:])
	acc, ok := witnessTrie.GetAccount(addrHash[:])
	_, _ = acc, ok

	// Build code map from codes list
	codeMap := make(map[common.Hash][]byte)
	for _, code := range result.Codes {
		codeHash := crypto.Keccak256Hash(code)
		codeMap[codeHash] = code
	}

	return &witnessStateless{
		t:              witnessTrie,
		codeMap:        codeMap,
		codeUpdates:    make(map[common.Hash][]byte),
		storageWrites:  make(map[common.Address]map[common.Hash]uint256.Int),
		storageDeletes: make(map[common.Address]map[common.Hash]struct{}),
		accountUpdates: make(map[common.Address]*accounts.Account),
		deleted:        make(map[common.Address]struct{}),
		created:        make(map[common.Address]struct{}),
		trace:          false,
	}, nil
}

// StateReader interface implementation

func (s *witnessStateless) SetTrace(trace bool, tracePrefix string) {
	s.trace = trace
}

func (s *witnessStateless) Trace() bool {
	return s.trace
}

func (s *witnessStateless) TracePrefix() string {
	return ""
}

func (s *witnessStateless) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return s.ReadAccountData(address)
}

func (s *witnessStateless) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return nil, err
	}

	// Check if account has been updated in memory
	if acc, ok := s.accountUpdates[addr]; ok {
		return acc, nil
	}

	// Check if account has been deleted
	if _, ok := s.deleted[addr]; ok {
		return nil, nil
	}

	// Read from trie
	acc, ok := s.t.GetAccount(addrHash[:])
	if ok {
		return acc, nil
	}
	return nil, nil
}

func (s *witnessStateless) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addr := address.Value()
	keyValue := key.Value()

	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return uint256.Int{}, false, err
	}

	seckey, err := common.HashData(keyValue[:])
	if err != nil {
		return uint256.Int{}, false, err
	}

	// Check if storage has been updated in memory
	if m, ok := s.storageWrites[addr]; ok {
		if v, ok := m[keyValue]; ok {
			return v, true, nil
		}
	}

	// Check if storage has been deleted
	if d, ok := s.storageDeletes[addr]; ok {
		if _, ok := d[keyValue]; ok {
			return uint256.Int{}, false, nil
		}
	}

	// Read from trie
	cKey := dbutils.GenerateCompositeTrieKey(addrHash, seckey)
	if enc, ok := s.t.Get(cKey); ok {
		var res uint256.Int
		res.SetBytes(enc)
		return res, true, nil
	}

	return uint256.Int{}, false, nil
}

func (s *witnessStateless) ReadAccountCode(address accounts.Address) ([]byte, error) {
	addr := address.Value()
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return nil, err
	}

	// Check code updates first — look up by the account's code hash (matching UpdateAccountCode key)
	acc, err := s.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if acc != nil {
		codeHashValue := acc.CodeHash.Value()
		if code, ok := s.codeUpdates[codeHashValue]; ok {
			return code, nil
		}
	}

	// Check trie for code
	if code, ok := s.t.GetAccountCode(addrHash[:]); ok {
		return code, nil
	}

	// Check code map (from witness)
	if acc != nil {
		codeHashValue := acc.CodeHash.Value()
		if code, ok := s.codeMap[codeHashValue]; ok {
			return code, nil
		}
	}

	return nil, nil
}

func (s *witnessStateless) ReadAccountCodeSize(address accounts.Address) (int, error) {
	code, err := s.ReadAccountCode(address)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (s *witnessStateless) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}

func (s *witnessStateless) HasStorage(address accounts.Address) (bool, error) {
	addr := address.Value()
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return false, err
	}

	// Check if account has been deleted
	if _, ok := s.deleted[addr]; ok {
		return false, nil
	}

	// Check if we know about any storage updates with non-empty values
	for _, v := range s.storageWrites[addr] {
		if !v.IsZero() {
			return true, nil
		}
	}

	// Check account in trie
	acc, ok := s.t.GetAccount(addrHash[:])
	if !ok {
		return false, nil
	}

	return acc.Root != trie.EmptyRoot, nil
}

// StateWriter interface implementation

func (s *witnessStateless) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	addr := address.Value()
	// Make a copy to avoid the account being modified later
	if account != nil {
		accCopy := new(accounts.Account)
		accCopy.Copy(account)
		s.accountUpdates[addr] = accCopy
	} else {
		s.accountUpdates[addr] = nil
	}
	return nil
}

func (s *witnessStateless) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	addr := address.Value()
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return err
	}
	// Only delete if the account exists in the original state (trie or was previously updated)
	// Skip deletes for accounts that weren't in the witness - they don't affect the state root
	existingInTrie, wasInTrie := s.t.GetAccount(addrHash[:])
	_, wasUpdated := s.accountUpdates[addr]
	if (!wasInTrie || existingInTrie == nil) && !wasUpdated {
		// Account wasn't in the original state, skip deletion
		return nil
	}
	s.accountUpdates[addr] = nil
	s.deleted[addr] = struct{}{}
	return nil
}

func (s *witnessStateless) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	s.codeUpdates[codeHash.Value()] = code
	return nil
}

func (s *witnessStateless) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	addr := address.Value()
	keyValue := key.Value()

	if value.IsZero() {
		// Delete: add to storageDeletes, remove from storageWrites
		d, ok := s.storageDeletes[addr]
		if !ok {
			d = make(map[common.Hash]struct{})
			s.storageDeletes[addr] = d
		}
		d[keyValue] = struct{}{}

		// Remove from writes if present
		if m, ok := s.storageWrites[addr]; ok {
			delete(m, keyValue)
		}
	} else {
		// Write: add to storageWrites, remove from storageDeletes
		m, ok := s.storageWrites[addr]
		if !ok {
			m = make(map[common.Hash]uint256.Int)
			s.storageWrites[addr] = m
		}
		m[keyValue] = value

		// Remove from deletes if present
		if d, ok := s.storageDeletes[addr]; ok {
			delete(d, keyValue)
		}
	}
	return nil
}

func (s *witnessStateless) CreateContract(address accounts.Address) error {
	addr := address.Value()
	s.created[addr] = struct{}{}
	delete(s.deleted, addr)
	return nil
}

// debugPrintPendingUpdates prints all pending state changes for debugging
func (s *witnessStateless) debugPrintPendingUpdates(blockNum uint64) {
	fmt.Printf("=== Block %d: Pending state updates before Finalize ===\n", blockNum)
	fmt.Printf("Initial trie root: %x\n", s.t.Hash())
	fmt.Printf("Trie root node type: %T\n", s.t.RootNode)

	fmt.Printf("\n--- Account Updates (%d) ---\n", len(s.accountUpdates))
	for addr, acc := range s.accountUpdates {
		// Check if account exists in trie
		addrHash, _ := common.HashData(addr[:])
		existingAcc, existsInTrie := s.t.GetAccount(addrHash[:])
		if acc != nil {
			if existsInTrie && existingAcc != nil {
				fmt.Printf("  UPDATE %x: Nonce=%d, Balance=%s, CodeHash=%x, Root=%x (exists in trie)\n",
					addr[:], acc.Nonce, acc.Balance.String(), acc.CodeHash, acc.Root)
			} else {
				fmt.Printf("  INSERT %x: Nonce=%d, Balance=%s, CodeHash=%x, Root=%x (NEW - not in trie)\n",
					addr[:], acc.Nonce, acc.Balance.String(), acc.CodeHash, acc.Root)
			}
		} else {
			fmt.Printf("  DELETE %x\n", addr[:])
		}
	}

	fmt.Printf("\n--- Created Contracts (%d) ---\n", len(s.created))
	for addr := range s.created {
		fmt.Printf("  CREATED %x\n", addr[:])
	}

	fmt.Printf("\n--- Deleted Accounts (%d) ---\n", len(s.deleted))
	for addr := range s.deleted {
		fmt.Printf("  DELETED %x\n", addr[:])
	}

	fmt.Printf("\n--- Storage Writes ---\n")
	for addr, storageMap := range s.storageWrites {
		fmt.Printf("  Account %x (%d slots):\n", addr[:8], len(storageMap))
		for key, value := range storageMap {
			fmt.Printf("    %x = %s\n", key[:], value.String())
		}
	}

	fmt.Printf("\n--- Storage Deletes ---\n")
	for addr, storageMap := range s.storageDeletes {
		fmt.Printf("  Account %x (%d slots):\n", addr[:8], len(storageMap))
		for key := range storageMap {
			fmt.Printf("    %x\n", key[:])
		}
	}

	fmt.Printf("\n--- Code Updates (%d) ---\n", len(s.codeUpdates))
	for codeHash, code := range s.codeUpdates {
		fmt.Printf("  %x: %d bytes\n", codeHash[:], len(code))
	}
	fmt.Println("=== End of pending updates ===")
}

// Finalize applies all pending updates to the trie and returns the new root hash
func (s *witnessStateless) Finalize() (common.Hash, error) {
	// fmt.Printf("\n=== Finalize: Applying updates ===\n")

	// Handle created contracts - clear their storage subtries
	for addr := range s.created {
		if account, ok := s.accountUpdates[addr]; ok && account != nil {
			account.Root = trie.EmptyRoot
		}
		addrHash, _ := common.HashData(addr[:])
		s.t.DeleteSubtree(addrHash[:])
		// fmt.Printf("  Created contract %x: cleared subtrie\n", addr[:8])
	}

	// Apply account updates
	for addr, account := range s.accountUpdates {
		addrHash, _ := common.HashData(addr[:])
		if account != nil {
			// fmt.Printf("  UpdateAccount %x: Nonce=%d, Balance=%s\n", addr[:8], account.Nonce, account.Balance.String())
			s.t.UpdateAccount(addrHash[:], account)
		} else {
			fmt.Printf("  Delete %x\n", addr[:8])
			s.t.Delete(addrHash[:])
		}
	}

	// Apply code updates - must be done after account updates so accounts exist in trie
	for addr, account := range s.accountUpdates {
		if account == nil {
			continue
		}
		addrHash, _ := common.HashData(addr[:])
		codeHashValue := account.CodeHash.Value()
		if code, ok := s.codeUpdates[codeHashValue]; ok {
			// fmt.Printf("  UpdateAccountCode %x: codeHash=%x, len=%d\n", addr[:8], codeHashValue[:8], len(code))
			if err := s.t.UpdateAccountCode(addrHash[:], code); err != nil {
				return common.Hash{}, fmt.Errorf("failed to update account code for addr %x: %v\n", addr, err)
			}
		}
	}

	updatedAccounts := map[common.Address]struct{}{}

	// Apply storage writes
	for addr, m := range s.storageWrites {
		if _, ok := s.deleted[addr]; ok {
			continue
		}
		updatedAccounts[addr] = struct{}{}
		addrHash, _ := common.HashData(addr[:])
		for key, v := range m {
			keyHash, _ := common.HashData(key[:])
			cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)
			// fmt.Printf("  Storage write: account=%x, key=%x, value=%x\n", addr[:8], key[:8], v.Bytes())
			s.t.Update(cKey, v.Bytes())
			s.t.DeepHash(addrHash[:])
			// Don't call Hash() here - it would cache node refs and interfere with later updates
		}
	}

	// Apply storage deletes
	for addr, m := range s.storageDeletes {
		if _, ok := s.deleted[addr]; ok {
			continue
		}
		updatedAccounts[addr] = struct{}{}
		addrHash, _ := common.HashData(addr[:])
		for key := range m {
			keyHash, _ := common.HashData(key[:])
			cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)
			// hashedKeyPath := trie.KeybytesToHex(cKey)
			// fmt.Printf("DELETING Storage Key at path %x\n", hashedKeyPath)
			s.t.Delete(cKey)
		}
	}

	// Update storage roots for modified accounts
	// DeepHash computes the storage root, then we update the account with it
	for addr := range updatedAccounts {
		if account, ok := s.accountUpdates[addr]; ok && account != nil {
			addrHash, _ := common.HashData(addr[:])
			gotRoot, root := s.t.DeepHash(addrHash[:])
			// fmt.Printf("  Storage root for %x: gotRoot=%v, root=%x\n", addr[:8], gotRoot, root)
			if gotRoot {
				// Update the account's storage root and re-apply to trie
				account.Root = root
				s.t.UpdateAccount(addrHash[:], account)
				// fmt.Printf("  Updated account %x with storage root %x\n", addr[:8], root)
			}
		}
	}

	// Handle deleted accounts
	for addr := range s.deleted {
		if _, ok := s.created[addr]; ok {
			continue
		}
		if account, ok := s.accountUpdates[addr]; ok && account != nil {
			account.Root = trie.EmptyRoot
		}
		addrHash, _ := common.HashData(addr[:])
		s.t.DeleteSubtree(addrHash[:])
		// fmt.Printf("  Deleted account subtrie %x, hash=%x\n", addr[:8], s.t.Hash())
	}

	// Compute and return the final hash
	finalHash := s.t.Hash()
	return finalHash, nil
}

// execBlockStatelessly executes the block statelessly.
// It decodes the witness trie, executes all transactions and returns the resulting state root
func execBlockStatelessly(result *ExecutionWitnessResult, block *types.Block, chainConfig *chain.Config, engine rules.Engine) (postStateRoot common.Hash, stateless *witnessStateless, err error) {
	// Skip verification for genesis block - it has no transactions to execute
	// but has pre-allocated accounts which would cause a state root mismatch
	if block.NumberU64() == 0 {
		return
	}

	// Skip verification if the witness trie is empty
	if len(result.State) == 0 {
		return common.Hash{}, nil, fmt.Errorf("empty State field in witness")
	}
	// Create stateless state from the witness - this is both reader and writer
	stateless, err = newWitnessStateless(result)
	if err != nil {
		return common.Hash{}, nil, fmt.Errorf("failed to create witness stateless: %w", err)
	}

	// Build header lookup map from result.Headers for BLOCKHASH opcode
	headerByNumber := make(map[uint64]*types.Header)
	for _, headerRLP := range result.Headers {
		var header types.Header
		if err := rlp.DecodeBytes(headerRLP, &header); err != nil {
			continue // Skip malformed headers
		}
		headerByNumber[header.Number.Uint64()] = &header
	}

	// Create getHashFn that uses the headers from the witness
	getHashFn := func(n uint64) (common.Hash, error) {
		if header, ok := headerByNumber[n]; ok {
			return header.Hash(), nil
		}
		return common.Hash{}, nil
	}

	// Create the in-block state with the witness stateless as reader
	ibs := state.New(stateless)
	header := block.Header()
	blockNum := block.NumberU64()

	// Create EVM block context - pass header.Coinbase as the author/beneficiary
	// This ensures gas fees go to the correct address based on the block header
	coinbase := accounts.InternAddress(header.Coinbase)
	blockCtx := protocol.NewEVMBlockContext(header, getHashFn, nil, coinbase, chainConfig)
	blockRules := blockCtx.Rules(chainConfig)
	signer := types.MakeSigner(chainConfig, blockNum, header.Time)

	// Run block initialization (e.g. EIP-2935 blockhash contract, EIP-4788 beacon root)
	systemCallCustom := func(contract accounts.Address, data []byte, ibState *state.IntraBlockState, hdr *types.Header, constCall bool) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibState, hdr, engine, constCall, vm.Config{})
	}
	if err = engine.Initialize(chainConfig, nil /* chainReader */, header, ibs, systemCallCustom, log.Root(), nil); err != nil {
		return common.Hash{}, stateless, fmt.Errorf("verification: failed to initialize block: %w", err)
	}
	if err = ibs.FinalizeTx(blockRules, stateless); err != nil {
		return common.Hash{}, stateless, fmt.Errorf("verification: failed to finalize engine.Initialize tx: %w", err)
	}

	// Execute all transactions in the block
	for txIndex, txn := range block.Transactions() {
		msg, err := txn.AsMessage(*signer, header.BaseFee, blockRules)
		if err != nil {
			return common.Hash{}, stateless, fmt.Errorf("[statelessExec] failed to convert tx %d to message: %w", txIndex, err)
		}

		txCtx := protocol.NewEVMTxContext(msg)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})

		gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time))
		ibs.SetTxContext(blockNum, txIndex)

		// Apply the message - gasBailout must be false to properly deduct gas from sender
		_, err = protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
		if err != nil {
			return common.Hash{}, stateless, fmt.Errorf("[statelessExec] failed to apply tx %d: %w", txIndex, err)
		}

		// Finalize tx - state changes go to the witness stateless
		if err = ibs.FinalizeTx(blockRules, stateless); err != nil {
			return common.Hash{}, stateless, fmt.Errorf("[statelessExec] failed to finalize tx %d: %w", txIndex, err)
		}
	}

	syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibs, header, engine, false /* constCall */, vm.Config{})
	}
	// only Bor and AuRa engine use ChainReader. And the ChainReader is only used to read headers. This means their
	// witness may need to be augmented with headers accessed during their engine.Finalize(). This is something that
	// can be implemented later. For now use ChainReader = nil, as this is sufficient for Ethereum.
	_, err = engine.Finalize(chainConfig, types.CopyHeader(header), ibs, block.Uncles() /*receipts */, nil, block.Withdrawals() /*chainReader */, nil, syscall /*skipReceiptsEval*/, true, log.Root())
	if err != nil {
		return common.Hash{}, stateless, fmt.Errorf("[statelessExec] engine.Finalize failed: %w", err)
	}

	err = ibs.CommitBlock(blockRules, stateless)
	if err != nil {
		return common.Hash{}, stateless, fmt.Errorf("[statelessExec] ibs.CommitBlock() failed : %w", err)
	}

	// Finalize and compute the resulting state root
	newStateRoot, err := stateless.Finalize()
	if err != nil {
		return common.Hash{}, stateless, fmt.Errorf("[statelessExec] stateless.Finalize() failed: %w", err)
	}
	return newStateRoot, stateless, nil
}

// MemStats returns detailed runtime memory statistics.
func (api *DebugAPIImpl) MemStats() *runtime.MemStats {
	s := new(runtime.MemStats)
	runtime.ReadMemStats(s)
	return s
}

// GcStats returns GC statistics.
func (api *DebugAPIImpl) GcStats() *debug.GCStats {
	s := new(debug.GCStats)
	debug.ReadGCStats(s)
	return s
}

// FreeOSMemory forces a garbage collection.
func (api *DebugAPIImpl) FreeOSMemory() {
	debug.FreeOSMemory()
}

// SetGCPercent sets the garbage collection target percentage. It returns the previous
// setting. A negative value disables GC.
func (api *DebugAPIImpl) SetGCPercent(v int) int {
	return debug.SetGCPercent(v)
}

// SetMemoryLimit sets the GOMEMLIMIT for the process. It returns the previous limit.
// Note:
//
//   - The input limit is provided as bytes. A negative input does not adjust the limit
//
//   - A zero limit or a limit that's lower than the amount of memory used by the Go
//     runtime may cause the garbage collector to run nearly continuously. However,
//     the application may still make progress.
//
//   - Setting the limit too low will cause Geth to become unresponsive.
//
//   - Geth also allocates memory off-heap, particularly for fastCache and Pebble,
//     which can be non-trivial (a few gigabytes by default).
func (api *DebugAPIImpl) SetMemoryLimit(limit int64) int64 {
	log.Info("Setting memory limit", "size", common.StorageSize(limit))
	return debug.SetMemoryLimit(limit)
}
