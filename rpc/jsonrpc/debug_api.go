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

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/jsonstream"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	tracersConfig "github.com/erigontech/erigon/eth/tracers/config"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
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
	AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, start []byte, maxResults int, nocode, nostorage bool) (state.IteratorDump, error)
	GetModifiedAccountsByNumber(ctx context.Context, startNum rpc.BlockNumber, endNum *rpc.BlockNumber) ([]common.Address, error)
	GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error)
	TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error
	AccountAt(ctx context.Context, blockHash common.Hash, txIndex uint64, account common.Address) (*AccountResult, error)
	GetRawHeader(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)
	GetRawBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)
	GetRawReceipts(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) ([]hexutil.Bytes, error)
	GetBadBlocks(ctx context.Context) ([]map[string]interface{}, error)
	GetRawTransaction(ctx context.Context, hash common.Hash) (hexutil.Bytes, error)
}

// PrivateDebugAPIImpl is implementation of the PrivateDebugAPI interface based on remote Db access
type PrivateDebugAPIImpl struct {
	*BaseAPI
	db     kv.TemporalRoDB
	GasCap uint64
}

// NewPrivateDebugAPI returns PrivateDebugAPIImpl instance
func NewPrivateDebugAPI(base *BaseAPI, db kv.TemporalRoDB, gascap uint64) *PrivateDebugAPIImpl {
	return &PrivateDebugAPIImpl{
		BaseAPI: base,
		db:      db,
		GasCap:  gascap,
	}
}

// storageRangeAt implements debug_storageRangeAt. Returns information about a range of storage locations (if any) for the given address.
func (api *PrivateDebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error) {
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
	minTxNum, err := api._txNumReader.Min(tx, *number)
	if err != nil {
		return StorageRangeResult{}, err
	}
	fromTxNum := minTxNum + txIndex + 1 //+1 for system txn in the beginning of block
	return storageRangeAt(tx, contractAddress, keyStart, fromTxNum, maxResult)
}

// AccountRange implements debug_accountRange. Returns a range of accounts involved in the given block rangeb
func (api *PrivateDebugAPIImpl) AccountRange(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, startKey []byte, maxResults int, excludeCode, excludeStorage bool) (state.IteratorDump, error) {
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

	dumper := state.NewDumper(tx, rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader)), blockNumber)
	res, err := dumper.IteratorDump(excludeCode, excludeStorage, common.BytesToAddress(startKey), maxResults)
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
func (api *PrivateDebugAPIImpl) GetModifiedAccountsByNumber(ctx context.Context, startNumber rpc.BlockNumber, endNumber *rpc.BlockNumber) ([]common.Address, error) {
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
		endNum = uint64(endNumber.Int64()) + 1
	}

	// is endNum too big?
	if endNum > latestBlock {
		return nil, fmt.Errorf("end block (%d) is later than the latest block (%d)", endNum, latestBlock)
	}

	if startNum > endNum {
		return nil, fmt.Errorf("start block (%d) must be less than or equal to end block (%d)", startNum, endNum)
	}
	//[from, to)
	startTxNum, err := api._txNumReader.Min(tx, startNum)
	if err != nil {
		return nil, err
	}
	endTxNum, err := api._txNumReader.Min(tx, endNum)
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
func (api *PrivateDebugAPIImpl) GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error) {
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
		endNum, err = api.headerNumberByHash(ctx, tx, *endHash)
		if err != nil {
			return nil, fmt.Errorf("end block %x not found", *endHash)
		}
		endNum = endNum + 1

	}

	if startNum > endNum {
		return nil, fmt.Errorf("start block (%d) must be less than or equal to end block (%d)", startNum, endNum)
	}

	//[from, to)
	startTxNum, err := api._txNumReader.Min(tx, startNum)
	if err != nil {
		return nil, err
	}
	endTxNum, err := api._txNumReader.Min(tx, endNum)
	if err != nil {
		return nil, err
	}
	return getModifiedAccounts(tx, startTxNum, endTxNum-1)
}

func (api *PrivateDebugAPIImpl) AccountAt(ctx context.Context, blockHash common.Hash, txIndex uint64, address common.Address) (*AccountResult, error) {
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

	minTxNum, err := api._txNumReader.Min(tx, header.Number.Uint64())
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
	result.CodeHash = a.CodeHash

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
func (api *PrivateDebugAPIImpl) GetRawHeader(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	n, h, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	header, err := api._blockReader.Header(ctx, tx, h, n)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, errors.New("header not found")
	}
	return rlp.EncodeToBytes(header)
}

// Implements debug_getRawBlock - Returns an RLP-encoded block
func (api *PrivateDebugAPIImpl) GetRawBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	n, h, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, h, n)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, errors.New("block not found")
	}
	return rlp.EncodeToBytes(block)
}

// GetRawReceipts implements debug_getRawReceipts - retrieves and returns an array of EIP-2718 binary-encoded receipts of a single block
func (api *PrivateDebugAPIImpl) GetRawReceipts(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) ([]hexutil.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, blockHash, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
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
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if chainConfig.Bor != nil {
		events, err := api.stateSyncEvents(ctx, tx, block.Hash(), blockNum, chainConfig)
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
func (api *PrivateDebugAPIImpl) GetBadBlocks(ctx context.Context) ([]map[string]interface{}, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blocks, err := rawdb.GetLatestBadBlocks(tx)
	if err != nil || len(blocks) == 0 {
		return nil, err
	}
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	results := make([]map[string]interface{}, 0, len(blocks))
	for _, block := range blocks {
		var blockRlp string
		if rlpBytes, err := rlp.EncodeToBytes(block); err != nil {
			blockRlp = err.Error() // hack
		} else {
			blockRlp = fmt.Sprintf("%#x", rlpBytes)
		}

		blockJson, err := ethapi.RPCMarshalBlock(block, true, true, nil, chainConfig.IsArbitrumNitro(block.Number()))
		if err != nil {
			log.Error("Failed to marshal block", "err", err)
			blockJson = map[string]interface{}{}
		}
		results = append(results, map[string]interface{}{
			"hash":  block.Hash(),
			"block": blockRlp,
			"rlp":   blockJson,
		})
	}

	return results, nil
}

// GetRawTransaction implements debug_getRawTransaction - Returns an array of EIP-2718 binary-encoded transactions
func (api *PrivateDebugAPIImpl) GetRawTransaction(ctx context.Context, txnHash common.Hash) (hexutil.Bytes, error) {
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

	// Private API returns 0 if transaction is not found.
	isBorStateSyncTx := blockNum == 0 && chainConfig.Bor != nil
	if isBorStateSyncTx {
		if api.useBridgeReader {
			blockNum, ok, err = api.bridgeReader.EventTxnLookup(ctx, txnHash)
		} else {
			blockNum, ok, err = api._blockReader.EventLookup(ctx, tx, txnHash)
		}

		if err != nil {
			return nil, err
		}
	}

	if !ok {
		return nil, nil
	}

	txNumMin, err := api._txNumReader.Min(tx, blockNum)
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
