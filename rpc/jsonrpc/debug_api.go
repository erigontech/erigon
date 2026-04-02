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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpchelper"
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
	SetHead(ctx context.Context, number hexutil.Uint64) error
	FreeOSMemory()
	SetGCPercent(v int) int
	SetMemoryLimit(limit int64) int64
	GcStats() *debug.GCStats
	MemStats() *runtime.MemStats
}

// PrivateDebugAPIImpl is implementation of the PrivateDebugAPI interface based on remote Db access
type DebugAPIImpl struct {
	*BaseAPI
	db                kv.TemporalRoDB
	ethBackend        rpchelper.ApiBackend
	GasCap            uint64
	gethCompatibility bool // Geth-compatible storage iteration order for debug_storageRangeAt
}

// NewPrivateDebugAPI returns PrivateDebugAPIImpl instance
func NewPrivateDebugAPI(base *BaseAPI, db kv.TemporalRoDB, ethBackend rpchelper.ApiBackend, gascap uint64, gethCompatibility bool) *DebugAPIImpl {
	return &DebugAPIImpl{
		BaseAPI:           base,
		db:                db,
		ethBackend:        ethBackend,
		GasCap:            gascap,
		gethCompatibility: gethCompatibility,
	}
}

// SetHead implements debug_setHead. Rewinds the local chain to the specified block number.
func (api *DebugAPIImpl) SetHead(ctx context.Context, number hexutil.Uint64) error {
	blockNum := number.Uint64()

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	currentHead, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return err
	}
	if blockNum > currentHead {
		return fmt.Errorf("block number %d is in the future: current head is %d", blockNum, currentHead)
	}

	if err := api.BaseAPI.checkPruneHistory(ctx, tx, blockNum); err != nil {
		return err
	}

	tx.Rollback() // release read tx before the backend opens write tx

	_, err = api.ethBackend.SetHead(ctx, &remoteproto.SetHeadRequest{BlockNumber: blockNum})
	return err
}

// StorageRangeAt implements debug_storageRangeAt. Returns information about a range of storage locations (if any) for the given address.
func (api *DebugAPIImpl) StorageRangeAt(ctx context.Context, blockHash common.Hash, txIndex uint64, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int) (StorageRangeResult, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer tx.Rollback()

	blockNrOrHash := rpc.BlockNumberOrHashWithHash(blockHash, true)
	blockNumber, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return StorageRangeResult{}, nil
		}
		return StorageRangeResult{}, err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNumber)
	if err != nil {
		return StorageRangeResult{}, err
	}

	minTxNum, err := api._txNumReader.Min(ctx, tx, blockNumber)
	if err != nil {
		return StorageRangeResult{}, err
	}

	fromTxNum := minTxNum + txIndex + 1 //+1 for system txn in the beginning of block
	return storageRangeAt(tx, contractAddress, keyStart, fromTxNum, maxResult, api.gethCompatibility)
}

// AccountRange implements debug_accountRange. Returns a paginated list of all accounts present in the state at the given block.
// To ensure compatibility, we've temporarily added support for the start parameter in two formats:
// - string (e.g., "0x..."), which is used by Geth and other APIs (i.e debug_storageRangeAt).
// - []byte, which was used in Erigon.
// Deprecation of []byte format: The []byte format is now deprecated and will be removed in a future release.
//
// New optional parameter incompletes: This parameter has been added for compatibility with Geth. It is currently not supported when set to true(as its functionality is specific to the Geth protocol).
//
// Note: Geth returns all accounts at the given block starting from `start`, where `start` is a
// keccak256(address) hash. Geth can seek directly to that position in the Merkle Patricia Trie
// since the trie is natively indexed by keccak256. In Erigon, accounts are stored by raw address
// (flat storage), so to match Geth's behaviour we would need to compute keccak256 for every account
// in order to find the one matching `start` — which is too expensive for production use.
// As a result, Erigon treats `start` as a raw address and iteration order differs from Geth.
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

	} else if _, ok := blockNrOrHash.Hash(); ok {
		bn, _, _, err2 := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
		if err2 != nil {
			return state.IteratorDump{}, err2
		}
		blockNumber = bn
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
		res.Root = fmt.Sprintf("%x", header.Root)
	}

	return res, nil
}

// GetModifiedAccountsByNumber implements debug_getModifiedAccountsByNumber.
//
// Geth semantics:
//   - single param N:     returns accounts modified in block N
//     (equivalent to comparing state root of N-1 vs N)
//   - two params (S, E]:  returns accounts modified in blocks S+1 … E
//     (equivalent to comparing state root of S vs E)
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

	if endNumber == nil {
		// Single param: cover exactly block startNum.
		if err = api.BaseAPI.checkPruneHistory(ctx, tx, startNum); err != nil {
			return nil, err
		}
		startTxNum, err := api._txNumReader.Min(ctx, tx, startNum)
		if err != nil {
			return nil, err
		}
		endTxNum, err := api._txNumReader.Max(ctx, tx, startNum)
		if err != nil {
			return nil, err
		}
		return getModifiedAccounts(tx, startTxNum, endTxNum+1)
	}

	// Two params: Geth compares state at startNum vs endNum → blocks (startNum, endNum].
	endNum := uint64(endNumber.Int64()) // forces negative numbers to fail (too large)
	if endNum > latestBlock {
		return nil, fmt.Errorf("end block (%d) is later than the latest block (%d)", endNum, latestBlock)
	}
	if startNum >= endNum {
		return nil, fmt.Errorf("start block (%d) must be less than end block (%d)", startNum, endNum)
	}

	// Checking startNum+1 is sufficient under sequential-pruning semantics: if block N is
	// available, all blocks > N are too. If pruning semantics ever change this would need
	// to also check endNum.
	if err = api.BaseAPI.checkPruneHistory(ctx, tx, startNum+1); err != nil {
		return nil, err
	}

	// startNum is the exclusive lower bound: first txNum is Max(startNum)+1.
	startTxNum, err := api._txNumReader.Max(ctx, tx, startNum)
	if err != nil {
		return nil, err
	}
	startTxNum++ // exclusive: first txNum belonging to startNum+1
	endTxNum, err := api._txNumReader.Max(ctx, tx, endNum)
	if err != nil {
		return nil, err
	}
	return getModifiedAccounts(tx, startTxNum, endTxNum+1)
}

// getModifiedAccounts returns a list of addresses that were modified in the txNum range
// [startTxNum, endTxNum) (endTxNum is exclusive).
//
// # Why we need two domain passes (Erigon vs Geth architecture)
//
// Geth uses a Merkle Patricia Trie (MPT). Every storage write changes the storageRoot field
// inside the account's trie node, so a trie diff between two state roots automatically surfaces
// ALL modified accounts — including contracts whose only change was a storage slot write.
//
// Erigon uses a flat key/value model with two separate domains:
//   - AccountsDomain: balance, nonce, code hash (one record per address)
//   - StorageDomain:  storage slots (one record per address+slot)
//
// A contract that writes to storage without changing balance/nonce/code produces history
// entries only in StorageDomain, not in AccountsDomain. Querying AccountsDomain alone would
// miss those contracts. We therefore perform two passes and union the results.
//
// # Filters applied to match Geth output exactly
//
//  1. Precompiles touched but not changed (e.g. 0x0000…0004 SHA256):
//     AccountsDomain records a "touch" but balance/nonce/code did not change.
//     Filter: bytes.Equal(preVal, postVal) → skip.
//
//  2. Self-destructed accounts (SELFDESTRUCT / EIP-161 zeroing):
//     Geth walks the NEW state trie; destroyed accounts are absent from it and are
//     never emitted. In Erigon, AccountsDomain records the deletion as postVal == empty.
//     Filter: len(postVal) == 0 → record in deletedAddrs, skip from result.
//
//  3. Storage slots of deleted accounts:
//     After SELFDESTRUCT all storage is cleared. Those slot entries appear in StorageDomain
//     history but the owning account no longer exists. We skip them via deletedAddrs.
//     No extra GetAsOf(Account) is needed: a destroyed account cannot run code, so it
//     cannot produce new StorageDomain entries. The only re-use case (CREATE2 at the same
//     address) causes an AccountsDomain entry (code hash changes) → address lands in `saw`
//     and is skipped by the "already in result" fast-path.
//
// # Two-pass approach
//
//  1. AccountsDomain pass: collect modified accounts; build deletedAddrs for SELFDESTRUCT'd ones.
//  2. StorageDomain pass: add any address with a net storage change that is not already included
//     and not deleted. Filters are applied cheapest-first to minimise GetAsOf I/O calls.
func getModifiedAccounts(tx kv.TemporalTx, startTxNum, endTxNum uint64) ([]common.Address, error) {
	saw := make(map[common.Address]struct{})
	var result []common.Address

	const addrLen = len(common.Address{})
	addAddr := func(k []byte) {
		addr := common.BytesToAddress(k[:addrLen])
		if _, ok := saw[addr]; !ok {
			saw[addr] = struct{}{}
			result = append(result, addr)
		}
	}

	// Pass 1 – AccountsDomain.
	// HistoryRange returns one entry per unique key: (key, value_before_first_change_in_range).
	// Because the first change's pre-value equals the state at startTxNum-1 (no intervening
	// change), this IS the pre-range value. GetAsOf(key, endTxNum) gives the post-range value.
	// Comparing the two detects net changes regardless of intermediate modifications.
	//   • preVal == postVal  → no net change (e.g. precompile touched but not modified): skip.
	//   • len(postVal) == 0  → account deleted (SELFDESTRUCT / EIP-161): record in deletedAddrs.
	// Geth compares two state roots by trie walk, so deleted accounts never appear in its output.
	deletedAddrs := make(map[common.Address]struct{})
	accIt, err := tx.HistoryRange(kv.AccountsDomain, int(startTxNum), int(endTxNum), order.Asc, kv.Unlim)
	if err != nil {
		return nil, err
	}
	defer accIt.Close()
	for accIt.HasNext() {
		k, preVal, err := accIt.Next()
		if err != nil {
			return nil, err
		}
		// ok==false (key not found at endTxNum) yields postVal==nil, so len(postVal)==0.
		// That correctly maps to "deleted" — a key absent from the end state was removed.
		postVal, _, err := tx.GetAsOf(kv.AccountsDomain, k, endTxNum)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(preVal, postVal) {
			continue // no net change
		}
		if len(postVal) == 0 {
			deletedAddrs[common.BytesToAddress(k[:addrLen])] = struct{}{}
			continue // deleted: Geth excludes these
		}
		addAddr(k)
	}

	// Pass 2 – StorageDomain (key = address 20B + slot 32B).
	// In Geth, every storage write changes storageRoot in the account record, so storage-only
	// modified contracts appear automatically in the trie diff. In Erigon's flat model we must
	// check StorageDomain separately.
	//
	// Filters (in cheapest-first order to minimise GetAsOf calls):
	//   1. Already in result → skip immediately (no I/O).
	//   2. In deletedAddrs  → skip immediately (no I/O).
	//   3. GetAsOf(Storage) to detect net no-change slots (the only remaining I/O per slot).
	//
	// We do NOT need GetAsOf(Account) here: a destroyed account cannot execute code, so it
	// cannot produce storage history entries. The only exception — CREATE2 re-deployment at
	// the same address — causes an AccountsDomain entry (code hash changes) and thus the
	// address would already be in `saw` (filter 1).
	storIt, err := tx.HistoryRange(kv.StorageDomain, int(startTxNum), int(endTxNum), order.Asc, kv.Unlim)
	if err != nil {
		return nil, err
	}
	defer storIt.Close()
	for storIt.HasNext() {
		k, preVal, err := storIt.Next()
		if err != nil {
			return nil, err
		}
		if len(k) < addrLen {
			continue
		}
		addr := common.BytesToAddress(k[:addrLen])
		if _, ok := saw[addr]; ok {
			continue // already in result
		}
		if _, ok := deletedAddrs[addr]; ok {
			continue // account deleted within range
		}
		postVal, _, err := tx.GetAsOf(kv.StorageDomain, k, endTxNum)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(preVal, postVal) {
			continue // no net change for this slot
		}
		addAddr(k)
	}

	return result, nil
}

// GetModifiedAccountsByHash implements debug_getModifiedAccountsByHash.
// Same Geth semantics as GetModifiedAccountsByNumber: single hash = block N,
// two hashes = accounts modified in (startHash, endHash].
func (api *DebugAPIImpl) GetModifiedAccountsByHash(ctx context.Context, startHash common.Hash, endHash *common.Hash) ([]common.Address, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	latestBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return nil, err
	}

	startNum, err := api.headerNumberByHash(ctx, tx, startHash)
	if err != nil {
		return nil, fmt.Errorf("start block %x not found", startHash)
	}

	if endHash == nil {
		// Single param: cover exactly block startNum.
		if err = api.BaseAPI.checkPruneHistory(ctx, tx, startNum); err != nil {
			return nil, err
		}
		startTxNum, err := api._txNumReader.Min(ctx, tx, startNum)
		if err != nil {
			return nil, err
		}
		endTxNum, err := api._txNumReader.Max(ctx, tx, startNum)
		if err != nil {
			return nil, err
		}
		return getModifiedAccounts(tx, startTxNum, endTxNum+1)
	}

	// Two params: Geth compares state at startNum vs endNum → blocks (startNum, endNum].
	endNum, err := api.headerNumberByHash(ctx, tx, *endHash)
	if err != nil {
		return nil, fmt.Errorf("end block %x not found", *endHash)
	}
	if endNum > latestBlock {
		return nil, fmt.Errorf("end block (%d) is later than the latest block (%d)", endNum, latestBlock)
	}
	if startNum >= endNum {
		return nil, fmt.Errorf("start block (%d) must be less than end block (%d)", startNum, endNum)
	}

	// Checking startNum+1 is sufficient under sequential-pruning semantics: if block N is
	// available, all blocks > N are too. If pruning semantics ever change this would need
	// to also check endNum.
	if err = api.BaseAPI.checkPruneHistory(ctx, tx, startNum+1); err != nil {
		return nil, err
	}

	startTxNum, err := api._txNumReader.Max(ctx, tx, startNum)
	if err != nil {
		return nil, err
	}
	startTxNum++ // exclusive: first txNum belonging to startNum+1
	endTxNum, err := api._txNumReader.Max(ctx, tx, endNum)
	if err != nil {
		return nil, err
	}
	return getModifiedAccounts(tx, startTxNum, endTxNum+1)
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
	if header == nil {
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
			"block": blockJson,
			"rlp":   blockRlp,
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
