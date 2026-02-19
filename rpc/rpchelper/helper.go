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

package rpchelper

import (
	"context"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
)

// unable to decode supplied params, or an invalid number of parameters
type nonCanonicalHashError struct{ hash common.Hash }

func (e nonCanonicalHashError) ErrorCode() int { return -32603 }

func (e nonCanonicalHashError) Error() string {
	return fmt.Sprintf("hash %x is not currently canonical", e.hash)
}

type BlockNotFoundErr struct {
	Hash common.Hash
}

func (e BlockNotFoundErr) Error() string {
	return fmt.Sprintf("block %x not found", e.Hash)
}

func CheckBlockExecuted(tx kv.Tx, blockNumber uint64) error {
	lastExecutedBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}

	if blockNumber > lastExecutedBlock {
		return fmt.Errorf("block %d is not executed (last executed: %d)", blockNumber, lastExecutedBlock)
	}

	return nil
}

func GetBlockNumber(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (uint64, common.Hash, bool, error) {
	bn, bh, latest, found, err := _GetBlockNumber(ctx, blockNrOrHash.RequireCanonical, blockNrOrHash, tx, br, filters)
	if err != nil {
		return 0, common.Hash{}, false, err
	}
	if !found {
		return bn, bh, latest, rpc.BlockNotFoundErr{BlockId: blockNrOrHash.String()}
	}
	return bn, bh, latest, err
}

func GetCanonicalBlockNumber(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (uint64, common.Hash, bool, error) {
	bn, bh, latest, _, err := _GetBlockNumber(ctx, true, blockNrOrHash, tx, br, filters)
	return bn, bh, latest, err
}

func _GetBlockNumber(ctx context.Context, requireCanonical bool, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (blockNumber uint64, hash common.Hash, latest bool, found bool, err error) {
	// Due to the changed semantics of `latest` block in RPC request, it is now distinct
	// from the block number corresponding to the plain state
	var plainStateBlockNumber uint64
	if plainStateBlockNumber, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
		return 0, common.Hash{}, false, false, fmt.Errorf("getting plain state block number: %w", err)
	}
	var ok bool
	hash, ok = blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		switch number {
		case rpc.LatestBlockNumber:
			if blockNumber, err = GetLatestBlockNumber(tx); err != nil {
				return 0, common.Hash{}, false, false, err
			}
		case rpc.EarliestBlockNumber:
			blockNumber = 0
		case rpc.FinalizedBlockNumber:
			blockNumber, err = GetFinalizedBlockNumber(tx)
			if err != nil {
				return 0, common.Hash{}, false, false, err
			}
		case rpc.SafeBlockNumber:
			blockNumber, err = GetSafeBlockNumber(tx)
			if err != nil {
				return 0, common.Hash{}, false, false, err
			}
		case rpc.PendingBlockNumber:
			pendingBlock := filters.LastPendingBlock()
			if pendingBlock == nil {
				blockNumber = plainStateBlockNumber
			} else {
				return pendingBlock.NumberU64(), pendingBlock.Hash(), false, true, nil
			}
		case rpc.LatestExecutedBlockNumber:
			blockNumber = plainStateBlockNumber
		default:
			blockNumber = uint64(number.Int64())
		}
		hash, ok, err = br.CanonicalHash(ctx, tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, false, false, err
		}
		if !ok {
			return blockNumber, hash, blockNumber == plainStateBlockNumber, false, nil
		}
	} else {
		number, err := br.HeaderNumber(ctx, tx, hash)
		if err != nil {
			return 0, common.Hash{}, false, false, err
		}
		if number == nil {
			return 0, common.Hash{}, false, false, rpc.BlockNotFoundErr{BlockId: blockNrOrHash.String()}
		}
		blockNumber = *number

		ch, ok, err := br.CanonicalHash(ctx, tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, false, false, err
		}
		if requireCanonical && (!ok || ch != hash) {
			return 0, common.Hash{}, false, false, nonCanonicalHashError{hash}
		}
	}
	return blockNumber, hash, blockNumber == plainStateBlockNumber, true, nil
}

func CreateStateReader(ctx context.Context, tx kv.TemporalTx, br services.FullBlockReader, blockNrOrHash rpc.BlockNumberOrHash, txnIndex int, filters *Filters, stateCache kvcache.Cache, txNumReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	blockNumber, _, latest, found, err := _GetBlockNumber(ctx, true, blockNrOrHash, tx, br, filters)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, rpc.BlockNotFoundErr{BlockId: blockNrOrHash.String()}
	}
	return CreateStateReaderFromBlockNumber(ctx, tx, blockNumber, latest, txnIndex, stateCache, txNumReader)
}

func CreateStateReaderFromBlockNumber(ctx context.Context, tx kv.TemporalTx, blockNumber uint64, latest bool, txnIndex int, stateCache kvcache.Cache, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	cacheView, err := stateCache.View(ctx, tx)
	if err != nil {
		return nil, err
	}
	if latest {
		return CreateLatestCachedStateReader(cacheView, tx), nil
	}
	return CreateHistoryCachedStateReader(ctx, cacheView, tx, blockNumber+1, txnIndex, txNumsReader)
}

func CreateHistoryStateReader(ctx context.Context, tx kv.TemporalTx, blockNumber uint64, txnIndex int, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	minTxNum, err := txNumsReader.Min(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}
	txNum := uint64(int(minTxNum) + txnIndex + /* 1 system txNum in beginning of block */ 1)
	if minHistoryTxNum := state.StateHistoryStartTxNum(tx); txNum < minHistoryTxNum {
		bn, _, _ := txNumsReader.FindBlockNum(ctx, tx, minHistoryTxNum)
		return nil, fmt.Errorf("%w: block tx: %d, min tx: %d last state history bn: %d", state.PrunedError, txNum, minHistoryTxNum, bn)
	}
	return state.NewHistoryReaderV3(tx, txNum), nil
}

func NewLatestStateReader(getter kv.TemporalGetter) state.StateReader {
	return state.NewReaderV3(getter)
}

func NewLatestStateWriter(tx kv.TemporalTx, domains *execctx.SharedDomains, blockReader services.FullBlockReader, blockNum uint64) state.StateWriter {
	minTxNum, err := blockReader.TxnumReader().Min(context.Background(), tx, blockNum)
	if err != nil {
		panic(err)
	}
	txNum := uint64(int(minTxNum) + /* 1 system txNum in beginning of block */ 1)
	domains.SetTxNum(txNum)
	return state.NewWriter(domains.AsPutDel(tx), nil, txNum)
}

func CreateLatestCachedStateReader(cache kvcache.CacheView, tx kv.TemporalTx) state.StateReader {
	return state.NewCachedReader3(cache, tx)
}

type asOfView interface {
	GetAsOf(key []byte, ts uint64) (v []byte, ok bool, err error)
}

func CreateHistoryCachedStateReader(ctx context.Context, cache kvcache.CacheView, tx kv.TemporalTx, blockNumber uint64, txnIndex int, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	asOfView, ok := cache.(asOfView)
	if !ok {
		return nil, fmt.Errorf("%T does not implement GetAsOf at: %s", cache, dbg.Stack())
	}
	minTxNum, err := txNumsReader.Min(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}
	txNum := uint64(int(minTxNum) + txnIndex + /* 1 system txNum in beginning of block */ 1)
	if minHistoryTxNum := state.StateHistoryStartTxNum(tx); txNum < minHistoryTxNum {
		return nil, fmt.Errorf("%w: block tx: %d, min tx: %d", state.PrunedError, txNum, minHistoryTxNum)
	}
	return &cachedHistoryReaderV3{asOfView, state.NewHistoryReaderV3(tx, txNum)}, nil
}

type cachedHistoryReaderV3 struct {
	cache  asOfView
	reader *state.HistoryReaderV3
}

func (hr *cachedHistoryReaderV3) SetTrace(trace bool, tracePrefix string) {
	hr.reader.SetTrace(trace, tracePrefix)
}

func (hr *cachedHistoryReaderV3) Trace() bool {
	return hr.reader.Trace()
}

func (hr *cachedHistoryReaderV3) TracePrefix() string {
	return hr.reader.TracePrefix()
}

func (hr *cachedHistoryReaderV3) GetTxNum() uint64 {
	return hr.reader.GetTxNum()
}

func (hr *cachedHistoryReaderV3) SetTxNum(txNum uint64) {
	hr.reader.SetTxNum(txNum)
}

func (hr *cachedHistoryReaderV3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addressValue := address.Value()
	enc, ok, err := hr.cache.GetAsOf(addressValue[:], hr.reader.GetTxNum())

	if err != nil {
		return nil, err
	}

	if ok {
		var a accounts.Account
		if err := accounts.DeserialiseV3(&a, enc); err != nil {
			return nil, fmt.Errorf("%sread account data (cache)(%x): %w", hr.TracePrefix(), address, err)
		}
		return &a, nil
	}

	return hr.reader.ReadAccountData(address)
}

// ReadAccountDataForDebug - is like ReadAccountData, but without adding key to `readList`.
// Used to get `prev` account balance
func (hr *cachedHistoryReaderV3) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return hr.ReadAccountData(address)
}

func (hr *cachedHistoryReaderV3) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addressValue := address.Value()
	keyValue := key.Value()
	enc, ok, err := hr.cache.GetAsOf(append(addressValue[:], keyValue[:]...), hr.reader.GetTxNum())
	if err != nil {
		return uint256.Int{}, false, err
	}
	if ok {
		var res uint256.Int
		(&res).SetBytes(enc)
		return res, ok, err
	}
	return hr.reader.ReadAccountStorage(address, key)
}

func (hr *cachedHistoryReaderV3) HasStorage(address accounts.Address) (bool, error) {
	return hr.reader.HasStorage(address)
}

func (hr *cachedHistoryReaderV3) ReadAccountCode(address accounts.Address) ([]byte, error) {
	return hr.reader.ReadAccountCode(address)
}

func (hr *cachedHistoryReaderV3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	return hr.reader.ReadAccountCodeSize(address)
}

func (hr *cachedHistoryReaderV3) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}
