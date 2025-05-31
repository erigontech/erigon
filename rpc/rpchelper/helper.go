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
	"errors"
	"fmt"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	borfinality "github.com/erigontech/erigon/polygon/bor/finality"
	"github.com/erigontech/erigon/polygon/bor/finality/whitelist"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

// unable to decode supplied params, or an invalid number of parameters
type nonCanonocalHashError struct{ hash common.Hash }

func (e nonCanonocalHashError) ErrorCode() int { return -32603 }

func (e nonCanonocalHashError) Error() string {
	return fmt.Sprintf("hash %x is not currently canonical", e.hash)
}

type BlockNotFoundErr struct {
	Hash common.Hash
}

func (e BlockNotFoundErr) Error() string {
	return fmt.Sprintf("block %x not found", e.Hash)
}

func GetBlockNumber(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (uint64, common.Hash, bool, error) {
	bn, bh, latest, _, err := _GetBlockNumber(ctx, blockNrOrHash.RequireCanonical, blockNrOrHash, tx, br, filters)
	return bn, bh, latest, err
}

func GetCanonicalBlockNumber(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (uint64, common.Hash, bool, error) {
	bn, bh, latest, _, err := _GetBlockNumber(ctx, blockNrOrHash.RequireCanonical, blockNrOrHash, tx, br, filters)
	return bn, bh, latest, err
}

func _GetBlockNumber(ctx context.Context, requireCanonical bool, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, br services.FullBlockReader, filters *Filters) (blockNumber uint64, hash common.Hash, latest bool, found bool, err error) {
	// Due to changed semantics of `lastest` block in RPC request, it is now distinct
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
			if whitelist.GetWhitelistingService() != nil {
				num := borfinality.GetFinalizedBlockNumber(tx)
				if num == 0 {
					// nolint
					return 0, common.Hash{}, false, false, errors.New("No finalized block")
				}

				blockNum := borfinality.CurrentFinalizedBlock(tx, num).NumberU64()
				blockHash := rawdb.ReadHeaderByNumber(tx, blockNum).Hash()
				return blockNum, blockHash, false, false, nil
			}
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
		if !ok { //future blocks must behave as "latest"
			return blockNumber, hash, blockNumber == plainStateBlockNumber, true, nil
		}
	} else {
		number, err := br.HeaderNumber(ctx, tx, hash)
		if err != nil {
			return 0, common.Hash{}, false, false, err
		}
		if number == nil {
			return 0, common.Hash{}, false, false, BlockNotFoundErr{Hash: hash}
		}
		blockNumber = *number

		ch, ok, err := br.CanonicalHash(ctx, tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, false, false, err
		}
		if requireCanonical && (!ok || ch != hash) {
			return 0, common.Hash{}, false, false, nonCanonocalHashError{hash}
		}
	}
	return blockNumber, hash, blockNumber == plainStateBlockNumber, true, nil
}

func CreateStateReader(ctx context.Context, tx kv.TemporalTx, br services.FullBlockReader, blockNrOrHash rpc.BlockNumberOrHash, txnIndex int, filters *Filters, stateCache kvcache.Cache, txNumReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	blockNumber, _, latest, _, err := _GetBlockNumber(ctx, true, blockNrOrHash, tx, br, filters)
	if err != nil {
		return nil, err
	}
	return CreateStateReaderFromBlockNumber(ctx, tx, blockNumber, latest, txnIndex, stateCache, txNumReader)
}

func CreateStateReaderFromBlockNumber(ctx context.Context, tx kv.TemporalTx, blockNumber uint64, latest bool, txnIndex int, stateCache kvcache.Cache, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	if latest {
		cacheView, err := stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		return CreateLatestCachedStateReader(cacheView, tx), nil
	}
	return CreateHistoryStateReader(tx, blockNumber+1, txnIndex, txNumsReader)
}

func CreateHistoryStateReader(tx kv.TemporalTx, blockNumber uint64, txnIndex int, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	r := state.NewHistoryReaderV3()
	r.SetTx(tx)
	//r.SetTrace(true)
	minTxNum, err := txNumsReader.Min(tx, blockNumber)
	if err != nil {
		return nil, err
	}
	txNum := uint64(int(minTxNum) + txnIndex + /* 1 system txNum in beginning of block */ 1)
	earliestTxNum := r.StateHistoryStartFrom()
	if txNum < earliestTxNum {
		// data available only starting from earliestTxNum, throw error to avoid unintended
		// consequences of using this StateReader
		return r, state.PrunedError
	}
	r.SetTxNum(txNum)
	return r, nil
}

func NewLatestStateReader(getter kv.TemporalGetter) state.StateReader {
	return state.NewReaderV3(getter)
}

func NewLatestStateWriter(tx kv.Tx, domains *libstate.SharedDomains, blockReader services.FullBlockReader, blockNum uint64) state.StateWriter {
	minTxNum, err := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(context.Background(), blockReader)).Min(tx, blockNum)
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
