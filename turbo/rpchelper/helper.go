package rpchelper

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter"
)

// unable to decode supplied params, or an invalid number of parameters
type nonCanonocalHashError struct{ hash common.Hash }

func (e nonCanonocalHashError) ErrorCode() int { return -32603 }

func (e nonCanonocalHashError) Error() string {
	return fmt.Sprintf("hash %x is not currently canonical", e.hash)
}

func GetBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *filters.Filters) (uint64, common.Hash, error) {
	return _GetBlockNumber(blockNrOrHash.RequireCanonical, blockNrOrHash, tx, filters)
}

func GetCanonicalBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *filters.Filters) (uint64, common.Hash, error) {
	return _GetBlockNumber(true, blockNrOrHash, tx, filters)
}

func _GetBlockNumber(requireCanonical bool, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *filters.Filters) (uint64, common.Hash, error) {
	var blockNumber uint64
	var err error
	hash, ok := blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		if number == rpc.LatestBlockNumber {
			blockNumber, err = stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return 0, common.Hash{}, fmt.Errorf("getting latest block number: %w", err)
			}
		} else if number == rpc.EarliestBlockNumber {
			blockNumber = 0
		} else if number == rpc.PendingBlockNumber {
			pendingBlock := filters.LastPendingBlock()
			if pendingBlock == nil {
				blockNumber, err = stages.GetStageProgress(tx, stages.Execution)
				if err != nil {
					return 0, common.Hash{}, fmt.Errorf("getting latest block number: %w", err)
				}
			} else {
				return pendingBlock.NumberU64(), pendingBlock.Hash(), nil
			}
		} else {
			blockNumber = uint64(number.Int64())
		}
		hash, err = rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, err
		}
	} else {
		number := rawdb.ReadHeaderNumber(tx, hash)
		if number == nil {
			return 0, common.Hash{}, fmt.Errorf("block %x not found", hash)
		}
		blockNumber = *number

		ch, err := rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, err
		}
		if requireCanonical && ch != hash {
			return 0, common.Hash{}, nonCanonocalHashError{hash}
		}
	}
	return blockNumber, hash, nil
}

func GetAccount(tx kv.Tx, blockNumber uint64, address common.Address) (*accounts.Account, error) {
	reader := adapter.NewStateReader(tx, blockNumber)
	return reader.ReadAccountData(address)
}

func CreateStateReader(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash, blockNumber uint64, stateCache kvcache.Cache) (state.StateReader, error) {
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		cacheView, err := stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateReader = state.NewCachedReader2(cacheView, tx)
	} else {
		stateReader = state.NewPlainState(tx, blockNumber)
	}
	return stateReader, nil
}
