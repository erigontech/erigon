package rpchelper

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
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

func GetBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (uint64, common.Hash, bool, error) {
	return _GetBlockNumber(blockNrOrHash.RequireCanonical, blockNrOrHash, tx, filters)
}

func GetCanonicalBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (uint64, common.Hash, bool, error) {
	return _GetBlockNumber(true, blockNrOrHash, tx, filters)
}

func _GetBlockNumber(requireCanonical bool, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (blockNumber uint64, hash common.Hash, latest bool, err error) {
	// Due to changed semantics of `lastest` block in RPC request, it is now distinct
	// from the block block number corresponding to the plain state
	var plainStateBlockNumber uint64
	if plainStateBlockNumber, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
		return 0, common.Hash{}, false, fmt.Errorf("getting plain state block number: %w", err)
	}
	var ok bool
	hash, ok = blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		switch number {
		case rpc.LatestBlockNumber:
			if blockNumber, err = GetLatestBlockNumber(tx); err != nil {
				return 0, common.Hash{}, false, err
			}
		case rpc.EarliestBlockNumber:
			blockNumber = 0
		case rpc.FinalizedBlockNumber:
			blockNumber, err = GetFinalizedBlockNumber(tx)
			if err != nil {
				return 0, common.Hash{}, false, err
			}
		case rpc.SafeBlockNumber:
			blockNumber, err = GetSafeBlockNumber(tx)
			if err != nil {
				return 0, common.Hash{}, false, err
			}
		case rpc.PendingBlockNumber:
			pendingBlock := filters.LastPendingBlock()
			if pendingBlock == nil {
				blockNumber = plainStateBlockNumber
			} else {
				return pendingBlock.NumberU64(), pendingBlock.Hash(), false, nil
			}
		default:
			blockNumber = uint64(number.Int64())
		}
		hash, err = rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, false, err
		}
	} else {
		number := rawdb.ReadHeaderNumber(tx, hash)
		if number == nil {
			return 0, common.Hash{}, false, fmt.Errorf("block %x not found", hash)
		}
		blockNumber = *number

		ch, err := rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, false, err
		}
		if requireCanonical && ch != hash {
			return 0, common.Hash{}, false, nonCanonocalHashError{hash}
		}
	}
	return blockNumber, hash, blockNumber == plainStateBlockNumber, nil
}

func GetAccount(tx kv.Tx, blockNumber uint64, address common.Address) (*accounts.Account, error) {
	reader := adapter.NewStateReader(tx, blockNumber)
	return reader.ReadAccountData(address)
}

func CreateStateReader(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash, filters *Filters, stateCache kvcache.Cache) (state.StateReader, error) {
	blockNumber, _, latest, err := _GetBlockNumber(true, blockNrOrHash, tx, filters)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if latest {
		cacheView, err := stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateReader = state.NewCachedReader2(cacheView, tx)
	} else {
		stateReader = state.NewPlainState(tx, blockNumber+1)
	}
	return stateReader, nil
}
