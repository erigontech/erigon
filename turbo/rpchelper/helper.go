package rpchelper

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
)

// unable to decode supplied params, or an invalid number of parameters
type nonCanonocalHashError struct{ hash libcommon.Hash }

func (e nonCanonocalHashError) ErrorCode() int { return -32603 }

func (e nonCanonocalHashError) Error() string {
	return fmt.Sprintf("hash %x is not currently canonical", e.hash)
}

func GetBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (uint64, libcommon.Hash, bool, error) {
	return _GetBlockNumber(blockNrOrHash.RequireCanonical, blockNrOrHash, tx, filters)
}

func GetCanonicalBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (uint64, libcommon.Hash, bool, error) {
	return _GetBlockNumber(true, blockNrOrHash, tx, filters)
}

func _GetBlockNumber(requireCanonical bool, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (blockNumber uint64, hash libcommon.Hash, latest bool, err error) {
	// Due to changed semantics of `lastest` block in RPC request, it is now distinct
	// from the block block number corresponding to the plain state
	var plainStateBlockNumber uint64
	if plainStateBlockNumber, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
		return 0, libcommon.Hash{}, false, fmt.Errorf("getting plain state block number: %w", err)
	}
	var ok bool
	hash, ok = blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		switch number {
		case rpc.LatestBlockNumber:
			if blockNumber, err = GetLatestBlockNumber(tx); err != nil {
				return 0, libcommon.Hash{}, false, err
			}
		case rpc.EarliestBlockNumber:
			blockNumber = 0
		case rpc.FinalizedBlockNumber:
			blockNumber, err = GetFinalizedBlockNumber(tx)
			if err != nil {
				return 0, libcommon.Hash{}, false, err
			}
		case rpc.SafeBlockNumber:
			blockNumber, err = GetSafeBlockNumber(tx)
			if err != nil {
				return 0, libcommon.Hash{}, false, err
			}
		case rpc.PendingBlockNumber:
			pendingBlock := filters.LastPendingBlock()
			if pendingBlock == nil {
				blockNumber = plainStateBlockNumber
			} else {
				return pendingBlock.NumberU64(), pendingBlock.Hash(), false, nil
			}
		case rpc.LatestExecutedBlockNumber:
			blockNumber = plainStateBlockNumber
		default:
			blockNumber = uint64(number.Int64())
		}
		hash, err = rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return 0, libcommon.Hash{}, false, err
		}
	} else {
		number := rawdb.ReadHeaderNumber(tx, hash)
		if number == nil {
			return 0, libcommon.Hash{}, false, fmt.Errorf("block %x not found", hash)
		}
		blockNumber = *number

		ch, err := rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return 0, libcommon.Hash{}, false, err
		}
		if requireCanonical && ch != hash {
			return 0, libcommon.Hash{}, false, nonCanonocalHashError{hash}
		}
	}
	return blockNumber, hash, blockNumber == plainStateBlockNumber, nil
}

func CreateStateReader(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash, txnIndex uint64, filters *Filters, stateCache kvcache.Cache, historyV3 bool, chainName string) (state.StateReader, error) {
	blockNumber, _, latest, err := _GetBlockNumber(true, blockNrOrHash, tx, filters)
	if err != nil {
		return nil, err
	}
	return CreateStateReaderFromBlockNumber(ctx, tx, blockNumber, latest, txnIndex, stateCache, historyV3, chainName)
}

func CreateStateReaderFromBlockNumber(ctx context.Context, tx kv.Tx, blockNumber uint64, latest bool, txnIndex uint64, stateCache kvcache.Cache, historyV3 bool, chainName string) (state.StateReader, error) {
	if latest {
		cacheView, err := stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		return state.NewCachedReader2(cacheView, tx), nil
	}
	return CreateHistoryStateReader(tx, blockNumber+1, txnIndex, historyV3, chainName)
}

func CreateHistoryStateReader(tx kv.Tx, blockNumber, txnIndex uint64, historyV3 bool, chainName string) (state.StateReader, error) {
	if !historyV3 {
		r := state.NewPlainState(tx, blockNumber, systemcontracts.SystemContractCodeLookup[chainName])
		//r.SetTrace(true)
		return r, nil
	}
	r := state.NewHistoryReaderV3()
	r.SetTx(tx)
	//r.SetTrace(true)
	minTxNum, err := rawdbv3.TxNums.Min(tx, blockNumber)
	if err != nil {
		return nil, err
	}
	r.SetTxNum(minTxNum + txnIndex + 1)
	return r, nil
}
