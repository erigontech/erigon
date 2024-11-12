package rpchelper

import (
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	borfinality "github.com/ledgerwatch/erigon/polygon/bor/finality"
	"github.com/ledgerwatch/erigon/polygon/bor/finality/whitelist"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/sequencer"
)

func GetBatchNumber(rpcBatchNumber rpc.BlockNumber, tx kv.Tx, filters *Filters) (batchNumber uint64, latest bool, err error) {
	// Due to changed semantics of `lastest` block in RPC request, it is now distinct
	// from the block block number corresponding to the plain state
	hermezDb := hermez_db.NewHermezDbReader(tx)

	switch rpcBatchNumber {
	case rpc.EarliestBlockNumber:
		batchNumber = 0
	case rpc.SafeBlockNumber, rpc.FinalizedBlockNumber:
		// [zkevm] safe not available, returns finilized instead
		// get highest verified batch
		if batchNumber, err = stages.GetStageProgress(tx, stages.L1VerificationsBatchNo); err != nil {
			return 0, false, fmt.Errorf("getting verified batch number: %w", err)
		}
	case rpc.LatestBlockNumber, rpc.LatestExecutedBlockNumber, rpc.PendingBlockNumber:
		// get highest finished batch
		latestFinishedBlock, err := stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
			return 0, false, fmt.Errorf("getting latest finished state block number: %w", err)
		}

		if batchNumber, err = hermezDb.GetBatchNoByL2Block(latestFinishedBlock); err != nil {
			return 0, false, fmt.Errorf("getting latest finished state batch number: %w", err)
		}

		latest = true
	default:
		batchNumber = uint64(rpcBatchNumber.Int64())
	}

	return batchNumber, latest, nil
}

func GetBlockNumber_zkevm(blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (uint64, libcommon.Hash, bool, error) {
	return _GetBlockNumber_zkevm(blockNrOrHash.RequireCanonical, blockNrOrHash, tx, filters)
}

func GetCanonicalBlockNumber_zkevm(blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (uint64, libcommon.Hash, bool, error) {
	return _GetBlockNumber_zkevm(true, blockNrOrHash, tx, filters)
}

func _GetBlockNumber_zkevm(requireCanonical bool, blockNrOrHash rpc.BlockNumberOrHash, tx kv.Tx, filters *Filters) (blockNumber uint64, hash libcommon.Hash, latest bool, err error) {
	blockFinalizationType := stages.Finish
	if sequencer.IsSequencer() {
		blockFinalizationType = stages.Execution
	}

	finishedBlockNumber, err := stages.GetStageProgress(tx, blockFinalizationType)
	if err != nil {
		return 0, libcommon.Hash{}, false, fmt.Errorf("getting finished block number: %w", err)
	}

	var ok bool
	hash, ok = blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		switch number {
		case rpc.LatestBlockNumber:
			if blockNumber, err = GetLatestFinishedBlockNumber(tx); err != nil {
				return 0, libcommon.Hash{}, false, err
			}
		case rpc.EarliestBlockNumber:
			blockNumber = 0
		case rpc.FinalizedBlockNumber:
			if whitelist.GetWhitelistingService() != nil {
				num := borfinality.GetFinalizedBlockNumber(tx)
				if num == 0 {
					// nolint
					return 0, libcommon.Hash{}, false, errors.New("No finalized block")
				}

				blockNum := borfinality.CurrentFinalizedBlock(tx, num).NumberU64()
				blockHash := rawdb.ReadHeaderByNumber(tx, blockNum).Hash()
				return blockNum, blockHash, false, nil
			}
			blockNumber, err = GetFinalizedBlockNumber(tx)
			if err != nil {
				return 0, libcommon.Hash{}, false, err
			}
		case rpc.SafeBlockNumber:
			// [zkevm] safe not available, returns finilized instead
			// blockNumber, err = GetSafeBlockNumber(tx)
			blockNumber, err = GetFinalizedBlockNumber(tx)
			if err != nil {
				return 0, libcommon.Hash{}, false, err
			}
		case rpc.PendingBlockNumber:
			pendingBlock := filters.LastPendingBlock()
			if pendingBlock == nil {
				blockNumber = finishedBlockNumber
			} else {
				return pendingBlock.NumberU64(), pendingBlock.Hash(), false, nil
			}
		case rpc.LatestExecutedBlockNumber:
			blockNumber, err = stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return 0, libcommon.Hash{}, false, fmt.Errorf("getting latest executed block number: %w", err)
			}
		default:
			blockNumber = uint64(number.Int64())
			if blockNumber > finishedBlockNumber {
				return 0, libcommon.Hash{}, false, fmt.Errorf("block with number %d not found", blockNumber)
			}
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
	return blockNumber, hash, blockNumber == finishedBlockNumber, nil
}
