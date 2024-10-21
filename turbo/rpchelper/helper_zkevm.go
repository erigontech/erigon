package rpchelper

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
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
