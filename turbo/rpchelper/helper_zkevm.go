package rpchelper

import (
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

// TODO: View and possibly edit
func GetBatchNumber(rpcBatchNumber rpc.BlockNumber, tx kv.Tx, filters *Filters) (batchNumber uint64, latest bool, err error) {
	// Due to changed semantics of `lastest` block in RPC request, it is now distinct
	// from the block block number corresponding to the plain state
	var (
		latestFinishedBlock   uint64
		plainStateBatchNumber uint64
	)
	hermezDb := hermez_db.NewHermezDbReader(tx)

	// get highest executed batch
	if latestFinishedBlock, err = stages.GetStageProgress(tx, stages.Finish); err != nil {
		return 0, false, fmt.Errorf("getting plain state block number: %w", err)
	}

	if plainStateBatchNumber, err = hermezDb.GetBatchNoByL2Block(latestFinishedBlock); err != nil {
		return 0, false, fmt.Errorf("getting plain state batch number: %w", err)
	}

	switch rpcBatchNumber {
	case rpc.EarliestBlockNumber:
		batchNumber = 0
	case rpc.FinalizedBlockNumber:
	case rpc.SafeBlockNumber:
		// [zkevm] safe not available, returns finilized instead
		// get highest verified batch
		if batchNumber, err = stages.GetStageProgress(tx, stages.L1VerificationsBatchNo); err != nil {
			return 0, false, fmt.Errorf("getting verified batch number: %w", err)
		}
	case rpc.PendingBlockNumber:
	case rpc.LatestBlockNumber:
	case rpc.LatestExecutedBlockNumber:
		batchNumber = plainStateBatchNumber
	default:
		batchNumber = uint64(rpcBatchNumber.Int64())
	}

	return batchNumber, batchNumber == plainStateBatchNumber, nil
}
