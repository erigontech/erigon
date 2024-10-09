package rpchelper

import (
	"fmt"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

var UnknownBlockError = &rpc.CustomError{
	Code:    -39001,
	Message: "Unknown block",
}

func GetLatestBlockNumber(tx kv.Tx) (uint64, error) {
	fmt.Println("== GetLatestBlockNumber func ==")
	forkchoiceHeadHash := rawdb.ReadForkchoiceHead(tx)
	if forkchoiceHeadHash != (libcommon.Hash{}) {
		forkchoiceHeadNum := rawdb.ReadHeaderNumber(tx, forkchoiceHeadHash)
		if forkchoiceHeadNum != nil {
			return *forkchoiceHeadNum, nil
		}
	}

	// TODO: maybe change here - use finish stage
	blockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, fmt.Errorf("getting latest block number: %w", err)
	}

	return blockNum, nil
}

// TODO: Getting the latest finished block number
func GetLatestFinishedBlockNumber(tx kv.Tx) (uint64, error) {
	fmt.Println("== GetLatestFinishedBlockNumber ==")
	forkchoiceHeadHash := rawdb.ReadForkchoiceHead(tx)
	if forkchoiceHeadHash != (libcommon.Hash{}) {
		forkchoiceHeadNum := rawdb.ReadHeaderNumber(tx, forkchoiceHeadHash)
		if forkchoiceHeadNum != nil {
			return *forkchoiceHeadNum, nil
		}
	}

	blockNum, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return 0, fmt.Errorf("getting latest block number: %w", err)
	}

	return blockNum, nil
}

func GetFinalizedBlockNumber(tx kv.Tx) (uint64, error) {
	// get highest verified batch
	highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return 0, err
	}

	hermezDb := hermez_db.NewHermezDbReader(tx)
	// we've got the highest batch to execute to, now get it's highest block
	highestVerifiedBlock, _, err := hermezDb.GetHighestBlockInBatch(highestVerifiedBatchNo)
	if err != nil {
		return 0, err
	}

	// TODO: maybe change here - use finish stage
	finishedBlockNumber, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return 0, fmt.Errorf("getting latest finished block number: %w", err)
	}

	blockNumber := highestVerifiedBlock
	if finishedBlockNumber < blockNumber {
		blockNumber = finishedBlockNumber
	}

	return blockNumber, nil
}

func GetSafeBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceSafeHash := rawdb.ReadForkchoiceSafe(tx)
	if forkchoiceSafeHash != (libcommon.Hash{}) {
		forkchoiceSafeNum := rawdb.ReadHeaderNumber(tx, forkchoiceSafeHash)
		if forkchoiceSafeNum != nil {
			return *forkchoiceSafeNum, nil
		}
	}
	return 0, UnknownBlockError
}

func GetLatestExecutedBlockNumber(tx kv.Tx) (uint64, error) {
	blockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, err
	}
	return blockNum, err
}
