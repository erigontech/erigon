package rpchelper

import (
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/rpc"
)

var UnknownBlockError = &rpc.CustomError{
	Code:    -39001,
	Message: "Unknown block",
}

func GetLatestBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceHeadHash := rawdb.ReadForkchoiceHead(tx)
	if forkchoiceHeadHash != (libcommon.Hash{}) {
		forkchoiceHeadNum := rawdb.ReadHeaderNumber(tx, forkchoiceHeadHash)
		if forkchoiceHeadNum != nil {
			return *forkchoiceHeadNum, nil
		}
	}

	blockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, fmt.Errorf("getting latest block number: %w", err)
	}

	return blockNum, nil
}

func GetFinalizedBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceFinalizedHash := rawdb.ReadForkchoiceFinalized(tx)
	if forkchoiceFinalizedHash != (libcommon.Hash{}) {
		forkchoiceFinalizedNum := rawdb.ReadHeaderNumber(tx, forkchoiceFinalizedHash)
		if forkchoiceFinalizedNum != nil {
			return *forkchoiceFinalizedNum, nil
		}
	}

	return 0, UnknownBlockError
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
