package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
)

func getBlockNumber(number rpc.BlockNumber, tx kv.Tx) (uint64, error) {
	var blockNum uint64
	var err error
	switch number {

	case rpc.LatestBlockNumber:
		blockNum, err = getLatestBlockNumber(tx)
		if err != nil {
			return 0, err
		}
	case rpc.PendingBlockNumber:
		blockNum, err = getLatestBlockNumber(tx)
		if err != nil {
			return 0, err
		}

	case rpc.EarliestBlockNumber:
		blockNum = 0

	case rpc.FinalizeBlockNumber:
		blockNum, err = getFinalizedBlockNumber(tx)
		if err != nil {
			return 0, err
		}

	case rpc.SafeBlockNumber:
		blockNum, err = getSafeBlockNumber(tx)
		if err != nil {
			return 0, err
		}

	default:
		blockNum = uint64(number.Int64())
	}

	return blockNum, nil
}

func getLatestBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceHeadHash := rawdb.ReadForkchoiceHead(tx)
	if forkchoiceHeadHash != (common.Hash{}) {
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

func getFinalizedBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceFinalizedHash := rawdb.ReadForkchoiceFinalized(tx)
	if forkchoiceFinalizedHash != (common.Hash{}) {
		forkchoiceFinalizedNum := rawdb.ReadHeaderNumber(tx, forkchoiceFinalizedHash)
		if forkchoiceFinalizedNum != nil {
			return *forkchoiceFinalizedNum, nil
		}
	}

	blockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, fmt.Errorf("getting latest finalized block number: %w", err)
	}

	// finalized block is the genesis block
	if blockNum-128 <= 0 {
	return 0, nil
	}

	return blockNum - 128, nil
}

func getSafeBlockNumber(tx kv.Tx) (uint64, error) {
	forkchoiceSafeHash := rawdb.ReadForkchoiceSafe(tx)
	if forkchoiceSafeHash != (common.Hash{}) {
		forkchoiceSafeNum := rawdb.ReadHeaderNumber(tx, forkchoiceSafeHash)
		if forkchoiceSafeNum != nil {
			return *forkchoiceSafeNum, nil
		}
	}
	// if we dont have a safe hash we return earliest
	return 0, nil
}
