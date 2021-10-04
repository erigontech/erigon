package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
)

func getBlockNumber(number rpc.BlockNumber, tx kv.Tx) (uint64, error) {
	var blockNum uint64
	var err error
	if number == rpc.LatestBlockNumber || number == rpc.PendingBlockNumber {
		blockNum, err = getLatestBlockNumber(tx)
		if err != nil {
			return 0, err
		}
	} else if number == rpc.EarliestBlockNumber {
		blockNum = 0
	} else {
		blockNum = uint64(number.Int64())
	}

	return blockNum, nil
}

func getLatestBlockNumber(tx kv.Tx) (uint64, error) {
	blockNum, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, fmt.Errorf("getting latest block number: %w", err)
	}

	return blockNum, nil
}
