package commands

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

func getBlockNumber(number rpc.BlockNumber, dbReader ethdb.Getter) (uint64, error) {
	var blockNum uint64
	var err error
	if number == rpc.LatestBlockNumber || number == rpc.PendingBlockNumber {
		blockNum, err = getLatestBlockNumber(dbReader)
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

func getLatestBlockNumber(dbReader ethdb.Getter) (uint64, error) {
	blockNum, _, err := stages.GetStageProgress(dbReader, stages.Execution)
	if err != nil {
		return 0, fmt.Errorf("getting latest block number: %v", err)
	}

	return blockNum, nil
}
