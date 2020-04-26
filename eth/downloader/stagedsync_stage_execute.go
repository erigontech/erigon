package downloader

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/log"
)

func (d *Downloader) spawnExecuteBlocksStage() error {
	origin, err := GetStageProgress(d.stateDB, Execution)
	if err != nil {
		return err
	}

	currentBlockNumber := origin + 1

	mutation := d.stateDB.NewBatch()

	for {
		block := d.blockchain.GetBlockByNumber(currentBlockNumber)
		if block == nil {
			break
		}

		stateReader := state.NewDbState(mutation, currentBlockNumber)
		stateWriter := state.NewDbStateWriter(mutation, currentBlockNumber)

		// where the magic happens
		if currentBlockNumber%1000 == 0 {
			log.Info("Executed blocks:", "blockNumber", currentBlockNumber)
		}
		err = d.blockchain.ExecuteBlockEuphemerally(block, stateReader, stateWriter)
		if err != nil {
			return err
		}

		SaveStageProgress(d.stateDB, Execution, currentBlockNumber)
		currentBlockNumber++

		if mutation.BatchSize() > 5000 {
			if _, err = mutation.Commit(); err != nil {
				return err
			}
			mutation = d.stateDB.NewBatch()
		}
	}
	return nil
}

func getBlockHashFromNumber(blockNumber uint64) common.Hash {
	return common.Hash{}
}
