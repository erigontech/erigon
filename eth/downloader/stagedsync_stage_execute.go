package downloader

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/log"
)

func (d *Downloader) spawnExecuteBlocksStage() (uint64, error) {
	lastProcessedBlockNumber, err := GetStageProgress(d.stateDB, Execution)
	if err != nil {
		return 0, err
	}

	nextBlockNumber := lastProcessedBlockNumber + 1

	mutation := d.stateDB.NewBatch()
	defer func() {
		_, dbErr := mutation.Commit()
		if dbErr != nil {
			log.Error("Sync (Execution): failed to write db commit", "err", dbErr)
		}
	}()

	var incarnationMap = make(map[common.Address]uint64)

	for {
		block := d.blockchain.GetBlockByNumber(nextBlockNumber)
		if block == nil {
			break
		}

		stateReader := state.NewDbStateReader(mutation)
		stateWriter := state.NewDbStateWriter(mutation, nextBlockNumber, incarnationMap)

		if nextBlockNumber%1000 == 0 {
			log.Info("Executed blocks:", "blockNumber", nextBlockNumber)
		}

		// where the magic happens
		err = d.blockchain.ExecuteBlockEuphemerally(block, stateReader, stateWriter)
		if err != nil {
			return 0, err
		}

		if err = SaveStageProgress(mutation, Execution, nextBlockNumber); err != nil {
			return 0, err
		}

		nextBlockNumber++

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if _, err = mutation.Commit(); err != nil {
				return 0, err
			}
			mutation = d.stateDB.NewBatch()
			incarnationMap = make(map[common.Address]uint64)
		}
	}

	return nextBlockNumber - 1 /* the last processed block */, nil
}
