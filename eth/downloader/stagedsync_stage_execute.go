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
	defer func() {
		_, dbErr := mutation.Commit()
		if dbErr != nil {
			log.Error("Sync (Execution): failed to write db commit", "err", dbErr)
		}
	}()

	var incarnationMap = make(map[common.Address]uint64)

	for {
		block := d.blockchain.GetBlockByNumber(currentBlockNumber)
		if block == nil {
			break
		}

		stateReader := state.NewDbStateReader(mutation)
		stateWriter := state.NewDbStateWriter(mutation, currentBlockNumber, incarnationMap)

		if currentBlockNumber%1000 == 0 {
			log.Info("Executed blocks:", "blockNumber", currentBlockNumber)
		}

		// where the magic happens
		err = d.blockchain.ExecuteBlockEuphemerally(block, stateReader, stateWriter)
		if err != nil {
			return err
		}

		if err = SaveStageProgress(mutation, Execution, currentBlockNumber); err != nil {
			return err
		}

		currentBlockNumber++

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if _, err = mutation.Commit(); err != nil {
				return err
			}
			mutation = d.stateDB.NewBatch()
			incarnationMap = make(map[common.Address]uint64)
		}
	}
	return nil
}
