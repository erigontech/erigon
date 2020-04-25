package downloader

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
)

func (d *Downloader) spawnExecuteBlocksStage() error {
	origin, err := GetStageProgress(d.stateDB, Execution)
	if err != nil {
		return err
	}

	currentBlockNumber := origin + 1

	for {
		stateReader := state.NewDbState(d.stateDB, currentBlockNumber)
		stateWriter := state.NewDbStateWriter(d.stateDB, currentBlockNumber)

		block := d.blockchain.GetBlockByNumber(currentBlockNumber)
		if block == nil {
			break
		}

		// where the magic happens
		err = d.blockchain.ExecuteBlockEuphemerally(block, stateReader, stateWriter)
		if err != nil {
			return err
		}

		SaveStageProgress(d.stateDB, Execution, currentBlockNumber)
		currentBlockNumber++
	}
	return nil
}

func getBlockHashFromNumber(blockNumber uint64) common.Hash {
	return common.Hash{}
}
