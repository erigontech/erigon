package downloader

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
)

func (d *Downloader) spawnExecuteBlocksStage() error {
	fmt.Printf("execute block stage started\n")
	origin, err := GetStageProgress(d.stateDB, Execution)
	if err != nil {
		return err
	}

	currentBlockNumber := origin + 1

	for {
		block := d.blockchain.GetBlockByNumber(currentBlockNumber)
		if block == nil {
			break
		}

		stateReader := state.NewDbState(d.stateDB, currentBlockNumber)
		stateWriter := state.NewDbStateWriter(d.stateDB, currentBlockNumber)

		fmt.Printf("execute block: %v -> %x\n", currentBlockNumber, block.Hash())

		// where the magic happens
		err = d.blockchain.ExecuteBlockEuphemerally(block, stateReader, stateWriter)
		fmt.Printf("execute block: %v -result-> err=%v\n", currentBlockNumber, err)
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
