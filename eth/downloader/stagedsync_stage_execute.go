package downloader

import (
	"fmt"
	//"os"
	//"runtime/pprof"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/log"
)

func (d *Downloader) spawnExecuteBlocksStage() (uint64, error) {
	lastProcessedBlockNumber, err := GetStageProgress(d.stateDB, Execution)
	if err != nil {
		return 0, err
	}

	nextBlockNumber := lastProcessedBlockNumber + 1

	/*
	profileNumber := nextBlockNumber
	f, err := os.Create(fmt.Sprintf("cpu-%d.prof", profileNumber))
	if err != nil {
		log.Error("could not create CPU profile", "error", err)
		return lastProcessedBlockNumber, err
	}
	if err1 := pprof.StartCPUProfile(f); err1 != nil {
		log.Error("could not start CPU profile", "error", err1)
		return lastProcessedBlockNumber, err
	}
	*/

	mutation := d.stateDB.NewBatch()
	defer func() {
		_, dbErr := mutation.Commit()
		if dbErr != nil {
			log.Error("Sync (Execution): failed to write db commit", "err", dbErr)
		}
	}()

	chainConfig := d.blockchain.Config()
	engine := d.blockchain.Engine()
	for {
		block := d.blockchain.GetBlockByNumber(nextBlockNumber)
		if block == nil {
			fmt.Printf("block %d nil\n", nextBlockNumber)
			break
		}

		stateReader := state.NewDbStateReader(mutation)
		stateWriter := state.NewDbStateWriter(mutation, nextBlockNumber)

		if nextBlockNumber%1000 == 0 {
			log.Info("Executed blocks:", "blockNumber", nextBlockNumber)
		}

		// where the magic happens
		err = core.ExecuteBlockEuphemerally(chainConfig, d.blockchain, engine, block, stateReader, stateWriter)
		if err != nil {
			return 0, err
		}

		if err = SaveStageProgress(mutation, Execution, nextBlockNumber); err != nil {
			return 0, err
		}
		fmt.Printf("Executed blocks: %d\n",nextBlockNumber)
		nextBlockNumber++

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if _, err = mutation.Commit(); err != nil {
				return 0, err
			}
			mutation = d.stateDB.NewBatch()
		}
		/*
		if nextBlockNumber-profileNumber == 100000 {
			// Flush the profiler
			pprof.StopCPUProfile()
		}
		*/
	}

	return nextBlockNumber - 1 /* the last processed block */, nil
}

func (d *Downloader) unwindExecutionStage(unwindPoint uint64) error {
	return fmt.Errorf("unwindExecutionStage not implemented")
}
