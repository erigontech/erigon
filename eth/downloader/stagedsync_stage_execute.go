package downloader

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/log"
	"os"
	"runtime/pprof"
)

func (d *Downloader) spawnExecuteBlocksStage() (uint64, error) {
	lastProcessedBlockNumber, err := GetStageProgress(d.stateDB, Execution)
	if err != nil {
		return 0, err
	}

	nextBlockNumber := lastProcessedBlockNumber + 1

	profileNumber := nextBlockNumber
	f, err := os.Create(fmt.Sprintf("cpu-%d.prof", profileNumber))
	if err != nil {
		log.Error("could not create CPU profile", "error", err)
		return 0, err
	}
	if err1 := pprof.StartCPUProfile(f); err1 != nil {
		log.Error("could not start CPU profile", "error", err1)
		return 0, err
	}

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

		if nextBlockNumber-profileNumber == 100000 {
			// Flush the profiler
			pprof.StopCPUProfile()
		}
	}

	return nextBlockNumber - 1 /* the last processed block */, nil
}
