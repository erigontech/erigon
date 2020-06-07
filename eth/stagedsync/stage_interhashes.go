package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)


func SpawnIntermediateHashesStage(s *StageState, stateDB ethdb.Database, datadir string, quit chan struct{}) error {
	lastProcessedBlockNumber, err := stages.GetStageProgress(stateDB, stages.IntermediateHashes)
	if err != nil {
		return fmt.Errorf("IntermediateHashes: get stage progress: %w", err)
	}
	log.Info("Generating intermediate hashes (currently no-op)", "from", lastProcessedBlockNumber)
	if err = stages.SaveStageProgress(stateDB, stages.IntermediateHashes, 0); err != nil {
		return fmt.Errorf("IntermediateHashes: update: %w", err)
	}
	return nil
}

func unwindIntermediateHashesStage(unwindPoint uint64, stateDB ethdb.Database, datadir string, quit chan struct{}) error {
	lastProcessedBlockNumber, err := stages.GetStageProgress(stateDB, stages.IntermediateHashes)
	if err != nil {
		return fmt.Errorf("unwind IntermediateHashes: get stage progress: %w", err)
	}
	if unwindPoint >= lastProcessedBlockNumber {
		err = stages.SaveStageUnwind(stateDB, stages.IntermediateHashes, 0)
		if err != nil {
			return fmt.Errorf("unwind IntermediateHashes: reset: %w", err)
		}
		return nil
	}
	if err = stages.SaveStageUnwind(stateDB, stages.IntermediateHashes, 0); err != nil {
		return fmt.Errorf("unwind IntermediateHashes: reset: %w", err)
	}
	return nil
}
