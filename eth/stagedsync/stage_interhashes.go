package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

//nolint:interfacer
func SpawnIntermediateHashesStage(s *StageState, stateDB ethdb.Database, _ string, _ chan struct{}) error {
	lastProcessedBlockNumber := s.BlockNumber
	hashedStateBlockNumber := s.BlockNumber

	log.Info("Generating intermediate hashes (currently no-op)", "from", lastProcessedBlockNumber, "to", hashedStateBlockNumber)
	// TODO: Actual work goes here
	return s.DoneAndUpdate(stateDB, lastProcessedBlockNumber)
}

//nolint:interfacer
func unwindIntermediateHashesStage(u *UnwindState, _ *StageState, stateDB ethdb.Database, _ string, _ chan struct{}) error {
	if err := u.Done(stateDB); err != nil {
		return fmt.Errorf("unwind IntermediateHashes: reset: %w", err)
	}
	return nil
}
