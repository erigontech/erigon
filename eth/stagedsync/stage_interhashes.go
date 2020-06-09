package stagedsync

import (
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

//nolint:interfacer
func SpawnIntermediateHashesStage(s *StageState, stateDB ethdb.Database, _ string, _ chan struct{}) error {
	if prof {
		f, err := os.Create("cpu6.prof")
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return err
		}
		defer f.Close()
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return err
		}
		defer pprof.StopCPUProfile()
	}

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
