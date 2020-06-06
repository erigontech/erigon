package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func DownloadHeaders(s *StageState, d DownloaderGlue, stateDB ethdb.Database, headersFetchers []func() error, datadir string, quitCh chan struct{}) error {
	err := d.SpawnSync(headersFetchers)
	if err != nil {
		return err
	}

	log.Info("Checking for unwinding...")
	// Check unwinds backwards and if they are outstanding, invoke corresponding functions
	for stage := stages.Finish - 1; stage > stages.Headers; stage-- {
		unwindPoint, err := stages.GetStageUnwind(stateDB, stage)
		if err != nil {
			return err
		}

		if unwindPoint == 0 {
			continue
		}

		switch stage {
		case stages.Bodies:
			err = unwindBodyDownloadStage(stateDB, unwindPoint)
		case stages.Senders:
			err = unwindSendersStage(stateDB, unwindPoint)
		case stages.Execution:
			err = unwindExecutionStage(unwindPoint, stateDB)
		case stages.HashCheck:
			err = unwindHashCheckStage(unwindPoint, stateDB, datadir, quitCh)
		case stages.AccountHistoryIndex:
			err = unwindAccountHistoryIndex(unwindPoint, stateDB, core.UsePlainStateExecution, quitCh)
		case stages.StorageHistoryIndex:
			err = unwindStorageHistoryIndex(unwindPoint, stateDB, core.UsePlainStateExecution, quitCh)
		default:
			return fmt.Errorf("unrecognized stage for unwinding: %d", stage)
		}

		if err != nil {
			return fmt.Errorf("error unwinding stage: %d: %w", stage, err)
		}
	}
	log.Info("Checking for unwinding... Complete!")

	s.Done()
	return nil
}
