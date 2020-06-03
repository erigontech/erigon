package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func DoStagedSyncWithFetchers(
	d DownloaderGlue,
	blockchain BlockChain,
	stateDB ethdb.Database,
	pid string,
	history bool,
	datadir string,
	quitCh chan struct{},
	headersFetchers []func() error,
) error {
	defer log.Info("Staged sync finished")

	stages := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState) error {
				return DownloadHeaders(s, d, stateDB, headersFetchers, quitCh)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState) error {
				return spawnBodyDownloadStage(s, d, pid)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState) error {
				return spawnRecoverSendersStage(s, stateDB, blockchain.Config(), quitCh)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Executing blocks w/o hash checks",
			ExecFunc: func(s *StageState) error {
				return SpawnExecuteBlocksStage(s, stateDB, blockchain, 0 /* limit (meaning no limit) */, quitCh)
			},
		},
		{
			ID:          stages.HashCheck,
			Description: "Validating final hash",
			ExecFunc: func(s *StageState) error {
				return SpawnCheckFinalHashStage(s, stateDB, datadir,  quitCh)
			},
		},
		{
			ID:                  stages.AccountHistoryIndex,
			Description:         "Generating account history index",
			Disabled:            !history,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			ExecFunc: func(s *StageState) error {
				return spawnAccountHistoryIndex(s, stateDB, datadir, core.UsePlainStateExecution, quitCh)
			},
		},
		{
			ID:                  stages.StorageHistoryIndex,
			Description:         "Generating storage history index",
			Disabled:            !history,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			ExecFunc: func(s *StageState) error {
				return spawnStorageHistoryIndex(s, stateDB, datadir, core.UsePlainStateExecution, quitCh)
			},
		},
	}

	state := NewState(stages)

	for !state.IsDone() {
		index, stage := state.CurrentStage()

		if stage.Disabled {
			message := fmt.Sprintf(
				"Sync stage %d/%d. %v disabled. %s",
				index+1,
				state.Len(),
				stage.Description,
				stage.DisabledDescription,
			)

			log.Info(message)

			state.NextStage()
			continue
		}

		stageState, err := state.StageState(stage.ID, stateDB)
		if err != nil {
			return err
		}

		message := fmt.Sprintf("Sync stage %d/%d. %v...", index+1, state.Len(), stage.Description)
		log.Info(message)

		err = stage.ExecFunc(stageState)
		if err != nil {
			return err
		}

		log.Info(fmt.Sprintf("%s DONE!", message))
	}

	return nil
}
