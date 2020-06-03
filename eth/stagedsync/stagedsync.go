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
	var err error
	defer log.Info("Staged sync finished")

	/*
	* Stage 1. Download Headers
	 */
	log.Info("Sync stage 1/7. Downloading headers...")
	err = DownloadHeaders(d, stateDB, headersFetchers, quitCh)
	if err != nil {
		return err
	}

	/*
	* Stage 2. Download Block bodies
	 */
	log.Info("Sync stage 2/7. Downloading block bodies...")
	cont := true

	for cont && err == nil {
		cont, err = spawnBodyDownloadStage(stateDB, d, pid)
		if err != nil {
			return err
		}
	}

	log.Info("Sync stage 2/7. Downloading block bodies... Complete!")

	/*
	* Stage 3. Recover senders from tx signatures
	 */
	log.Info("Sync stage 3/7. Recovering senders from tx signatures...")

	if err = spawnRecoverSendersStage(stateDB, blockchain.Config(), quitCh); err != nil {
		return err
	}
	log.Info("Sync stage 3/7. Recovering senders from tx signatures... Complete!")

	/*
	* Stage 4. Execute block bodies w/o calculating trie roots
	 */
	log.Info("Sync stage 4/7. Executing blocks w/o hash checks...")
	syncHeadNumber, err := spawnExecuteBlocksStage(stateDB, blockchain, quitCh)
	if err != nil {
		return err
	}

	log.Info("Sync stage 4/7. Executing blocks w/o hash checks... Complete!")

	// Further stages go there
	log.Info("Sync stage 5/7. Validating final hash")
	err = SpawnCheckFinalHashStage(stateDB, syncHeadNumber, datadir, quitCh)
	if err != nil {
		return err
	}

	log.Info("Sync stage 5/7. Validating final hash... Complete!")

	if history {
		log.Info("Sync stage 6/7. Generating account history index")
		err = spawnAccountHistoryIndex(stateDB, datadir, core.UsePlainStateExecution, quitCh)
		if err != nil {
			return err
		}
		log.Info("Sync stage 6/7. Generating account history index... Complete!")
	} else {
		log.Info("Sync stage 6/7, generating account history index is disabled. Enable by adding `h` to --storage-mode")
	}

	if history {
		log.Info("Sync stage 7/7. Generating storage history index")
		err = spawnStorageHistoryIndex(stateDB, datadir, core.UsePlainStateExecution, quitCh)
		if err != nil {
			return err
		}
		log.Info("Sync stage 7/7. Generating storage history index... Complete!")
	} else {
		log.Info("Sync stage 7/7, generating storage history index is disabled. Enable by adding `h` to --storage-mode")
	}

	return err
}

func DownloadHeaders(d DownloaderGlue, stateDB ethdb.Database, headersFetchers []func() error, quitCh chan struct{}) error {
	err := d.SpawnSync(headersFetchers)
	if err != nil {
		return err
	}

	log.Info("Sync stage 1/7. Downloading headers... Complete!")
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
			err = unwindHashCheckStage(unwindPoint, stateDB)
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
	return nil
}
