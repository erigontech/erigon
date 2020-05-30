package downloader

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/log"
)

func (d *Downloader) doStagedSyncWithFetchers(p *peerConnection, headersFetchers []func() error) error {
	var err error
	defer log.Info("Staged sync finished")

	/*
	* Stage 1. Download Headers
	 */
	log.Info("Sync stage 1/7. Downloading headers...")
	err = d.DownloadHeaders(headersFetchers)
	if err != nil {
		return err
	}

	/*
	* Stage 2. Download Block bodies
	 */
	log.Info("Sync stage 2/7. Downloading block bodies...")
	cont := true

	for cont && err == nil {
		cont, err = d.spawnBodyDownloadStage(p.id)
		if err != nil {
			return err
		}
	}

	log.Info("Sync stage 2/7. Downloading block bodies... Complete!")

	/*
	* Stage 3. Recover senders from tx signatures
	 */
	log.Info("Sync stage 3/7. Recovering senders from tx signatures...")

	if err = d.spawnRecoverSendersStage(); err != nil {
		return err
	}
	log.Info("Sync stage 3/7. Recovering senders from tx signatures... Complete!")

	/*
	* Stage 4. Execute block bodies w/o calculating trie roots
	 */
	log.Info("Sync stage 4/7. Executing blocks w/o hash checks...")
	syncHeadNumber, err := spawnExecuteBlocksStage(d.stateDB, d.blockchain, d.quitCh)
	if err != nil {
		return err
	}

	log.Info("Sync stage 4/7. Executing blocks w/o hash checks... Complete!")

	// Further stages go there
	log.Info("Sync stage 5/7. Validating final hash")
	err = spawnCheckFinalHashStage(d.stateDB, syncHeadNumber, d.datadir)
	if err != nil {
		return err
	}

	log.Info("Sync stage 5/7. Validating final hash... Complete!")

	if d.history {
		log.Info("Sync stage 6/7. Generating account history index")
		err = spawnAccountHistoryIndex(d.stateDB, d.datadir, core.UsePlainStateExecution, d.quitCh)
		if err != nil {
			return err
		}
		log.Info("Sync stage 6/7. Generating account history index... Complete!")
	} else {
		log.Info("Sync stage 6/7, generating account history index is disabled. Enable by adding `h` to --storage-mode")
	}

	if d.history {
		log.Info("Sync stage 7/7. Generating storage history index")
		err = spawnStorageHistoryIndex(d.stateDB, d.datadir, core.UsePlainStateExecution, d.quitCh)
		if err != nil {
			return err
		}
		log.Info("Sync stage 7/7. Generating storage history index... Complete!")
	} else {
		log.Info("Sync stage 7/7, generating storage history index is disabled. Enable by adding `h` to --storage-mode")
	}

	return err
}

func (d *Downloader) DownloadHeaders(headersFetchers []func() error) error {
	err := d.spawnSync(headersFetchers)
	if err != nil {
		return err
	}

	log.Info("Sync stage 1/7. Downloading headers... Complete!")
	log.Info("Checking for unwinding...")
	// Check unwinds backwards and if they are outstanding, invoke corresponding functions
	for stage := Finish - 1; stage > Headers; stage-- {
		unwindPoint, err := GetStageUnwind(d.stateDB, stage)
		if err != nil {
			return err
		}

		if unwindPoint == 0 {
			continue
		}

		switch stage {
		case Bodies:
			err = d.unwindBodyDownloadStage(unwindPoint)
		case Senders:
			err = d.unwindSendersStage(unwindPoint)
		case Execution:
			err = unwindExecutionStage(unwindPoint, d.stateDB)
		case HashCheck:
			err = unwindHashCheckStage(unwindPoint, d.stateDB)
		case AccountHistoryIndex:
			err = unwindAccountHistoryIndex(unwindPoint, d.stateDB, core.UsePlainStateExecution)
		case StorageHistoryIndex:
			err = unwindStorageHistoryIndex(unwindPoint, d.stateDB, core.UsePlainStateExecution)
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
