package downloader

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/log"
)

func (d *Downloader) doStagedSyncWithFetchers(p *peerConnection, headersFetchers []func() error) error {
	fmt.Println("doStagedSyncWithFetchers")
	log.Info("Sync stage 1/5. Downloading headers...")

	var err error

	/*
	* Stage 1. Download Headers
	 */
	if err = d.spawnSync(headersFetchers); err != nil {
		return err
	}

	log.Info("Sync stage 1/6. Downloading headers... Complete!")
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
			if !core.UsePlainStateExecution {
				err = d.unwindHashCheckStage(unwindPoint)
			}
		case HistoryIndex:
			err = unwindGenerateIndexes(unwindPoint, d.stateDB, core.UsePlainStateExecution)
		default:
			return fmt.Errorf("unrecognized stage for unwinding: %d", stage)
		}
		if err != nil {
			return fmt.Errorf("error unwinding stage: %d: %v", stage, err)
		}
	}
	log.Info("Checking for unwinding... Complete!")
	log.Info("Sync stage 2/6. Downloading block bodies...")

	/*
	* Stage 2. Download Block bodies
	 */
	cont := true

	for cont && err == nil {
		cont, err = d.spawnBodyDownloadStage(p.id)
	}
	if err != nil {
		return err
	}

	log.Info("Sync stage 2/6. Downloading block bodies... Complete!")
	/*
	* Stage 3. Recover senders from tx signatures
	 */
	log.Info("Sync stage 3/6. Recovering senders from tx signatures...")

	err = d.spawnRecoverSendersStage()
	if err != nil {
		return err
	}

	log.Info("Sync stage 3/6. Recovering senders from tx signatures... Complete!")
	log.Info("Sync stage 4/6. Executing blocks w/o hash checks...")

	/*
	* Stage 4. Execute block bodies w/o calculating trie roots
	 */

	var syncHeadNumber uint64
	syncHeadNumber, err = spawnExecuteBlocksStage(d.stateDB, d.blockchain)
	if err != nil {
		return err
	}

	log.Info("Sync stage 4/6. Executing blocks w/o hash checks... Complete!")

	// Further stages go there
	log.Info("Sync stage 5/6. Validating final hash")
	if !core.UsePlainStateExecution {
		if err = d.spawnCheckFinalHashStage(syncHeadNumber); err != nil {
			return err
		}
	}

	log.Info("Sync stage 5/6. Validating final hash... Complete!")

	if d.history {
		log.Info("Sync stage 6/6. Generating history indexes")
		err = spawnGenerateIndexes(d.stateDB, core.UsePlainStateExecution)
		if err != nil {
			return err
		}
		log.Info("Sync stage 6/6. Generating history indexes... Complete!")
	}

	return err
}
