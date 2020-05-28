package downloader

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/log"
)

func (d *Downloader) doStagedSyncWithFetchers(ctx context.Context, p *peerConnection, headersFetchers []func() error) error {
	var err error
	withDone := newWithCancel(ctx, &err)

	/*
	* Stage 1. Download Headers
	 */
	log.Info("Sync stage 1/7. Downloading headers...")
	withDone(func() {
		err = d.DownloadHeaders(ctx, headersFetchers)
	})
	if err != nil {
		return err
	}

	/*
	* Stage 2. Download Block bodies
	 */
	log.Info("Sync stage 2/7. Downloading block bodies...")
	cont := true

	for cont && err == nil {
		withDone(func() {
			cont, err = d.spawnBodyDownloadStage(ctx, p.id)
		})
	}
	if err != nil {
		return err
	}

	log.Info("Sync stage 2/7. Downloading block bodies... Complete!")

	/*
	* Stage 3. Recover senders from tx signatures
	 */
	log.Info("Sync stage 3/7. Recovering senders from tx signatures...")

	withDone(func() {
		err = d.spawnRecoverSendersStage()
	})
	if err != nil {
		return err
	}
	log.Info("Sync stage 3/7. Recovering senders from tx signatures... Complete!")

	/*
	* Stage 4. Execute block bodies w/o calculating trie roots
	 */
	log.Info("Sync stage 4/7. Executing blocks w/o hash checks...")
	var syncHeadNumber uint64
	withDone(func() {
		syncHeadNumber, err = spawnExecuteBlocksStage(d.stateDB, d.blockchain)
	})
	if err != nil {
		return err
	}

	log.Info("Sync stage 4/7. Executing blocks w/o hash checks... Complete!")

	// Further stages go there
	log.Info("Sync stage 5/7. Validating final hash")
	withDone(func() {
		err = spawnCheckFinalHashStage(d.stateDB, syncHeadNumber, d.datadir)
	})
	if err != nil {
		return err
	}

	log.Info("Sync stage 5/7. Validating final hash... Complete!")

	if d.history {
		log.Info("Sync stage 6/7. Generating account history index")
		withDone(func() {
			err = spawnAccountHistoryIndex(d.stateDB, d.datadir, core.UsePlainStateExecution)
		})
		if err != nil {
			return err
		}
		log.Info("Sync stage 6/7. Generating account history index... Complete!")
	} else {
		log.Info("Sync stage 6/7, generating account history index is disabled. Enable by adding `h` to --storage-mode")
	}

	if d.history {
		log.Info("Sync stage 7/7. Generating storage history index")
		withDone(func() {
			err = spawnStorageHistoryIndex(d.stateDB, d.datadir, core.UsePlainStateExecution)
		})
		if err != nil {
			return err
		}
		log.Info("Sync stage 7/7. Generating storage history index... Complete!")
	} else {
		log.Info("Sync stage 7/7, generating storage history index is disabled. Enable by adding `h` to --storage-mode")
	}

	return err
}

func (d *Downloader) DownloadHeaders(ctx context.Context, headersFetchers []func() error) error {
	var err error
	withDone := newWithCancel(ctx, &err)
	withDone(func() {
		err = d.spawnSync(headersFetchers)
	})
	if err != nil {
		return err
	}

	log.Info("Sync stage 1/7. Downloading headers... Complete!")
	log.Info("Checking for unwinding...")
	// Check unwinds backwards and if they are outstanding, invoke corresponding functions
	for stage := Finish - 1; stage > Headers; stage-- {
		var unwindPoint uint64
		withCancel(ctx, &err, func() {
			unwindPoint, err = GetStageUnwind(d.stateDB, stage)
		})

		if err != nil {
			return err
		}
		if unwindPoint == 0 {
			continue
		}
		switch stage {
		case Bodies:
			withDone(func() { d.unwindBodyDownloadStage(unwindPoint) })
		case Senders:
			withDone(func() { d.unwindSendersStage(unwindPoint) })
		case Execution:
			withDone(func() { unwindExecutionStage(unwindPoint, d.stateDB) })
		case HashCheck:
			withDone(func() { unwindHashCheckStage(unwindPoint, d.stateDB) })
		case AccountHistoryIndex:
			withDone(func() { unwindAccountHistoryIndex(unwindPoint, d.stateDB, core.UsePlainStateExecution) })
		case StorageHistoryIndex:
			withDone(func() { unwindStorageHistoryIndex(unwindPoint, d.stateDB, core.UsePlainStateExecution) })
		default:
			return fmt.Errorf("unrecognized stage for unwinding: %d", stage)
		}
		if err != nil {
			return fmt.Errorf("error unwinding stage: %d: %v", stage, err)
		}
	}
	log.Info("Checking for unwinding... Complete!")
	return nil
}

func withCancel(ctx context.Context, err *error, run func()) {
	select {
	case <-ctx.Done():
		*err = errCanceled
		return
	default:
	}
	run()
}

func newWithCancel(ctx context.Context, err *error) func(func()) {
	return func(run func()) {
		withCancel(ctx, err, run)
	}
}
