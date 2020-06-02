package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func spawnBodyDownloadStage(db ethdb.Getter, d DownloaderGlue, pid string) (bool, error) {
	// Figure out how many blocks have already been downloaded
	origin, err := stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return false, fmt.Errorf("getting Bodies stage progress: %w", err)
	}
	return d.SpawnBodyDownloadStage(pid, origin)
}

func unwindBodyDownloadStage(db ethdb.Database, unwindPoint uint64) error {
	// Here we may want to remove all blocks if we wanted to
	lastProcessedBlockNumber, err := stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return fmt.Errorf("unwind Bodies: get stage progress: %v", err)
	}
	if unwindPoint >= lastProcessedBlockNumber {
		err = stages.SaveStageUnwind(db, stages.Bodies, 0)
		if err != nil {
			return fmt.Errorf("unwind Bodies: reset: %v", err)
		}
		return nil
	}
	mutation := db.NewBatch()
	err = stages.SaveStageUnwind(mutation, stages.Bodies, 0)
	if err != nil {
		return fmt.Errorf("unwind Bodies: reset: %v", err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("unwind Bodies: failed to write db commit: %v", err)
	}
	return nil
}
