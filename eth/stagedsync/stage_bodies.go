package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func spawnBodyDownloadStage(s *StageState, db ethdb.Getter, d DownloaderGlue, pid string) error {
	cont, err := d.SpawnBodyDownloadStage(pid, s.BlockNumber)
	if err != nil {
		return err
	}
	if !cont {
		s.Done()
	}
	return nil

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
