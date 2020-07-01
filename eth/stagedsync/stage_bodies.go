package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func spawnBodyDownloadStage(s *StageState, u Unwinder, d DownloaderGlue, pid string) error {
	cont, err := d.SpawnBodyDownloadStage(pid, s, u)
	if err != nil {
		return err
	}
	if !cont {
		s.Done()
	}
	return nil

}

func unwindBodyDownloadStage(u *UnwindState, db ethdb.Database) error {
	mutation := db.NewBatch()
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind Bodies: reset: %v", err)
	}
	if _, err := mutation.Commit(); err != nil {
		return fmt.Errorf("unwind Bodies: failed to write db commit: %v", err)
	}
	return nil
}
