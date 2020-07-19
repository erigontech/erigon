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
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind Bodies: reset: %v", err)
	}
	return nil
}
