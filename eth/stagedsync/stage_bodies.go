package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func spawnBodyDownloadStage(s *StageState, u Unwinder, d DownloaderGlue, pid string, pb *PrefetchedBlocks) error {
	logPrefix := s.state.LogPrefix()
	cont, err := d.SpawnBodyDownloadStage(logPrefix, pid, s, u, pb)
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
		return fmt.Errorf("unwind Bodies: reset: %w", err)
	}
	return nil
}
