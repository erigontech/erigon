package stagedsync

import (
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func spawnBodyDownloadStage(s *StageState, u Unwinder, d DownloaderGlue, pid string) error {
	if prof {
		f, err := os.Create("cpu2.prof")
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return err
		}
		defer f.Close()
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return err
		}
		defer pprof.StopCPUProfile()
	}

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
