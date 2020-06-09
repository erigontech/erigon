package stagedsync

import (
	"os"
	"runtime/pprof"

	"github.com/ledgerwatch/turbo-geth/log"
)

func SpawnHeaderDownloadStage(s *StageState, u Unwinder, d DownloaderGlue, headersFetchers []func() error) error {
	if prof {
		f, err := os.Create("cpu1.prof")
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

	err := d.SpawnHeaderDownloadStage(headersFetchers, s, u)
	if err == nil {
		s.Done()
	}
	return err
}
