package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func DownloadHeaders(s *StageState, d DownloaderGlue, stateDB ethdb.Database, headersFetchers []func() error, datadir string, u Unwinder, quitCh chan struct{}) error {
	err := d.SpawnSync(headersFetchers)
	if err == nil {
		s.Done()
	}
	return err
}
