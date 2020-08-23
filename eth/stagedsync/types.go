package stagedsync

import "github.com/ledgerwatch/turbo-geth/ethdb"

type DownloaderGlue interface {
	SpawnHeaderDownloadStage(db ethdb.Database, download func() error, process func(db ethdb.Database) error, s *StageState, u Unwinder) error
	SpawnBodyDownloadStage(ethdb.Database, string, *StageState, Unwinder) (bool, error)
}
