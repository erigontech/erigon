package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
)

func SpawnBodiesSnapshotGenerationStage(s *StageState, db ethdb.RwKV, tx ethdb.RwTx, snapshotDir string, torrentClient *snapshotsync.Client, quit <-chan struct{}) error {
	s.Done()
	return nil
}
