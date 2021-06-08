package stagedsync

import (
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

func SpawnBodiesSnapshotGenerationStage(s *StageState, db ethdb.RwKV, tx ethdb.RwTx, snapshotDir string, torrentClient *snapshotsync.Client, quit <-chan struct{}) error {
	s.Done()
	return nil
}
