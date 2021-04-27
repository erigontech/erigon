package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
)

func SpawnBodiesSnapshotGenerationStage(s *StageState, db ethdb.Database, snapshotDir string, torrentClient *snapshotsync.Client, quit <-chan struct{}) error {
	return s.DoneAndUpdate(db, 0)
}