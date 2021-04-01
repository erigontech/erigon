package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
)

func SpawnStateSnapshotGenerationStage(s *StageState, db ethdb.Database, snapshotDir string, torrentClient *bittorrent.Client, quit <-chan struct{}) error {
	return s.DoneAndUpdate(db, 0)
}