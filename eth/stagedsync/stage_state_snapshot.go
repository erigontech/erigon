package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

func SpawnStateSnapshotGenerationStage(s *StageState, db ethdb.RwKV, tx ethdb.RwTx, snapshotDir string, torrentClient *snapshotsync.Client, quit <-chan struct{}) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	err := s.DoneAndUpdate(tx, 0)

	if err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil

}
