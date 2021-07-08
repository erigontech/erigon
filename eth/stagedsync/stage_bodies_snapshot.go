package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type SnapshotBodiesCfg struct {
	db               ethdb.RwKV
	snapshotDir      string
	tmpDir           string
	client           *snapshotsync.Client
	snapshotMigrator *snapshotsync.SnapshotMigrator
}

func StageSnapshotBodiesCfg(db ethdb.RwKV, snapshot ethconfig.Snapshot, client *snapshotsync.Client, snapshotMigrator *snapshotsync.SnapshotMigrator, tmpDir string) SnapshotBodiesCfg {
	return SnapshotBodiesCfg{
		db:               db,
		snapshotDir:      snapshot.Dir,
		client:           client,
		snapshotMigrator: snapshotMigrator,
		tmpDir:           tmpDir,
	}
}

func SpawnBodiesSnapshotGenerationStage(s *StageState, tx ethdb.RwTx, cfg SnapshotBodiesCfg, quit <-chan struct{}) error {
	s.Done()
	return nil
}

func UnwindBodiesSnapshotGenerationStage(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg SnapshotBodiesCfg, quit <-chan struct{}) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
