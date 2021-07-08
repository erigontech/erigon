package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type SnapshotStateCfg struct {
	db               ethdb.RwKV
	snapshotDir      string
	tmpDir           string
	client           *snapshotsync.Client
	snapshotMigrator *snapshotsync.SnapshotMigrator
}

func StageSnapshotStateCfg(db ethdb.RwKV, snapshot ethconfig.Snapshot, tmpDir string, client *snapshotsync.Client, snapshotMigrator *snapshotsync.SnapshotMigrator) SnapshotStateCfg {
	return SnapshotStateCfg{
		db:               db,
		snapshotDir:      snapshot.Dir,
		client:           client,
		snapshotMigrator: snapshotMigrator,
		tmpDir:           tmpDir,
	}
}

func SpawnStateSnapshotGenerationStage(s *StageState, tx ethdb.RwTx, cfg SnapshotStateCfg, quit <-chan struct{}) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
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

func UnwindStateSnapshotGenerationStage(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg SnapshotStateCfg, quit <-chan struct{}) error {
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
