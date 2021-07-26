package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type SnapshotStateCfg struct {
	enabled          bool
	db               ethdb.RwKV
	snapshotDir      string
	tmpDir           string
	client           *snapshotsync.Client
	snapshotMigrator *snapshotsync.SnapshotMigrator
}

func StageSnapshotStateCfg(db ethdb.RwKV, snapshot ethconfig.Snapshot, tmpDir string, client *snapshotsync.Client, snapshotMigrator *snapshotsync.SnapshotMigrator) SnapshotStateCfg {
	return SnapshotStateCfg{
		enabled:          snapshot.Enabled && snapshot.Mode.State,
		db:               db,
		snapshotDir:      snapshot.Dir,
		client:           client,
		snapshotMigrator: snapshotMigrator,
		tmpDir:           tmpDir,
	}
}

func SpawnStateSnapshotGenerationStage(s *StageState, tx ethdb.RwTx, cfg SnapshotStateCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err = s.Update(tx, 0); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindStateSnapshotGenerationStage(s *UnwindState, tx ethdb.RwTx, cfg SnapshotStateCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err = s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneStateSnapshotGenerationStage(s *PruneState, tx ethdb.RwTx, cfg SnapshotStateCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
