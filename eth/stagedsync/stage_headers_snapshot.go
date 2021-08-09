package stagedsync

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

type SnapshotHeadersCfg struct {
	db               kv.RwDB
	snapshotDir      string
	client           *snapshotsync.Client
	snapshotMigrator *snapshotsync.SnapshotMigrator
	log              log.Logger
}

func StageSnapshotHeadersCfg(db kv.RwDB, snapshot ethconfig.Snapshot, client *snapshotsync.Client, snapshotMigrator *snapshotsync.SnapshotMigrator, logger log.Logger) SnapshotHeadersCfg {
	return SnapshotHeadersCfg{
		db:               db,
		snapshotDir:      snapshot.Dir,
		client:           client,
		snapshotMigrator: snapshotMigrator,
		log:              logger,
	}
}

func SpawnHeadersSnapshotGenerationStage(s *StageState, tx kv.RwTx, cfg SnapshotHeadersCfg, initial bool, ctx context.Context) error {
	//generate snapshot only on initial mode
	if !initial {
		return nil
	}

	readTX, err := cfg.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer readTX.Rollback()

	to, err := stages.GetStageProgress(readTX, stages.Headers)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	//it's too early for snapshot
	if to < snapshotsync.EpochSize {
		return nil
	}

	currentSnapshotBlock, err := stages.GetStageProgress(readTX, stages.CreateHeadersSnapshot)
	if err != nil {
		return fmt.Errorf("%w", err)
	}
	snapshotBlock := snapshotsync.CalculateEpoch(to, snapshotsync.EpochSize)

	//Problem: we must inject this stage, because it's not possible to do compact mdbx after sync.
	//So we have to move headers to snapshot right after headers stage.
	//but we don't want to block not initial sync
	if snapshotBlock <= currentSnapshotBlock {
		return nil
	}

	err = cfg.snapshotMigrator.AsyncStages(snapshotBlock, cfg.log, cfg.db, readTX, cfg.client, false)
	if err != nil {
		return err
	}
	readTX.Rollback()

	for !cfg.snapshotMigrator.Replaced() {
		time.Sleep(time.Minute)
		log.Info("Wait old snapshot to close")
	}

	writeTX, err := cfg.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer writeTX.Rollback()

	err = cfg.snapshotMigrator.SyncStages(snapshotBlock, cfg.db, writeTX)
	if err != nil {
		return err
	}
	err = s.Update(tx, snapshotBlock)
	if err != nil {
		return err
	}

	err = writeTX.Commit()
	if err != nil {
		return err
	}

	final := func() (bool, error) {
		readTX, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return false, err
		}
		defer readTX.Rollback()
		err = cfg.snapshotMigrator.Final(readTX)

		return atomic.LoadUint64(&cfg.snapshotMigrator.HeadersCurrentSnapshot) == snapshotBlock, err
	}

	for {
		ok, err := final()
		if err != nil {
			return err
		}
		if ok {
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func UnwindHeadersSnapshotGenerationStage(u *UnwindState, tx kv.RwTx, cfg SnapshotHeadersCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
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

func PruneHeadersSnapshotGenerationStage(u *PruneState, tx kv.RwTx, cfg SnapshotHeadersCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
