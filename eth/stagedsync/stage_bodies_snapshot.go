package stagedsync

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

type SnapshotBodiesCfg struct {
	enabled          bool
	db               kv.RwDB
	epochSize        uint64
	snapshotDir      string
	tmpDir           string
	client           *snapshotsync.Client
	snapshotMigrator *snapshotsync.SnapshotMigrator
	log              log.Logger
}

func StageSnapshotBodiesCfg(db kv.RwDB, snapshot ethconfig.Snapshot, client *snapshotsync.Client, snapshotMigrator *snapshotsync.SnapshotMigrator, tmpDir string, logger log.Logger) SnapshotBodiesCfg {
	return SnapshotBodiesCfg{
		enabled:          snapshot.Enabled && snapshot.Mode.Bodies,
		db:               db,
		snapshotDir:      snapshot.Dir,
		client:           client,
		snapshotMigrator: snapshotMigrator,
		tmpDir:           tmpDir,
		log:              logger,
		epochSize:        snapshot.EpochSize,
	}
}

func SpawnBodiesSnapshotGenerationStage(s *StageState, tx kv.RwTx, cfg SnapshotBodiesCfg, initial bool, ctx context.Context) error {
	//generate snapshot only on initial mode
	if tx != nil || cfg.epochSize == 0 {
		return nil
	}

	readTX, err := cfg.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer readTX.Rollback()

	to, err := stages.GetStageProgress(readTX, stages.Bodies)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	//it's too early for snapshot
	if to < cfg.epochSize {
		return nil
	}

	currentSnapshotBlock, err := stages.GetStageProgress(readTX, stages.CreateBodiesSnapshot)
	if err != nil {
		return fmt.Errorf("%w", err)
	}
	snapshotBlock := snapshotsync.CalculateEpoch(to, cfg.epochSize)

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
	err = s.Update(writeTX, snapshotBlock)
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

		return atomic.LoadUint64(&cfg.snapshotMigrator.BodiesCurrentSnapshot) == snapshotBlock, err
	}

	for {
		log.Info("Final", "Snapshot", "bodies")
		ok, err := final()
		if err != nil {
			return err
		}
		if ok {
			break
		}
		time.Sleep(10 * time.Second)
	}
	return nil
}

func UnwindBodiesSnapshotGenerationStage(s *UnwindState, tx kv.RwTx, cfg SnapshotBodiesCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneBodiesSnapshotGenerationStage(s *PruneState, tx kv.RwTx, cfg SnapshotBodiesCfg, ctx context.Context) (err error) {
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
