package stagedsync

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type SnapshotHeadersCfg struct {
	db               ethdb.RwKV
	snapshotDir      string
	client           *snapshotsync.Client
	snapshotMigrator *snapshotsync.SnapshotMigrator
}

func StageSnapshotHeadersCfg(db ethdb.RwKV, snapshot ethconfig.Snapshot, client *snapshotsync.Client, snapshotMigrator *snapshotsync.SnapshotMigrator) SnapshotHeadersCfg {
	return SnapshotHeadersCfg{
		db:               db,
		snapshotDir:      snapshot.Dir,
		client:           client,
		snapshotMigrator: snapshotMigrator,
	}
}

func SpawnHeadersSnapshotGenerationStage(s *StageState, tx ethdb.RwTx, cfg SnapshotHeadersCfg, initial bool, quit <-chan struct{}) error {
	//generate snapshot only on initial mode
	if !initial {
		s.Done()
		return nil
	}

	readTX, err := cfg.db.BeginRo(context.Background())
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
		s.Done()
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
		s.Done()
		return nil
	}

	err = cfg.snapshotMigrator.AsyncStages(snapshotBlock, cfg.db, readTX, cfg.client, false)
	if err != nil {
		return err
	}
	readTX.Rollback()

	for !cfg.snapshotMigrator.Replaced() {
		time.Sleep(time.Minute)
		log.Info("Wait old snapshot to close")
	}

	writeTX, err := cfg.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer writeTX.Rollback()

	err = cfg.snapshotMigrator.SyncStages(snapshotBlock, cfg.db, writeTX)
	if err != nil {
		return err
	}
	err = s.DoneAndUpdate(tx, snapshotBlock)
	if err != nil {
		return err
	}

	err = writeTX.Commit()
	if err != nil {
		return err
	}

	final := func() (bool, error) {
		readTX, err = cfg.db.BeginRw(context.Background())
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

func UnwindHeadersSnapshotGenerationStage(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg SnapshotHeadersCfg, quit <-chan struct{}) error {
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
