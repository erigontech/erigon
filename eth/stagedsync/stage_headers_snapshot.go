package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type HeadersSnapshotGenCfg struct {
	db          ethdb.RwKV
	snapshotDir string
}

func StageHeadersSnapshotGenCfg(db ethdb.RwKV, snapshotDir string) HeadersSnapshotGenCfg {
	return HeadersSnapshotGenCfg{
		db:          db,
		snapshotDir: snapshotDir,
	}
}

func SpawnHeadersSnapshotGenerationStage(s *StageState, tx ethdb.RwTx, cfg HeadersSnapshotGenCfg, sm *snapshotsync.SnapshotMigrator2, torrentClient *snapshotsync.Client, quit <-chan struct{}) error {
	//generate snapshot only on initial mode
	if tx != nil {
		s.Done()
		return nil
	}

	readTX, err := cfg.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer readTX.Rollback()

	to, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	//it's too early for snapshot
	if to < snapshotsync.EpochSize {
		s.Done()
		return nil
	}

	currentSnapshotBlock, err := stages.GetStageProgress(tx, stages.CreateHeadersSnapshot)
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

	err = sm.AsyncStages(snapshotBlock, cfg.db, tx, torrentClient, false)
	if err != nil {
		return err
	}
	readTX.Rollback()

	for !sm.Replaced() {
		time.Sleep(time.Minute)
		log.Info("Wait old snapshot to close")
	}

	tx, err = cfg.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = sm.SyncStages(snapshotBlock, cfg.db, tx)
	if err != nil {
		return err
	}
	err = s.DoneAndUpdate(tx, snapshotBlock)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	final := func() (bool, error) {
		readTX, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return false, err
		}
		defer readTX.Rollback()

		return sm.Final(readTX)
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
