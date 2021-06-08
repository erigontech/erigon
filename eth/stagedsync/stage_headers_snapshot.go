package stagedsync

import (
	"context"
	"fmt"
	"sync/atomic"
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

func SpawnHeadersSnapshotGenerationStage(s *StageState, tx ethdb.RwTx, cfg HeadersSnapshotGenCfg, initial bool, sm *snapshotsync.SnapshotMigrator, torrentClient *snapshotsync.Client, quit <-chan struct{}) error {
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

	err = sm.AsyncStages(snapshotBlock, cfg.db, readTX, torrentClient, false)
	if err != nil {
		return err
	}
	readTX.Rollback()

	for !sm.Replaced() {
		time.Sleep(time.Minute)
		log.Info("Wait old snapshot to close")
	}

	writeTX, err := cfg.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer writeTX.Rollback()

	err = sm.SyncStages(snapshotBlock, cfg.db, writeTX)
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
		err = sm.Final(readTX)

		return atomic.LoadUint64(&sm.HeadersCurrentSnapshot) == snapshotBlock, err
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
