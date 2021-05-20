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

func SpawnHeadersSnapshotGenerationStage(s *StageState, tx ethdb.RwTx, cfg HeadersSnapshotGenCfg, sm *snapshotsync.SnapshotMigrator, torrentClient *snapshotsync.Client, quit <-chan struct{}) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	to, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	currentSnapshotBlock, err := stages.GetStageProgress(tx, stages.CreateHeadersSnapshot)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	//Problem: we must inject this stage, because it's not possible to do compact mdbx after sync.
	//So we have to move headers to snapshot right after headers stage.
	//but we don't want to block not initial sync
	if to < currentSnapshotBlock+snapshotsync.EpochSize {
		s.Done()
		return nil
	}

	if to < snapshotsync.EpochSize {
		s.Done()
		return nil
	}
	if s.BlockNumber > to {
		return fmt.Errorf("headers snapshot is higher canonical. snapshot %d headers %d", s.BlockNumber, to)
	}

	snapshotBlock := snapshotsync.CalculateEpoch(to, snapshotsync.EpochSize)

	if s.BlockNumber == snapshotBlock {
		// we already did snapshot creation for this block
		s.Done()
		return nil
	}

	err = sm.Migrate(cfg.db, tx, snapshotBlock, torrentClient)
	if err != nil {
		return err
	}
	for !sm.Finished(snapshotBlock) {
		select {
		case <-quit:
			break
		default:
			log.Info("Migrating to new snapshot", "stage", sm.GetStage())
			err = sm.Migrate(cfg.db, tx, snapshotBlock, torrentClient)
			if err != nil {
				return err
			}
		}
		time.Sleep(time.Second * 10)
	}
	err = s.DoneAndUpdate(tx, snapshotBlock)
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
