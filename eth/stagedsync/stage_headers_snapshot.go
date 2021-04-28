package stagedsync

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"time"
)


func SpawnHeadersSnapshotGenerationStage(s *StageState, db ethdb.Database, sm *snapshotsync.SnapshotMigrator, snapshotDir string, torrentClient *snapshotsync.Client, quit <-chan struct{}) error {
	to, err := stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return fmt.Errorf("%w",  err)
	}

	currentSnapshotBlock, err := stages.GetStageProgress(db, stages.CreateHeadersSnapshot)
	if err != nil {
		return fmt.Errorf("%w",  err)
	}

	//Problem: we must inject this stage, because it's not possible to do compact mdbx after sync.
	//So we have to move headers to snapshot right after headers stage.
	//but we don't want to block not initial sync
	if to < currentSnapshotBlock+snapshotsync.EpochSize {
		s.Done()
		return nil
	}

	if to<snapshotsync.EpochSize {
		s.Done()
		return nil
	}
	if s.BlockNumber > to {
		return fmt.Errorf("headers snapshot is higher canonical. snapshot %d headers %d", s.BlockNumber, to)
	}

	snapshotBlock :=snapshotsync.CalculateEpoch(to, 50)

	if s.BlockNumber == snapshotBlock {
		// we already did snapshot creation for this block
		s.Done()
		return nil
	}

	err = sm.Migrate(db, db, snapshotBlock, torrentClient)
	for !sm.Finished(snapshotBlock) {
		select {
		case <-quit:
			break
		default:
			log.Info("Migrating to new snapshot", "stage", sm.GetStage())
			err = sm.Migrate(db,db,snapshotBlock, torrentClient)
			if err!=nil {
				return err
			}
		}
		time.Sleep(time.Second*10)
	}
	return s.DoneAndUpdate(db, snapshotBlock)
}




