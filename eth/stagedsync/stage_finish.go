package stagedsync

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func FinishForward(s *StageState, db ethdb.Database, notifier ChainEventNotifier, tx ethdb.Database, btClient *snapshotsync.Client, snBuilder *snapshotsync.SnapshotMigrator) error {
	var executionAt uint64
	var err error
	if executionAt, err = s.ExecutionAt(db); err != nil {
		return err
	}
	if executionAt <= s.BlockNumber {
		s.Done()
		return nil
	}

	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Update current block for the RPC API", logPrefix), "to", executionAt)

	err = NotifyNewHeaders(s.BlockNumber+1, executionAt, notifier, db)
	if err != nil {
		return err
	}

	err = MigrateSnapshot(executionAt, tx, db, btClient, snBuilder)
	if err != nil {
		return err
	}
	return s.DoneAndUpdate(db, executionAt)
}

func UnwindFinish(u *UnwindState, s *StageState, db ethdb.Database) error {
	return u.Done(db)
}

func NotifyNewHeaders(from, to uint64, notifier ChainEventNotifier, db ethdb.Database) error {
	if notifier == nil {
		log.Warn("rpc notifier is not set, rpc daemon won't be updated about headers")
		return nil
	}
	for i := from; i <= to; i++ {
		header := rawdb.ReadHeaderByNumber(db, i)
		if header == nil {
			return fmt.Errorf("could not find canonical header for number: %d", i)
		}
		notifier.OnNewHeader(header)
	}

	return nil
}

func MigrateSnapshot(to uint64, tx ethdb.Database, db ethdb.Database, btClient *snapshotsync.Client, mg *snapshotsync.SnapshotMigrator) error {
	if mg == nil {
		return nil
	}

	snBlock := snapshotsync.CalculateEpoch(to, snapshotsync.EpochSize)
	return mg.Migrate(db, tx, snBlock, btClient)
}
