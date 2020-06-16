package stagedsync

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func spawnAccountHistoryIndex(s *StageState, db ethdb.Database, datadir string, plainState bool, quitCh chan struct{}) error {
	var blockNum uint64
	lastProcessedBlockNumber := s.BlockNumber
	if lastProcessedBlockNumber > 0 {
		blockNum = lastProcessedBlockNumber + 1
	}

	ig := core.NewIndexGenerator(db, quitCh)
	ig.TempDir = datadir
	endBlock, err := s.ExecutionAt(db)
	if err != nil {
		log.Warn("Execution block error is empty")
	}
	if plainState {
		err = ig.GenerateIndex(blockNum, endBlock, dbutils.PlainAccountChangeSetBucket)
	} else {
		err = ig.GenerateIndex(blockNum, endBlock, dbutils.AccountChangeSetBucket)
	}
	if err != nil {
		return err
	}

	return s.DoneAndUpdate(db, endBlock)
}

func spawnStorageHistoryIndex(s *StageState, db ethdb.Database, datadir string, plainState bool, quitCh chan struct{}) error {
	var blockNum uint64
	lastProcessedBlockNumber := s.BlockNumber
	if lastProcessedBlockNumber > 0 {
		blockNum = lastProcessedBlockNumber + 1
	}
	ig := core.NewIndexGenerator(db, quitCh)
	ig.TempDir = datadir
	endBlock, err := s.ExecutionAt(db)
	if err != nil {
		log.Warn("Execution block error is empty")
	}
	if plainState {
		err = ig.GenerateIndex(blockNum, endBlock, dbutils.PlainStorageChangeSetBucket)
	} else {
		err = ig.GenerateIndex(blockNum, endBlock, dbutils.StorageChangeSetBucket)
	}
	if err != nil {
		return err
	}

	return s.DoneAndUpdate(db, endBlock)
}

func unwindAccountHistoryIndex(u *UnwindState, db ethdb.Database, plainState bool, quitCh chan struct{}) error {
	ig := core.NewIndexGenerator(db, quitCh)
	if plainState {
		if err := ig.Truncate(u.UnwindPoint, dbutils.PlainAccountChangeSetBucket); err != nil {
			return err
		}
	} else {
		if err := ig.Truncate(u.UnwindPoint, dbutils.AccountChangeSetBucket); err != nil {
			return err
		}
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind AccountHistorytIndex: %w", err)
	}
	return nil
}

func unwindStorageHistoryIndex(u *UnwindState, db ethdb.Database, plainState bool, quitCh chan struct{}) error {
	ig := core.NewIndexGenerator(db, quitCh)
	if plainState {
		if err := ig.Truncate(u.UnwindPoint, dbutils.PlainStorageChangeSetBucket); err != nil {
			return err
		}
	} else {
		if err := ig.Truncate(u.UnwindPoint, dbutils.StorageChangeSetBucket); err != nil {
			return err
		}
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind StorageHistorytIndex: %w", err)
	}
	return nil
}
