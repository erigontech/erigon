package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func SpawnAccountHistoryIndex(s *StageState, db ethdb.Database, datadir string, quitCh <-chan struct{}) error {
	endBlock, err := s.ExecutionAt(db)
	if err != nil {
		return fmt.Errorf("account history index: getting last executed block: %w", err)
	}
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}
	var blockNum uint64
	lastProcessedBlockNumber := s.BlockNumber
	if lastProcessedBlockNumber > 0 {
		blockNum = lastProcessedBlockNumber + 1
	}

	ig := core.NewIndexGenerator(db, quitCh)
	ig.TempDir = datadir

	if err := ig.GenerateIndex(blockNum, endBlock, dbutils.PlainAccountChangeSetBucket, datadir); err != nil {
		return fmt.Errorf("account history index: fail to generate index: %w", err)
	}

	return s.DoneAndUpdate(db, endBlock)
}

func SpawnStorageHistoryIndex(s *StageState, db ethdb.Database, datadir string, quitCh <-chan struct{}) error {
	endBlock, err := s.ExecutionAt(db)
	if err != nil {
		return fmt.Errorf("storage history index: getting last executed block: %w", err)
	}
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}
	var blockNum uint64
	lastProcessedBlockNumber := s.BlockNumber
	if lastProcessedBlockNumber > 0 {
		blockNum = lastProcessedBlockNumber + 1
	}
	ig := core.NewIndexGenerator(db, quitCh)
	ig.TempDir = datadir
	if err := ig.GenerateIndex(blockNum, endBlock, dbutils.PlainStorageChangeSetBucket, datadir); err != nil {
		return fmt.Errorf("storage history index: fail to generate index: %w", err)
	}

	return s.DoneAndUpdate(db, endBlock)
}

func UnwindAccountHistoryIndex(u *UnwindState, db ethdb.Database, quitCh <-chan struct{}) error {
	ig := core.NewIndexGenerator(db, quitCh)
	if err := ig.Truncate(u.UnwindPoint, dbutils.PlainAccountChangeSetBucket); err != nil {
		return fmt.Errorf("account history index: fail to truncate index: %w", err)
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind AccountHistorytIndex: %w", err)
	}
	return nil
}

func UnwindStorageHistoryIndex(u *UnwindState, db ethdb.Database, quitCh <-chan struct{}) error {
	ig := core.NewIndexGenerator(db, quitCh)
	if err := ig.Truncate(u.UnwindPoint, dbutils.PlainStorageChangeSetBucket); err != nil {
		return fmt.Errorf("storage history index: fail to truncate index: %w", err)
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind StorageHistorytIndex: %w", err)
	}
	return nil
}
