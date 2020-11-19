package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func SpawnAccountHistoryIndex(s *StageState, db ethdb.Database, tmpdir string, quitCh <-chan struct{}) error {
	executionAt, err := s.ExecutionAt(db)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	if executionAt == s.BlockNumber {
		s.Done()
		return nil
	}
	var startChangeSetsLookupAt uint64
	if s.BlockNumber > 0 {
		startChangeSetsLookupAt = s.BlockNumber + 1
	}
	stopChangeSetsLookupAt := executionAt + 1

	ig := core.NewIndexGenerator(logPrefix, db, quitCh)
	ig.TempDir = tmpdir
	if err := ig.GenerateIndex(startChangeSetsLookupAt, stopChangeSetsLookupAt, dbutils.PlainAccountChangeSetBucket, tmpdir); err != nil {
		return fmt.Errorf("%s: fail to generate index: %w", logPrefix, err)
	}

	return s.DoneAndUpdate(db, executionAt)
}

func SpawnStorageHistoryIndex(s *StageState, db ethdb.Database, tmpdir string, quitCh <-chan struct{}) error {
	endBlock, err := s.ExecutionAt(db)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
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
	ig := core.NewIndexGenerator(logPrefix, db, quitCh)
	ig.TempDir = tmpdir
	if err := ig.GenerateIndex(blockNum, endBlock+1, dbutils.PlainStorageChangeSetBucket, tmpdir); err != nil {
		return fmt.Errorf("%s: fail to generate index: %w", logPrefix, err)
	}

	return s.DoneAndUpdate(db, endBlock)
}

func UnwindAccountHistoryIndex(u *UnwindState, s *StageState, db ethdb.Database, quitCh <-chan struct{}) error {
	logPrefix := s.state.LogPrefix()
	ig := core.NewIndexGenerator(logPrefix, db, quitCh)
	if err := ig.Truncate(u.UnwindPoint, dbutils.PlainAccountChangeSetBucket); err != nil {
		return fmt.Errorf("%s: fail to truncate index: %w", logPrefix, err)
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("%s: %w", logPrefix, err)
	}
	return nil
}

func UnwindStorageHistoryIndex(u *UnwindState, s *StageState, db ethdb.Database, quitCh <-chan struct{}) error {
	logPrefix := s.state.LogPrefix()
	ig := core.NewIndexGenerator(logPrefix, db, quitCh)
	if err := ig.Truncate(u.UnwindPoint, dbutils.PlainStorageChangeSetBucket); err != nil {
		return fmt.Errorf("%s: fail to truncate index: %w", logPrefix, err)
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("%s: %w", logPrefix, err)
	}
	return nil
}
