package stagedsync

import (
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

	return s.DoneAndUpdate(db, blockNum)
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

	return s.DoneAndUpdate(db, blockNum)
}

func unwindAccountHistoryIndex(unwindPoint uint64, db ethdb.Database, plainState bool, quitCh chan struct{}) error {
	ig := core.NewIndexGenerator(db, quitCh)
	if plainState {
		return ig.Truncate(unwindPoint, dbutils.PlainAccountChangeSetBucket)
	}
	return ig.Truncate(unwindPoint, dbutils.AccountChangeSetBucket)
}

func unwindStorageHistoryIndex(unwindPoint uint64, db ethdb.Database, plainState bool, quitCh chan struct{}) error {
	ig := core.NewIndexGenerator(db, quitCh)
	if plainState {
		return ig.Truncate(unwindPoint, dbutils.PlainStorageChangeSetBucket)
	}
	return ig.Truncate(unwindPoint, dbutils.StorageChangeSetBucket)
}
