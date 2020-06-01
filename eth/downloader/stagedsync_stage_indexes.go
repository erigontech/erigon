package downloader

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func spawnAccountHistoryIndex(db ethdb.Database, datadir string, plainState bool, quitCh chan struct{}) error {
	var blockNum uint64
	if lastProcessedBlockNumber, err := GetStageProgress(db, AccountHistoryIndex); err == nil {
		if lastProcessedBlockNumber > 0 {
			blockNum = lastProcessedBlockNumber + 1
		}
	} else {
		return fmt.Errorf("reading account history process: %v", err)
	}
	log.Info("Account history index generation started", "from", blockNum)

	ig := core.NewIndexGenerator(db, quitCh)
	ig.TempDir = datadir
	var err error
	if plainState {
		err = ig.GenerateIndex(blockNum, dbutils.PlainAccountChangeSetBucket)
	} else {
		err = ig.GenerateIndex(blockNum, dbutils.AccountChangeSetBucket)
	}
	if err != nil {
		return err
	}

	if err := SaveStageProgress(db, AccountHistoryIndex, blockNum); err != nil {
		return err
	}
	return nil

}

func spawnStorageHistoryIndex(db ethdb.Database, datadir string, plainState bool, quitCh chan struct{}) error {
	var blockNum uint64
	if lastProcessedBlockNumber, err := GetStageProgress(db, StorageHistoryIndex); err == nil {
		if lastProcessedBlockNumber > 0 {
			blockNum = lastProcessedBlockNumber + 1
		}
	} else {
		return fmt.Errorf("reading storage history process: %v", err)
	}
	ig := core.NewIndexGenerator(db, quitCh)
	ig.TempDir = datadir
	var err error
	if plainState {
		err = ig.GenerateIndex(blockNum, dbutils.PlainStorageChangeSetBucket)
	} else {
		err = ig.GenerateIndex(blockNum, dbutils.StorageChangeSetBucket)
	}
	if err != nil {
		return err
	}

	if err = SaveStageProgress(db, StorageHistoryIndex, blockNum); err != nil {
		return err
	}

	return nil
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
