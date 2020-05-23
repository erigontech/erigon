package downloader

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func spawnAccountHistoryIndex(db ethdb.Database, plainState bool) error {
	lastProcessedBlockNumber, err := GetStageProgress(db, AccountHistoryIndex)
	if err != nil {
		return err
	}
	if plainState {
		log.Info("Skipped account index generation for plain state")
		return nil
	}
	ig := core.NewIndexGenerator(db)
	if err := ig.GenerateIndex(lastProcessedBlockNumber, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, walkerFactory(dbutils.AccountChangeSetBucket, plainState), func(innerDB ethdb.Database, blockNum uint64) error {
		return SaveStageProgress(innerDB, AccountHistoryIndex, blockNum)
	}); err != nil {
		fmt.Println("AccountChangeSetBucket, err", err)
		return err
	}
	return nil
}

func spawnStorageHistoryIndex(db ethdb.Database, plainState bool) error {
	lastProcessedBlockNumber, err := GetStageProgress(db, StorageHistoryIndex)
	if err != nil {
		return err
	}
	if plainState {
		log.Info("Skipped storaged index generation for plain state")
		return nil
	}
	ig := core.NewIndexGenerator(db)
	if err := ig.GenerateIndex(lastProcessedBlockNumber, dbutils.StorageChangeSetBucket, dbutils.StorageHistoryBucket, walkerFactory(dbutils.StorageChangeSetBucket, plainState), func(innerDB ethdb.Database, blockNum uint64) error {
		return SaveStageProgress(innerDB, StorageHistoryIndex, blockNum)
	}); err != nil {
		fmt.Println("StorageChangeSetBucket, err", err)
		return err
	}
	return nil
}

func unwindAccountHistoryIndex(unwindPoint uint64, db ethdb.Database, plainState bool) error {
	ig := core.NewIndexGenerator(db)
	return ig.Truncate(unwindPoint, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, walkerFactory(dbutils.AccountChangeSetBucket, plainState))
}

func unwindStorageHistoryIndex(unwindPoint uint64, db ethdb.Database, plainState bool) error {
	ig := core.NewIndexGenerator(db)
	return ig.Truncate(unwindPoint, dbutils.StorageChangeSetBucket, dbutils.StorageHistoryBucket, walkerFactory(dbutils.StorageChangeSetBucket, plainState))
}

func walkerFactory(csBucket []byte, plainState bool) func(bytes []byte) core.ChangesetWalker {
	switch {
	case bytes.Equal(csBucket, dbutils.AccountChangeSetBucket) && !plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.AccountChangeSetBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.AccountChangeSetBucket) && plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.AccountChangeSetPlainBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.StorageChangeSetBucket) && !plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.StorageChangeSetBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.StorageChangeSetBucket) && plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.StorageChangeSetPlainBytes(bytes)
		}
	default:
		log.Error("incorrect bucket", "bucket", string(csBucket), "plainState", plainState)
		panic("incorrect bucket " + string(csBucket))
	}
}
