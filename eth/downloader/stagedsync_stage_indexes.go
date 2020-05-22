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

func spawnGenerateIndexes(db ethdb.Database, plainState bool, from uint64) error {
	if plainState {
		log.Info("Skip index generation for plain state")
		return nil
	}

	ig := core.NewIndexGenerator(db)
	if err := ig.GenerateIndex(from, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, walkerFactory(dbutils.AccountChangeSetBucket, plainState), func(innerDB ethdb.Database, blockNum uint64) error {
		return SaveStageProgress(innerDB, IndexGeneration, blockNum)
	}); err != nil {
		fmt.Println("AccountChangeSetBucket, err", err)
		return err
	}

	ig = core.NewIndexGenerator(db)
	if err := ig.GenerateIndex(from, dbutils.StorageChangeSetBucket, dbutils.StorageHistoryBucket, walkerFactory(dbutils.StorageChangeSetBucket, plainState), func(innerDB ethdb.Database, blockNum uint64) error {
		return SaveStageProgress(innerDB, IndexGeneration, blockNum)
	}); err != nil {
		fmt.Println("StorageChangeSetBucket, err", err)
		return err
	}

	return nil
}

func unwindGenerateIndexes(unwindPoint uint64, db ethdb.Database, plainState bool) error {
	ig := core.NewIndexGenerator(db)
	err := ig.Truncate(unwindPoint, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, walkerFactory(dbutils.AccountChangeSetBucket, plainState))
	if err != nil {
		return err
	}
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
