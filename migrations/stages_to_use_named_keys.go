package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var dbKeys = []stages.SyncStage{
	stages.Headers,
	stages.BlockHashes,
	stages.Bodies,
	stages.Senders,
	stages.Execution,
	stages.IntermediateHashes,
	stages.HashState,
	stages.AccountHistoryIndex,
	stages.StorageHistoryIndex,
	stages.TxLookup,
	stages.TxPool,
	stages.Finish,
}

var stagesToUseNamedKeys = Migration{
	Name: "stages_to_use_named_keys",
	Up: func(db ethdb.Database, datadir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {

		if exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.SyncStageProgressOld1); err != nil {
			return err
		} else if !exists {
			return OnLoadCommit(db, nil, true)
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.SyncStageProgress); err != nil {
			return err
		}

		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if int(k[0]) >= len(dbKeys) || int(k[0]) < 0 {
				return nil
			}
			newKey := dbKeys[int(k[0])]
			// create new version of keys with same data
			if err := next(k, newKey, v); err != nil {
				return err
			}
			return nil
		}

		if err := etl.Transform(
			db,
			dbutils.SyncStageProgressOld1,
			dbutils.SyncStageProgress,
			datadir,
			extractFunc,
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: OnLoadCommit},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).DropBuckets(dbutils.SyncStageProgressOld1); err != nil {
			return err
		}
		return nil
	},
}

var unwindStagesToUseNamedKeys = Migration{
	Name: "unwind_stages_to_use_named_keys",
	Up: func(db ethdb.Database, datadir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.SyncStageUnwindOld1); err != nil {
			return err
		} else if !exists {
			return OnLoadCommit(db, nil, true)
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.SyncStageUnwind); err != nil {
			return err
		}

		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if int(k[0]) >= len(dbKeys) || int(k[0]) < 0 {
				return nil
			}
			newKey := dbKeys[int(k[0])]
			// create new version of keys with same data
			if err := next(k, newKey, v); err != nil {
				return err
			}
			return nil
		}

		if err := etl.Transform(
			db,
			dbutils.SyncStageUnwindOld1,
			dbutils.SyncStageUnwind,
			datadir,
			extractFunc,
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: OnLoadCommit},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).DropBuckets(dbutils.SyncStageUnwindOld1); err != nil {
			return err
		}
		return nil
	},
}
