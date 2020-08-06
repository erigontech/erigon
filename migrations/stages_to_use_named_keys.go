package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var stagesToUseNamedKeys = Migration{
	Name: "stages_to_use_named_keys",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := db.(ethdb.NonTransactional).BucketExists(dbutils.SyncStageProgressOld1); err != nil {
			return err
		} else if !exists {
			return nil
		}

		if err := db.(ethdb.NonTransactional).ClearBuckets(dbutils.SyncStageProgress); err != nil {
			return err
		}

		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			newKey, ok := stages.DBKeys[stages.SyncStage(k[0])]
			if !ok {
				return nil // nothing to do
			}
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

		if err := db.(ethdb.NonTransactional).DropBuckets(dbutils.SyncStageProgressOld1); err != nil {
			return err
		}
		return nil
	},
}

var unwindStagesToUseNamedKeys = Migration{
	Name: "unwind_stages_to_use_named_keys",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := db.(ethdb.NonTransactional).BucketExists(dbutils.SyncStageUnwindOld1); err != nil {
			return err
		} else if !exists {
			return nil
		}

		if err := db.(ethdb.NonTransactional).ClearBuckets(dbutils.SyncStageUnwind); err != nil {
			return err
		}

		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			newKey, ok := stages.DBKeys[stages.SyncStage(k[0])]
			if !ok {
				return nil // nothing to do

			}
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

		if err := db.(ethdb.NonTransactional).DropBuckets(dbutils.SyncStageUnwindOld1); err != nil {
			return err
		}
		return nil
	},
}
