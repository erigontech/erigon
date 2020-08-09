package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var stagedsyncToUseStageBlockhashes = Migration{
	Name: "stagedsync_to_use_stage_blockhashes",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := db.(ethdb.NonTransactional).BucketExists(dbutils.SyncStageProgressOld2); err != nil {
			return err
		} else if !exists {
			return nil
		}

		if err := db.(ethdb.NonTransactional).ClearBuckets(dbutils.SyncStageProgress); err != nil {
			return err
		}

		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			// create new version of keys with same data
			return next(k, k, v)
		}

		if err := etl.Transform(
			db,
			dbutils.SyncStageProgressOld2,
			dbutils.SyncStageProgress,
			datadir,
			extractFunc,
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: OnLoadCommit},
		); err != nil {
			return err
		}

		var progress []byte
		var err error
		if progress, err = db.Get(dbutils.SyncStageProgress, stages.DBKeys[stages.Headers]); err != nil {
			return err
		}

		if err = db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.BlockHashes], progress); err != nil {
			return err
		}

		if err = db.(ethdb.NonTransactional).DropBuckets(dbutils.SyncStageProgressOld2); err != nil {
			return err
		}
		return nil
	},
}

var unwindStagedsyncToUseStageBlockhashes = Migration{
	Name: "stagedsync_to_use_stage_blockhashes",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := db.(ethdb.NonTransactional).BucketExists(dbutils.SyncStageUnwindOld2); err != nil {
			return err
		} else if !exists {
			return nil
		}

		if err := db.(ethdb.NonTransactional).ClearBuckets(dbutils.SyncStageUnwind); err != nil {
			return err
		}

		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			// create new version of keys with same data
			return next(k, k, v)
		}

		if err := etl.Transform(
			db,
			dbutils.SyncStageUnwindOld2,
			dbutils.SyncStageUnwind,
			datadir,
			extractFunc,
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: OnLoadCommit},
		); err != nil {
			return err
		}

		var progress []byte
		var err error
		if progress, err = db.Get(dbutils.SyncStageUnwind, stages.DBKeys[stages.Headers]); err != nil {
			return err
		}

		if err = db.Put(dbutils.SyncStageUnwind, stages.DBKeys[stages.BlockHashes], progress); err != nil {
			return err
		}

		if err = db.(ethdb.NonTransactional).DropBuckets(dbutils.SyncStageUnwindOld2); err != nil {
			return err
		}
		return nil
	},
}
