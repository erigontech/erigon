package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var headerPrefixToSeparateBuckets = Migration{
	Name: "header_prefix_to_separate_buckets",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		if exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.HeaderPrefixOld); err != nil {
			return err
		} else if !exists {
			return CommitProgress(db, nil, true)
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.HeaderCanonicalBucket); err != nil {
			return err
		}

		if err := etl.Transform(
			"canonical headers",
			db,
			dbutils.HeaderPrefixOld,
			dbutils.HeaderCanonicalBucket,
			tmpdir,
			func(k []byte, v []byte, next etl.ExtractNextFunc) error {
				if dbutils.CheckCanonicalKey(k) {
					return next(k, k, v)
				}
				return nil
			},
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: CommitProgress},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.HeadersBucket); err != nil {
			return err
		}

		if err := etl.Transform(
			"headers",
			db,
			dbutils.HeaderPrefixOld,
			dbutils.HeadersBucket,
			tmpdir,
			func(k []byte, v []byte, next etl.ExtractNextFunc) error {
				if dbutils.CheckCanonicalKey(k) {
					return next(k, k, v)
				}
				return nil
			},
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: CommitProgress},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.HeaderTDBucket); err != nil {
			return err
		}
		if err := etl.Transform(
			"headers td",
			db,
			dbutils.HeaderPrefixOld,
			dbutils.HeaderTDBucket,
			tmpdir,
			func(k []byte, v []byte, next etl.ExtractNextFunc) error {
				if dbutils.CheckCanonicalKey(k) {
					return next(k, k, v)
				}
				return nil
			},
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: CommitProgress},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).DropBuckets(dbutils.CurrentStateBucketOld1); err != nil {
			return err
		}
		return nil
	},
}