package snapshotsync

import (
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

var (
	bucketConfigs = map[SnapshotType]dbutils.BucketsCfg{
		SnapshotType_bodies: {
			dbutils.BlockBodyPrefix:          dbutils.BucketConfigItem{},
			dbutils.BodiesSnapshotInfoBucket: dbutils.BucketConfigItem{},
		},
		SnapshotType_headers: {
			dbutils.HeaderPrefix:              dbutils.BucketConfigItem{},
			dbutils.HeadersSnapshotInfoBucket: dbutils.BucketConfigItem{},
		},
		SnapshotType_state: {
			dbutils.PlainStateBucket: dbutils.BucketConfigItem{
				Flags:                     dbutils.DupSort,
				AutoDupSortKeysConversion: true,
				DupFromLen:                60,
				DupToLen:                  28,
			},
			dbutils.PlainContractCodeBucket: dbutils.BucketConfigItem{},
			dbutils.CodeBucket:              dbutils.BucketConfigItem{},
			dbutils.StateSnapshotInfoBucket: dbutils.BucketConfigItem{},
		},
	}
)

func WrapBySnapshotsFromDir(kv ethdb.KV, snapshotDir string, mode SnapshotMode) (ethdb.KV, error) {
	log.Info("Wrap db to snapshots", "dir", snapshotDir, "mode", mode.ToString())
	snkv := ethdb.NewSnapshot2KV().DB(kv)

	if mode.Bodies {
		snapshotKV, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path(snapshotDir + "/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return bucketConfigs[SnapshotType_bodies]
		}).Open()
		if err != nil {
			log.Error("Can't open body snapshot", "err", err)
			return nil, err
		} else { //nolint
			snkv.SnapshotDB([]string{dbutils.BlockBodyPrefix, dbutils.BodiesSnapshotInfoBucket}, snapshotKV)
		}
	}

	if mode.Headers {
		snapshotKV, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path(snapshotDir + "/headers").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return bucketConfigs[SnapshotType_headers]
		}).Open()
		if err != nil {
			log.Error("Can't open headers snapshot", "err", err)
			return nil, err
		} else { //nolint
			snkv.SnapshotDB([]string{dbutils.HeaderPrefix, dbutils.HeadersSnapshotInfoBucket}, snapshotKV)
		}
	}
	if mode.State {
		snapshotKV, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path(snapshotDir + "/headers").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return bucketConfigs[SnapshotType_headers]
		}).Open()
		if err != nil {
			log.Error("Can't open headers snapshot", "err", err)
			return nil, err
		} else { //nolint
			snkv.SnapshotDB([]string{dbutils.StateSnapshotInfoBucket, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket, dbutils.CodeBucket}, snapshotKV)
		}
	}
	return snkv.MustOpen(), nil
}

func WrapBySnapshotsFromDownloader(kv ethdb.KV, snapshots map[SnapshotType]*SnapshotsInfo) (ethdb.KV, error) {
	snKV := ethdb.NewSnapshot2KV().DB(kv)
	for k, v := range snapshots {
		log.Info("Wrap db by", "snapshot", k.String(), "dir", v.Dbpath)
		cfg := bucketConfigs[k]
		snapshotKV, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path(v.Dbpath).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return cfg
		}).Open()

		if err != nil {
			log.Error("Can't open snapshot", "err", err)
			return nil, err
		} else { //nolint
			buckets := make([]string, 0, 1)
			for bucket := range bucketConfigs[k] {
				buckets = append(buckets, bucket)
			}

			snKV.SnapshotDB(buckets, snapshotKV)
		}
	}

	return snKV.MustOpen(), nil
}
