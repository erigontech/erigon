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
			dbutils.BlockBodyPrefix:    dbutils.BucketConfigItem{},
			dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
		},
		SnapshotType_headers: {
			dbutils.HeaderPrefix:       dbutils.BucketConfigItem{},
			dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
		},
	}
)

func WrapBySnapshots(kv ethdb.KV, snapshotDir string, mode SnapshotMode) (ethdb.KV, error) {
	log.Info("Wrap db to snapshots", "dir", snapshotDir, "mode", mode.ToString())
	if mode.Bodies {
		snapshotKV, err := ethdb.NewLMDB().Flags(lmdb.Readonly).Path(snapshotDir + "/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return bucketConfigs[SnapshotType_bodies]
		}).Open()
		if err != nil {
			log.Error("Can't open body snapshot", "err", err)
			return nil, err
		} else { //nolint
			kv = ethdb.NewSnapshotKV().SnapshotDB(snapshotKV).
				For(dbutils.BlockBodyPrefix).
				For(dbutils.SnapshotInfoBucket).
				DB(kv).MustOpen()
		}
	}

	if mode.Headers {
		snapshotKV, err := ethdb.NewLMDB().Flags(lmdb.Readonly).Path(snapshotDir + "/headers").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return bucketConfigs[SnapshotType_headers]
		}).Open()
		if err != nil {
			log.Error("Can't open headers snapshot", "err", err)
			return nil, err
		} else { //nolint
			kv = ethdb.NewSnapshotKV().SnapshotDB(snapshotKV).
				For(dbutils.HeaderPrefix).
				For(dbutils.SnapshotInfoBucket).
				DB(kv).MustOpen()
		}
	}
	return kv, nil
}

func WrapBySnapshots2(kv ethdb.KV, snapshots map[SnapshotType]*SnapshotsInfo) (ethdb.KV, error) {

	for k, v := range snapshots {
		log.Info("Wrap db by", "snapshot", k.String(), "dir", v.Dbpath)
		cfg := bucketConfigs[k]
		snapshotKV, err := ethdb.NewLMDB().Flags(lmdb.Readonly).Path(v.Dbpath).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return cfg
		}).Open()
		if err != nil {
			log.Error("Can't open snapshot", "err", err)
			return nil, err
		} else { //nolint
			snKV := ethdb.NewSnapshotKV().SnapshotDB(snapshotKV)
			for i := range bucketConfigs[k] {
				snKV.For(i)
			}
			kv = snKV.DB(kv).MustOpen()
		}
	}
	return kv, nil
}
