package snapshotdownloader

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func WrapBySnapshots(kv ethdb.KV, snapshotDir string, mode SnapshotMode) (ethdb.KV, error) {
	log.Info("Wrap db to snapshots", "dir", snapshotDir, "mode", mode.ToString())
	if mode.Bodies {
		snapshotKV, err := ethdb.NewLMDB().Path(snapshotDir + "/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.BlockBodyPrefix:    dbutils.BucketConfigItem{},
				dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
			}
		}).ReadOnly().Open()
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
		snapshotKV, err := ethdb.NewLMDB().Path(snapshotDir + "/headers").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.HeaderPrefix:       dbutils.BucketConfigItem{},
				dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
			}
		}).ReadOnly().Open()
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
