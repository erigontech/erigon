package snapshotsync

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"path/filepath"
)

var (
	BucketConfigs = map[SnapshotType]dbutils.BucketsCfg{
		SnapshotType_bodies: {
			dbutils.BlockBodyPrefix:          dbutils.BucketConfigItem{},
			dbutils.EthTx:          dbutils.BucketConfigItem{},
			dbutils.BodiesSnapshotInfoBucket: dbutils.BucketConfigItem{},
		},
		SnapshotType_headers: {
			dbutils.HeadersBucket:             dbutils.BucketConfigItem{},
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
		},
	}
)

func WrapBySnapshotsFromDir(kv ethdb.RwKV, snapshotDir string, mode SnapshotMode) (ethdb.RwKV, error) {
	log.Info("Wrap db to snapshots", "dir", snapshotDir, "mode", mode.ToString())
	snkv := ethdb.NewSnapshotKV().DB(kv)

	if mode.Bodies {
		snapshotKV, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path(snapshotDir + "/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return BucketConfigs[SnapshotType_bodies]
		}).Open()
		if err != nil {
			log.Error("Can't open body snapshot", "err", err)
			return nil, err
		} else { //nolint
			snkv = snkv.SnapshotDB([]string{dbutils.BlockBodyPrefix, dbutils.BodiesSnapshotInfoBucket, dbutils.EthTx}, snapshotKV)
		}
	}

	if mode.Headers {
		snapshotKV, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path(snapshotDir + "/headers").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return BucketConfigs[SnapshotType_headers]
		}).Open()
		if err != nil {
			log.Error("Can't open headers snapshot", "err", err)
			return nil, err
		} else { //nolint
			snkv = snkv.SnapshotDB([]string{dbutils.HeadersBucket, dbutils.HeadersSnapshotInfoBucket}, snapshotKV)
		}
	}
	if mode.State {
		snapshotKV, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path(snapshotDir + "/state").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return BucketConfigs[SnapshotType_state]
		}).Open()
		if err != nil {
			log.Error("Can't open state snapshot", "err", err)
			return nil, err
		} else { //nolint
			snkv = snkv.SnapshotDB([]string{dbutils.StateSnapshotInfoBucket, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket, dbutils.CodeBucket}, snapshotKV)
		}
	}
	return snkv.Open(), nil
}

func WrapBySnapshotsFromDownloader(kv ethdb.RwKV, snapshots map[SnapshotType]*SnapshotsInfo) (ethdb.RwKV, error) {
	snKV := ethdb.NewSnapshotKV().DB(kv)
	for k, v := range snapshots {
		log.Info("Wrap db by", "snapshot", k.String(), "dir", v.Dbpath)
		cfg := BucketConfigs[k]
		fmt.Println("Wrap", filepath.Dir(v.Dbpath))
		spew.Dump(cfg)
		snapshotKV, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path(filepath.Dir(v.Dbpath)).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return cfg
		}).Open()

		if err != nil {
			log.Error("Can't open snapshot", "err", err)
			return nil, err
		} else { //nolint
			buckets := make([]string, 0, 1)
			for bucket := range BucketConfigs[k] {
				buckets = append(buckets, bucket)
			}

			snKV = snKV.SnapshotDB(buckets, snapshotKV)
		}
	}

	return snKV.Open(), nil
}
