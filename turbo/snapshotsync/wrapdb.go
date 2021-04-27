package snapshotsync

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"time"
)

var (
	BucketConfigs = map[SnapshotType]dbutils.BucketsCfg{
		SnapshotType_bodies: {
			dbutils.BlockBodyPrefix:          dbutils.BucketConfigItem{},
			dbutils.EthTx:          dbutils.BucketConfigItem{},
		},
		SnapshotType_headers: {
			dbutils.HeadersBucket:             dbutils.BucketConfigItem{},
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
		snapshotKV, err := ethdb.NewLMDB().Readonly().Path(snapshotDir + "/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
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
		snapshotKV, err := ethdb.NewLMDB().Readonly().Path(snapshotDir + "/headers").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
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
		snapshotKV, err := ethdb.NewLMDB().Readonly().Path(snapshotDir + "/state").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return BucketConfigs[SnapshotType_headers]
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
		snapshotKV, err := ethdb.NewLMDB().Readonly().Path(v.Dbpath).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
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

func WrapSnapshots(chainDb ethdb.Database, snapshotsDir string)  error {
	snapshotBlock,err:=chainDb.Get(dbutils.BittorrentInfoBucket, dbutils.CurrentHeadersSnapshotBlock)
	if err!=nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}
	snKVOpts:=ethdb.NewSnapshotKV().DB(chainDb.(ethdb.HasRwKV).RwKV())
	if len(snapshotBlock)==8 {
		snKV, innerErr:=OpenHeadersSnapshot(SnapshotName(snapshotsDir, "headers", binary.BigEndian.Uint64(snapshotBlock)))
		if innerErr!=nil {
			return  innerErr
		}
		snKVOpts = snKVOpts.SnapshotDB([]string{dbutils.HeadersBucket}, snKV)
	}
	//manually wrap current db for snapshot generation
	chainDb.(ethdb.HasRwKV).SetRwKV(snKVOpts.Open())

	return nil
}



func DownloadSnapshots(torrentClient *Client, ExternalSnapshotDownloaderAddr string, networkID uint64, snapshotMode SnapshotMode, chainDb ethdb.Database) error {
	var downloadedSnapshots map[SnapshotType]*SnapshotsInfo
	if ExternalSnapshotDownloaderAddr != "" {
		cli, cl, innerErr := NewClient(ExternalSnapshotDownloaderAddr)
		if innerErr != nil {
			return  innerErr
		}
		defer cl() //nolint

		_, innerErr = cli.Download(context.Background(), &DownloadSnapshotRequest{
			NetworkId: networkID,
			Type:      snapshotMode.ToSnapshotTypes(),
		})
		if innerErr != nil {
			return innerErr
		}

		waitDownload := func() (map[SnapshotType]*SnapshotsInfo, error) {
			snapshotReadinessCheck := func(mp map[SnapshotType]*SnapshotsInfo, tp SnapshotType) bool {
				if mp[tp].Readiness != int32(100) {
					log.Info("Downloading", "snapshot", tp, "%", mp[tp].Readiness)
					return false
				}
				return true
			}
			for {
				downloadedSnapshots = make(map[SnapshotType]*SnapshotsInfo)
				snapshots, err1 := cli.Snapshots(context.Background(), &SnapshotsRequest{NetworkId: networkID})
				if err1 != nil {
					return nil, err1
				}
				for i := range snapshots.Info {
					if downloadedSnapshots[snapshots.Info[i].Type].SnapshotBlock < snapshots.Info[i].SnapshotBlock && snapshots.Info[i] != nil {
						downloadedSnapshots[snapshots.Info[i].Type] = snapshots.Info[i]
					}
				}

				downloaded := true
				if snapshotMode.Headers {
					if !snapshotReadinessCheck(downloadedSnapshots, SnapshotType_headers) {
						downloaded = false
					}
				}
				if snapshotMode.Bodies {
					if !snapshotReadinessCheck(downloadedSnapshots, SnapshotType_bodies) {
						downloaded = false
					}
				}
				if snapshotMode.State {
					if !snapshotReadinessCheck(downloadedSnapshots, SnapshotType_state) {
						downloaded = false
					}
				}
				if snapshotMode.Receipts {
					if !snapshotReadinessCheck(downloadedSnapshots, SnapshotType_receipts) {
						downloaded = false
					}
				}
				if downloaded {
					return downloadedSnapshots, nil
				}
				time.Sleep(time.Second * 10)
			}
		}
		downloadedSnapshots, innerErr := waitDownload()
		if innerErr != nil {
			return innerErr
		}
		snapshotKV := chainDb.(ethdb.HasRwKV).RwKV()

		snapshotKV, innerErr = WrapBySnapshotsFromDownloader(snapshotKV, downloadedSnapshots)
		if innerErr != nil {
			return innerErr
		}
		chainDb.(ethdb.HasRwKV).SetRwKV(snapshotKV)

		innerErr = PostProcessing(chainDb, downloadedSnapshots)
		if innerErr != nil {
			return innerErr
		}
	} else {
		err := torrentClient.Load(chainDb)
		if err != nil {
			return  err
		}
		err = torrentClient.AddSnapshotsTorrents(context.Background(), chainDb, networkID, snapshotMode)
		if err == nil {
			torrentClient.Download()
			var innerErr error
			snapshotKV := chainDb.(ethdb.HasRwKV).RwKV()
			downloadedSnapshots, innerErr := torrentClient.GetSnapshots(chainDb, networkID)
			if innerErr != nil {
				return innerErr
			}

			snapshotKV, innerErr = WrapBySnapshotsFromDownloader(snapshotKV, downloadedSnapshots)
			if innerErr != nil {
				return innerErr
			}
			chainDb.(ethdb.HasRwKV).SetRwKV(snapshotKV)
			tx, err := chainDb.Begin(context.Background(), ethdb.RW)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			innerErr = PostProcessing(chainDb, downloadedSnapshots)
			if err = tx.Commit(); err != nil {
				return err
			}
			if innerErr != nil {
				return innerErr
			}
		} else {
			log.Error("There was an error in snapshot init. Swithing to regular sync", "err", err)
		}
	}
	return nil
}