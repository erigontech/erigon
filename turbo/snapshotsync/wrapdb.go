package snapshotsync

import (
	"context"
	"errors"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/snapshotsync"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
	"github.com/ledgerwatch/log/v3"
)

var (
	BucketConfigs = map[snapshotsync.SnapshotType]kv.TableCfg{
		snapshotsync.SnapshotType_bodies: {
			kv.BlockBody: kv.TableCfgItem{},
			kv.EthTx:     kv.TableCfgItem{},
		},
		snapshotsync.SnapshotType_headers: {
			kv.Headers: kv.TableCfgItem{},
		},
		snapshotsync.SnapshotType_state: {
			kv.PlainState: kv.TableCfgItem{
				Flags:                     kv.DupSort,
				AutoDupSortKeysConversion: true,
				DupFromLen:                60,
				DupToLen:                  28,
			},
			kv.PlainContractCode: kv.TableCfgItem{},
			kv.Code:              kv.TableCfgItem{},
		},
	}
)

//nolint
func WrapBySnapshotsFromDir(kv kv.RwDB, snapshotDir string, mode SnapshotMode) (kv.RwDB, error) {
	//todo remove it
	return nil, errors.New("deprecated") //nolint
}

func WrapBySnapshotsFromDownloader(db kv.RwDB, snapshots map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo) (kv.RwDB, error) {
	snKV := snapshotdb.NewSnapshotKV().DB(db)
	for k, v := range snapshots {
		log.Info("Wrap db by", "snapshot", k.String(), "dir", v.Dbpath)
		cfg := BucketConfigs[k]
		snapshotKV, err := kv2.NewMDBX(log.New()).Readonly().Path(v.Dbpath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
			return cfg
		}).Open()

		if err != nil {
			log.Error("Can't open snapshot", "err", err)
			return nil, err
		} else { //nolint
			switch k {
			case snapshotsync.SnapshotType_headers:
				snKV = snKV.HeadersSnapshot(snapshotKV)
			case snapshotsync.SnapshotType_bodies:
				snKV = snKV.BodiesSnapshot(snapshotKV)
			case snapshotsync.SnapshotType_state:
				snKV = snKV.StateSnapshot(snapshotKV)
			}
		}
	}

	return snKV.Open(), nil
}

func DownloadSnapshots(torrentClient *Client, ExternalSnapshotDownloaderAddr string, networkID uint64, snapshotMode SnapshotMode, chainDb ethdb.Database) error {
	var downloadedSnapshots map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo
	if ExternalSnapshotDownloaderAddr != "" {
		cli, cl, innerErr := NewClient(ExternalSnapshotDownloaderAddr)
		if innerErr != nil {
			return innerErr
		}
		defer cl() //nolint

		_, innerErr = cli.Download(context.Background(), &snapshotsync.DownloadSnapshotRequest{
			NetworkId: networkID,
			Type:      snapshotMode.ToSnapshotTypes(),
		})
		if innerErr != nil {
			return innerErr
		}

		waitDownload := func() (map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo, error) {
			snapshotReadinessCheck := func(mp map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo, tp snapshotsync.SnapshotType) bool {
				if mp[tp].Readiness != int32(100) {
					log.Info("Downloading", "snapshot", tp, "%", mp[tp].Readiness)
					return false
				}
				return true
			}
			for {
				downloadedSnapshots = make(map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo)
				snapshots, err1 := cli.Snapshots(context.Background(), &snapshotsync.SnapshotsRequest{NetworkId: networkID})
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
					if !snapshotReadinessCheck(downloadedSnapshots, snapshotsync.SnapshotType_headers) {
						downloaded = false
					}
				}
				if snapshotMode.Bodies {
					if !snapshotReadinessCheck(downloadedSnapshots, snapshotsync.SnapshotType_bodies) {
						downloaded = false
					}
				}
				if snapshotMode.State {
					if !snapshotReadinessCheck(downloadedSnapshots, snapshotsync.SnapshotType_state) {
						downloaded = false
					}
				}
				if snapshotMode.Receipts {
					if !snapshotReadinessCheck(downloadedSnapshots, snapshotsync.SnapshotType_receipts) {
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

		if err := PostProcessing(chainDb.RwKV(), downloadedSnapshots); err != nil {
			return err
		}

	} else {
		if err := chainDb.RwKV().Update(context.Background(), func(tx kv.RwTx) error {
			err := torrentClient.Load(tx)
			if err != nil {
				return err
			}
			return torrentClient.AddSnapshotsTorrents(context.Background(), tx, networkID, snapshotMode)
		}); err != nil {
			log.Error("There was an error in snapshot init. Swithing to regular sync", "err", err)
		} else {
			torrentClient.Download()
			var innerErr error
			var downloadedSnapshots map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo
			if err := chainDb.RwKV().View(context.Background(), func(tx kv.Tx) (err error) {
				downloadedSnapshots, err = torrentClient.GetSnapshots(tx, networkID)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}

			snapshotKV := chainDb.(ethdb.HasRwKV).RwKV()
			snapshotKV, innerErr = WrapBySnapshotsFromDownloader(snapshotKV, downloadedSnapshots)
			if innerErr != nil {
				return innerErr
			}
			chainDb.(ethdb.HasRwKV).SetRwKV(snapshotKV)
			if err := PostProcessing(snapshotKV, downloadedSnapshots); err != nil {

				return err
			}
		}

	}
	return nil
}
