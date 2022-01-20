package snapshotsync

/*
import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces/snapshotsync"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
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

func WrapBySnapshotsFromDownloader(db kv.RwDB, snapshots map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo) (kv.RwDB, error) {
	snKV := snapshotdb.NewSnapshotKV().DB(db)
	for k, v := range snapshots {
		log.Info("Wrap db by", "snapshot", k.String(), "dir", v.Dbpath)
		chainSnapshotCfg := BucketConfigs[k]
		snapshotKV, err := kv2.NewMDBX(log.New()).Readonly().Path(v.Dbpath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
			return chainSnapshotCfg
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
*/
