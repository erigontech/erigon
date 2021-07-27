package snapshotsync

import (
	"context"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"os"
)

func CreateStateSnapshot(ctx context.Context, snapshotPath string) (ethdb.RwKV, error) {
	// remove created snapshot if it's not saved in main db(to avoid append error)
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return nil, err
	}

	return kv.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return BucketConfigs[SnapshotType_state]
	}).Path(snapshotPath).Open()
}

func OpenStateSnapshot(dbPath string) (ethdb.RoKV, error) {
	return kv.NewMDBX().Path(dbPath).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return BucketConfigs[SnapshotType_state]
	}).Readonly().Open()
}
