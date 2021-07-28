package snapshotsync

import (
	"context"
	"os"

	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbx"
	"github.com/ledgerwatch/erigon/log"
)

func CreateStateSnapshot(ctx context.Context, snapshotPath string, logger log.Logger) (kv.RwDB, error) {
	// remove created snapshot if it's not saved in main db(to avoid append error)
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return nil, err
	}

	return mdbx.NewMDBX(logger).WithBucketsConfig(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return BucketConfigs[SnapshotType_state]
	}).Path(snapshotPath).Open()
}

func OpenStateSnapshot(dbPath string, logger log.Logger) (kv.RoDB, error) {
	return mdbx.NewMDBX(logger).Path(dbPath).WithBucketsConfig(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return BucketConfigs[SnapshotType_state]
	}).Readonly().Open()
}
