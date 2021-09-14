package snapshotsync

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"os"
	"github.com/ledgerwatch/log/v3"



)

func CreateStateSnapshot(ctx context.Context, snapshotPath string, logger log.Logger) (kv.RwDB, error) {
	// remove created snapshot if it's not saved in main db(to avoid append error)
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return nil, err
	}

	return mdbx.NewMDBX(logger).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return BucketConfigs[SnapshotType_state]
	}).Path(snapshotPath).DBVerbosity(kv.DBVerbosityLvl(2)).Open()
}

func OpenStateSnapshot(dbPath string, logger log.Logger) (kv.RoDB, error) {
	return mdbx.NewMDBX(logger).Path(dbPath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return BucketConfigs[SnapshotType_state]
	}).Readonly().Open()
}
