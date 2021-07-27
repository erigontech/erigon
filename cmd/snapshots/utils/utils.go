package utils

import (
	"errors"
	"os"

	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbxdb"
)

var ErrUnsupported error = errors.New("unsupported KV type")

func RmTmpFiles(snapshotPath string) error {
	return os.Remove(snapshotPath + "/mdbx.lck")
}

func OpenSnapshotKV(configsFunc mdbx.BucketConfigsFunc, path string) kv.RwDB {
	return mdbx.NewMDBX().WithBucketsConfig(configsFunc).Path(path).MustOpen()
}
