package utils

import (
	"errors"
	"os"

	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
)

var ErrUnsupported error = errors.New("unsupported KV type")

func RmTmpFiles(snapshotPath string) error {
	return os.Remove(snapshotPath + "/mdbx.lck")
}

func OpenSnapshotKV(configsFunc kv.BucketConfigsFunc, path string) ethdb.RwKV {
	return kv.NewMDBX().WithBucketsConfig(configsFunc).Path(path).MustOpen()
}
