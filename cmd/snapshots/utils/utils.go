package utils

import (
	"errors"
	"os"

	"github.com/ledgerwatch/erigon/ethdb"
)

var ErrUnsupported error = errors.New("unsupported KV type")

func RmTmpFiles(snapshotPath string) error {
	return os.Remove(snapshotPath + "/mdbx.lck")
}

func OpenSnapshotKV(configsFunc ethdb.BucketConfigsFunc, path string) ethdb.RwKV {
	return ethdb.NewMDBX().WithBucketsConfig(configsFunc).Path(path).MustOpen()
}
