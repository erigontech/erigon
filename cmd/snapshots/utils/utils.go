package utils

import (
	"errors"
	"os"

	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbx"
	"github.com/ledgerwatch/log/v3"
)

var ErrUnsupported error = errors.New("unsupported KV type")

func RmTmpFiles(snapshotPath string) error {
	return os.Remove(snapshotPath + "/mdbx.lck")
}

func OpenSnapshotKV(configsFunc mdbx.TableCfgFunc, path string) kv.RwDB {
	return mdbx.NewMDBX(log.New()).WithTablessCfg(configsFunc).Path(path).MustOpen()
}
