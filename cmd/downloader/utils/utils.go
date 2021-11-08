package utils

import (
	"errors"
	"os"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
)

var ErrUnsupported error = errors.New("unsupported KV type")

func RmTmpFiles(snapshotPath string) error {
	return os.Remove(snapshotPath + "/mdbx.lck")
}

func OpenSnapshotKV(configsFunc mdbx.TableCfgFunc, path string) kv.RwDB {
	return mdbx.NewMDBX(log.New()).WithTablessCfg(configsFunc).Path(path).MustOpen()
}
