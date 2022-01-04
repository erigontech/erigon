package db

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
)

func OpenDatabase(path string, logger log.Logger, inmem bool) kv.RwDB {
	log.Info("Opening consensus db", "path", path, "in memory", inmem)
	opts := mdbx.NewMDBX(logger)
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
