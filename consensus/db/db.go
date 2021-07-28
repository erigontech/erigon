package db

import (
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbx"
	"github.com/ledgerwatch/log/v3"
)

func OpenDatabase(path string, logger log.Logger, inmem bool) kv.RwDB {
	opts := mdbx.NewMDBX(logger)
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
