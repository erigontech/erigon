package db

import (
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
)

func OpenDatabase(path string, inmem bool) ethdb.RwKV {
	opts := kv.NewMDBX()
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
