package db

import (
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbxdb"
)

func OpenDatabase(path string, inmem bool) kv.RwDB {
	opts := mdbx.NewMDBX()
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
