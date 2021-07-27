package db

import (
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbxdb"
)

func OpenDatabase(path string, inmem bool) kv.RwKV {
	opts := mdbx.NewMDBX()
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
