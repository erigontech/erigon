package db

import (
	"github.com/ledgerwatch/erigon/ethdb"
)

func OpenDatabase(path string, inmem bool) ethdb.RwKV {
	opts := ethdb.NewMDBX()
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
