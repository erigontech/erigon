package db

import (
	"github.com/ledgerwatch/erigon/ethdb"
)

func OpenDatabase(path string, inmem bool, mdbx bool) ethdb.RwKV {
	if mdbx {
		opts := ethdb.NewMDBX()
		if inmem {
			opts = opts.InMem()
		} else {
			opts = opts.Path(path)
		}

		return opts.MustOpen()
	}
	opts := ethdb.NewLMDB()
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()

}
