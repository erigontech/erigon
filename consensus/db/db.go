package db

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func OpenDatabase(path string, inmem bool, mdbx bool) *ethdb.ObjectDatabase {
	return ethdb.NewObjectDatabase(openKV(path, inmem, mdbx))
}

func openKV(path string, inmem bool, mdbx bool) ethdb.RwKV {
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
