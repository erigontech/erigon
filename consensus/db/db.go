//+build mdbx lmdb

package db

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func OpenDatabase(path string, inmem bool, mdbx bool) *ethdb.ObjectDatabase {
	db := ethdb.NewObjectDatabase(openKV(path, inmem, mdbx))
	return db
}

func openKV(path string, inmem bool, mdbx bool) ethdb.KV {
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
