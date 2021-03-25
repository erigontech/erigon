//+build mdbx lmdb

package db

import (
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func OpenDatabase(path string, inmem bool) *ethdb.ObjectDatabase {
	db := ethdb.NewObjectDatabase(openKV(path, inmem))
	return db
}

func openKV(path string, inmem bool) ethdb.KV {
	if dbType == "lmdb" {
		opts := ethdb.NewLMDB()
		if inmem {
			opts = opts.InMem()
		} else {
			opts = opts.Path(path)
		}

		return opts.MustOpen()

	}

	opts := ethdb.NewMDBX()
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
