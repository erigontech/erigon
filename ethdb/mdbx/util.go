package mdbx

import (
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/log"
	mdbxbind "github.com/torquem-ch/mdbx-go/mdbx"
)

func MustOpen(path string) kv.RwDB {
	db, err := Open(path, log.New(), false)
	if err != nil {
		panic(err)
	}
	return db
}

// Open - main method to open database.
func Open(path string, logger log.Logger, readOnly bool) (kv.RwDB, error) {
	var db kv.RwDB
	var err error
	opts := NewMDBX(logger).Path(path)
	if readOnly {
		opts = opts.Flags(func(flags uint) uint { return flags | mdbxbind.Readonly })
	}
	db, err = opts.Open()

	if err != nil {
		return nil, err
	}
	return db, nil
}
