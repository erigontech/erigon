package db

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
)

func OpenDatabase(path string, logger log.Logger, inmem bool, readonly bool) kv.RwDB {
	opts := mdbx.NewMDBX(logger).Label(kv.ConsensusDB)
	if readonly {
		opts = opts.Readonly()
	}
	if inmem {
		opts = opts.InMem()
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
