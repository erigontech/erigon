package migrations

import (
	"context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

var BorWitnessTables = Migration{
	Name: "bor_witness_tables",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}

		if err := tx.CreateTable(kv.BorWitnesses); err != nil {
			return err
		}
		if err := tx.CreateTable(kv.BorWitnessSizes); err != nil {
			return err
		}

		return tx.Commit()
	},
}
