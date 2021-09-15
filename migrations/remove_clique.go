package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
)

var removeCliqueBucket = Migration{
	Name: "remove_clique_bucket",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if exists, err := tx.ExistsBucket(kv.Clique); err != nil {
			return err
		} else if !exists {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()
		}

		if err := tx.DropBucket(kv.Clique); err != nil {
			return err
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
