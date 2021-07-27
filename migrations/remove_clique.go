package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon/ethdb/kv"
)

var removeCliqueBucket = Migration{
	Name: "remove_clique_bucket",
	Up: func(db kv.RwKV, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if exists, err := tx.ExistsBucket(kv.CliqueBucket); err != nil {
			return err
		} else if !exists {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()
		}

		if err := tx.DropBucket(kv.CliqueBucket); err != nil {
			return err
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
