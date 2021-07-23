package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
)

var removeCliqueBucket = Migration{
	Name: "remove_clique_bucket",
	Up: func(db ethdb.RwKV, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if exists, err := tx.ExistsBucket(dbutils.CliqueBucket); err != nil {
			return err
		} else if !exists {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()
		}

		if err := tx.DropBucket(dbutils.CliqueBucket); err != nil {
			return err
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
