package migrations

import (
	"context"

	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	smtdb "github.com/ledgerwatch/erigon/smt/pkg/db"
)

var refactorTableLastRoot = Migration{
	Name: "refactor_table_last_root",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		oldBucketName := "HermezSmtLastRoot"
		lastRootKey := []byte("lastRoot")

		// get last root
		lastRootAsBytes, err := tx.GetOne(oldBucketName, lastRootKey)
		if err != nil {
			if err = tx.CreateBucket(oldBucketName); err != nil {
				return err
			}

			lastRootAsBytes, err = tx.GetOne(oldBucketName, lastRootKey)
			if err != nil {
				return err
			}
		}

		// create new bucket
		err = tx.CreateBucket(smtdb.TableStats)
		if err != nil {
			return err
		}

		// set the last root to the new table
		if lastRootAsBytes != nil && len(lastRootAsBytes) > 0 {
			err = tx.Put(smtdb.TableStats, lastRootKey, lastRootAsBytes)
			if err != nil {
				return err
			}
		}

		// delete old bucket
		tx.DropBucket(oldBucketName)

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
