package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
)

var resetBlocks = Migration{
	Name: "db_schema_version5",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		enabled, err := snap.Enabled(tx)
		if err != nil {
			return err
		}

		if !enabled {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return
		}

		if err := rawdb.ResetBlocks(tx); err != nil {
			return err
		}

		if err := rawdb.ResetSenders(tx); err != nil {
			return err
		}

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
