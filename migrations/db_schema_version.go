package migrations

import (
	"context"

	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

var dbSchemaVersion5 = Migration{
	Name: "db_schema_version5",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
