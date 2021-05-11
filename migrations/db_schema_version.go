package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var dbSchemaVersion = Migration{
	Name: "db_schema_version",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		return CommitProgress(db, nil, true)
	},
}
