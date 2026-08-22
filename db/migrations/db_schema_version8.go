// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package migrations

import (
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

// dbSchemaVersion8 is a no-op migration whose sole purpose is to trigger an
// exclusive (non-accede) DB open for existing databases. When the DB is opened
// exclusively the MDBX wrapper creates any missing tables listed in
// ChaindataTables (Witnesses and WitnessSizes, renamed from BorWitnesses and
// BorWitnessSizes when Polygon support was removed).
// Without this migration, opening an existing chaindata in accede mode panics
// if a new table has been added to the schema since the DB was created.
//
// NOTE: the old BorWitnesses/BorWitnessSizes tables are left in place and are
// no longer read. Witnesses are a regenerable cache, so the rename starts from
// an empty table rather than copying rows across.
var dbSchemaVersion8 = Migration{
	Name: "db_schema_version8",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// No-op: the exclusive DB open triggered by the migration mechanism
		// is sufficient to create the renamed witness tables.
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
