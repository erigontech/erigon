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

// dropBorWitnessTables drops the BorWitnesses / BorWitnessSizes buckets, which
// the WIT side-protocol now stores under kv.Witnesses / kv.WitnessSizes.
//
// Nothing is copied across. On main the witness-processing stage was built only
// when chainConfig.Bor != nil, so every row in these buckets belongs to a
// Polygon datadir, and Polygon is no longer a supported chain.
//
// Registering this migration also forces the exclusive (non-accede) DB open
// that creates the renamed buckets. Without it, opening an existing chaindata
// in accede mode fails with `db-table doesn't exists: Witnesses` — MDBX cannot
// create a table in accede mode, and the accede consumers (rpcdaemon,
// integration, downloader, backup, snapshots) all take that path.
//
// The migration is idempotent — DropTable is a no-op on a bucket whose DBI is
// NonExistingDBI and that cannot be re-opened.
var dropBorWitnessTables = Migration{
	Name: "drop_bor_witness_tables",
	Up: func(db kv.RwDB, _ datadir.Dirs, _ []byte, BeforeCommit Callback, _ log.Logger) error {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for _, table := range []string{
			kv.BorWitnessesDeprecated,
			kv.BorWitnessSizesDeprecated,
		} {
			if err := tx.DropTable(table); err != nil {
				return err
			}
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
