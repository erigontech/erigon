// Copyright 2024 The Erigon Authors
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

// dbSchemaVersion7 is a no-op migration whose sole purpose is to trigger an
// exclusive (non-accede) DB open for existing databases. When the DB is opened
// exclusively the MDBX wrapper creates any missing tables listed in
// ChaindataTables (the 10 GLOAS / EIP-7732 state tables: Builders,
// BuildersDump, BuilderPendingWithdrawals, BuilderPendingWithdrawalsDump,
// PayloadExpectedWithdrawals, PayloadExpectedWithdrawalsDump,
// ExecutionPayloadAvailability, BuilderPendingPayments, PtcWindow,
// LatestExecutionPayloadBid).
// Without this migration, opening an existing chaindata in accede mode panics
// if a new table has been added to the schema since the DB was created.
//
// NOTE: this migration only creates empty tables — it does NOT backfill rows
// for GLOAS-era slots that were already antiquated before the upgrade.
// ReadHistoricalState will return ErrMissingGloasData for those slots;
// the state antiquary handles this by backing off to an earlier (pre-GLOAS)
// slot and re-antiquating forward, which populates the missing tables.
var dbSchemaVersion7 = Migration{
	Name: "db_schema_version7",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// No-op: the exclusive DB open triggered by the migration mechanism
		// is sufficient to create any new tables (GLOAS / EIP-7732).
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
