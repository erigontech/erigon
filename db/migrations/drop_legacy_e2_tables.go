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

// dropLegacyE2Tables drops the pre-E3 ("E2-era") plain/hashed state,
// changeset, and history index buckets. None of these are read by the E3
// execution path — kv.AccountsDomain / kv.StorageDomain / kv.CommitmentDomain
// are the active stores, and their keys are already incarnation-free
// ([addr]+[key]).
//
// PlainState, HashedStorage and the StorageChangeSet baked the per-account
// `incarnation` counter into the storage key; the others (HashedAccounts,
// AccountChangeSet, AccountHistory, StorageHistory) are dead E2 indices.
//
// The corresponding constants have been moved from kv.ChaindataTables to
// kv.ChaindataDeprecatedTables so the buckets are not recreated on next
// open. DropTable both empties any dormant E2 rows and removes the bucket
// metadata itself.
//
// The migration is idempotent — DropTable is a no-op on a bucket whose
// DBI is NonExistingDBI and that cannot be re-opened.
var dropLegacyE2Tables = Migration{
	Name: "drop_legacy_e2_tables",
	Up: func(db kv.RwDB, _ datadir.Dirs, _ []byte, BeforeCommit Callback, _ log.Logger) error {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for _, table := range []string{
			kv.PlainState,
			kv.HashedStorageDeprecated,
			kv.HashedAccountsDeprecated,
			kv.StorageChangeSetDeprecated,
			kv.AccountChangeSetDeprecated,
			kv.E2AccountsHistory,
			kv.E2StorageHistory,
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
