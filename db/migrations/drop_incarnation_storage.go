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

// dropIncarnationFromStorage clears the legacy E2 plain/hashed state and
// changeset tables whose storage keys bake the per-account `incarnation`
// counter into the composite key ([addr]+[inc]+[key] in PlainState's
// dupsort layout; [hash(addr)]+[inc]+[hash(key)] in HashedStorage).
// None of these tables are read by the E3 execution path — kv.StorageDomain
// is the active store and its keys are already incarnation-free
// ([addr]+[key]).
//
// On a database originally created under E2 these tables may still hold
// dormant rows; we drop them outright here. The follow-up commit removes
// the table constants from kv.ChaindataTables so the empty buckets are
// not recreated on next open.
//
// The migration is idempotent — ClearTable is a no-op on already-empty
// tables.
var dropIncarnationFromStorage = Migration{
	Name: "drop_incarnation_from_storage",
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
		} {
			if err := tx.ClearTable(table); err != nil {
				return err
			}
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
