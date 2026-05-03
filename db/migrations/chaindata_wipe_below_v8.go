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
	"fmt"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
)

// chaindataWipeMinMajor is the first DBSchemaVersion.Major that uses the new
// LargeValues two-table indirect layout (Code/RCache/Commitment domains:
// keysTable DupSort `bareKey -> invStep+seqID` + valsTable plain `seqID -> value`).
// A v7 chaindata cannot be migrated in place — its values reference seqIDs that
// don't exist on the old layout. Re-syncing live state from snapshots is the
// only safe path.
const chaindataWipeMinMajor uint32 = 8

var chaindataWipeBelowV8 = Migration{
	Name: "chaindata_wipe_below_v8",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) error {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		major, _, _, ok, err := rawdb.ReadDBSchemaVersion(tx)
		if err != nil {
			return fmt.Errorf("chaindata_wipe_below_v8: %w", err)
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		if !ok || major >= chaindataWipeMinMajor {
			return nil
		}

		chaindataPath := db.Path()
		logger.Warn("[migration] chaindata schema major below cutoff — wiping chaindata; live state will be re-synced from snapshots",
			"db_major", major, "cutoff_major", chaindataWipeMinMajor, "path", chaindataPath)
		db.Close()
		if err := dir.RemoveAll(chaindataPath); err != nil {
			return fmt.Errorf("chaindata_wipe_below_v8: remove %s: %w", chaindataPath, err)
		}
		return errDBWiped
	},
}
