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

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

// HistoryTableFormatChange removes chaindata so it gets rebuilt with the new history table format:
//   - DataTable (formerly HistoryKeys): txNum+key → prev_value (non-DupSort, sequential writes)
//   - InvIndexTable (formerly HistoryVals): key → txNum (DupSort, no embedded value)
var HistoryTableFormatChange = Migration{
	Name: "history_table_format_change",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		if err := dir.RemoveAll(dirs.Chaindata); err != nil {
			return err
		}
		return nil
	},
}
