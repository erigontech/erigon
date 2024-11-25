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
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/erigon-lib/common/datadir"
	"github.com/erigontech/erigon/erigon-lib/common/dir"
	"github.com/erigontech/erigon/erigon-lib/downloader"
	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/erigon-lib/log/v3"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
)

var ProhibitNewDownloadsLock = Migration{
	Name: "prohibit_new_downloads_lock",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		snapshotsStageProgress, err := stages.GetStageProgress(tx, stages.Snapshots)
		if err != nil {
			return err
		}
		if snapshotsStageProgress > 0 {
			fPath := filepath.Join(dirs.Snap, downloader.ProhibitNewDownloadsFileName)
			exists, err := dir.FileExist(fPath)
			if err != nil {
				return err
			}
			if !exists {
				f, err := os.Create(fPath)
				if err != nil {
					return err
				}
				defer f.Close()
				if err := f.Sync(); err != nil {
					return err
				}
			}
		}

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
