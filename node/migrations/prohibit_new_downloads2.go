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
	"io/fs"
	"os"
	"path/filepath"

	coresnaptype "github.com/erigontech/erigon-db/snaptype"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/downloader"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/fastjson"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall"
)

// Switch to the second version of download.lock.
var ProhibitNewDownloadsLock2 = Migration{
	Name: "prohibit_new_downloads_lock2",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		fPath := filepath.Join(dirs.Snap, downloader.ProhibitNewDownloadsFileName)
		exists, err := dir.FileExist(fPath)
		if err != nil {
			return err
		}
		if !exists {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()

		}
		content, err := os.ReadFile(fPath)
		if err != nil {
			return err
		}
		if len(content) == 0 { // old format, need to change to all snaptypes except blob sidecars
			locked := []string{}

			for _, t := range coresnaptype.BlockSnapshotTypes {
				locked = append(locked, t.Name())
			}

			for _, t := range coresnaptype.E3StateTypes {
				locked = append(locked, t.Name())
			}

			for _, t := range heimdall.SnapshotTypes() {
				locked = append(locked, t.Name())
			}

			for _, t := range snaptype.CaplinSnapshotTypes {
				if t.Name() != snaptype.BlobSidecars.Name() {
					locked = append(locked, t.Name())
				}
			}

			newContent, err := fastjson.Marshal(locked)
			if err != nil {
				return err
			}
			if err := os.WriteFile(fPath, newContent, fs.FileMode(os.O_TRUNC|os.O_WRONLY)); err != nil {
				return err
			}
		}

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
