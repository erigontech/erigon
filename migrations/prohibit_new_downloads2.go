package migrations

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	coresnaptype "github.com/ledgerwatch/erigon/core/snaptype"
	borsnaptype "github.com/ledgerwatch/erigon/polygon/bor/snaptype"
	"github.com/ledgerwatch/log/v3"
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
		if !dir.FileExist(fPath) {
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

			for _, t := range borsnaptype.BorSnapshotTypes {
				locked = append(locked, t.Name())
			}

			for _, t := range snaptype.CaplinSnapshotTypes {
				if t.Name() != snaptype.BlobSidecars.Name() {
					locked = append(locked, t.Name())
				}
			}

			newContent, err := json.Marshal(locked)
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
