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
			ts := snaptype.AllTypes
			for _, t := range ts {
				if t.String() != snaptype.BlobSidecars.String() {
					locked = append(locked, t.String())
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
