package migrations

import (
	"context"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/downloader"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
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
			if !dir.FileExist(fPath) {
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
