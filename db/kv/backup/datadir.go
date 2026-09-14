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

package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/mdbx-go/mdbx"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
)

type datadirDB struct {
	path  string
	label kv.Label
}

// caplin/blobs/chaindata is the deepest db a datadir holds.
const maxDBDepth = 2

// datadirDBs finds the mdbx databases of a datadir. It only descends into the
// top-level dirs erigon puts a db under - never into snapshots/ or temp/ - and
// takes the label from that dir.
func datadirDBs(dirs datadir.Dirs) ([]datadirDB, error) {
	roots := []datadirDB{
		{dirs.Chaindata, dbcfg.ChainDB},
		{filepath.Join(dirs.DataDir, "aura"), dbcfg.ConsensusDB},
		{dirs.TxPool, dbcfg.TxPoolDB},
		{dirs.Downloader, dbcfg.DownloaderDB},
		{dirs.Migrations, dbcfg.MigrationsDB},
		{dirs.Nodes, dbcfg.SentryDB},
		{filepath.Join(dirs.DataDir, "caplin"), dbcfg.CaplinDB},
	}
	var found []datadirDB
	for _, root := range roots {
		if err := findDBs(root.path, root.label, maxDBDepth, &found); err != nil {
			return nil, err
		}
	}
	return found, nil
}

func findDBs(path string, label kv.Label, depth int, found *[]datadirDB) error {
	exists, err := dir.FileExist(filepath.Join(path, dataFileName))
	if err != nil {
		return err
	}
	if exists {
		*found = append(*found, datadirDB{path, label})
		return nil
	}
	if depth == 0 {
		return nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if err := findDBs(filepath.Join(path, e.Name()), label, depth-1, found); err != nil {
			return err
		}
	}
	return nil
}

// bloatRatio is how many times the free pages of a db must outweigh its data
// before AutoCompactDatadir rewrites it.
const bloatRatio = 3

// autoCompactMinFree skips a small db: it crosses bloatRatio with a few free
// pages, and the growth step pads its compacted file back to the same size.
var autoCompactMinFree = datasize.GB

// ApplyMigrations upgrades an old datadir layout and compacts bloated dbs. A
// datadir locked by another process is skipped.
func ApplyMigrations(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	unlock, err := dirs.TryFlock()
	if errors.Is(err, datadir.ErrDataDirLocked) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unlock()

	if err := downloaderV2Migration(dirs); err != nil {
		return err
	}
	return autoCompactDatadir(ctx, dirs, logger)
}

// downloaderV2Migration moves the downloader db from snapshots/db to downloader/.
func downloaderV2Migration(dirs datadir.Dirs) error {
	from, to := filepath.Join(dirs.Snap, "db", dataFileName), filepath.Join(dirs.Downloader, dataFileName)
	if exists, err := dir.FileExist(from); err != nil || !exists {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return datadir.CopyFile(from, to) // the dirs may be on different disks
	}
	return nil
}

// autoCompactDatadir compacts each db of the datadir whose free pages exceed
// bloatRatio times its data and are at least autoCompactMinFree. A db that fails
// to compact is left as it was. The caller holds the datadir lock.
func autoCompactDatadir(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	dbs, err := datadirDBs(dirs)
	if err != nil {
		return err
	}
	for _, db := range dbs {
		data, free, err := pageUsage(db.path)
		if err != nil {
			logger.Warn("[compact] can't read db page usage", "db", db.path, "err", err)
			continue
		}
		if free <= bloatRatio*data || free < autoCompactMinFree {
			continue
		}
		logger.Info("[compact] auto-compact", "db", db.path, "data", data.HR(), "free", free.HR())
		if err := CompactInPlace(ctx, db.path, db.label, logger); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Warn("[compact] auto-compact failed, db left as it was", "db", db.path, "err", err)
		}
	}
	return nil
}

// pageUsage returns the bytes held by the tables of a db and the bytes of free
// pages below its last used page. The unallocated tail of the file is not free
// space: mdbx grows the file ahead of use, so counting it would compact a
// freshly compacted db again.
func pageUsage(dbDir string) (data, free datasize.ByteSize, err error) {
	env, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		return 0, 0, err
	}
	defer env.Close()
	if err := env.Open(dbDir, mdbx.Readonly, 0o644); err != nil {
		return 0, 0, err
	}
	st, err := env.Stat()
	if err != nil {
		return 0, 0, err
	}
	info, err := env.Info(nil)
	if err != nil {
		return 0, 0, err
	}
	pageSize := datasize.ByteSize(st.PSize)
	data = datasize.ByteSize(st.BranchPages+st.LeafPages+st.OverflowPages) * pageSize
	used := datasize.ByteSize(info.MiLastPgNo+1) * pageSize
	return data, used - min(used, data), nil
}

// CompactDatadir compacts every mdbx db of the datadir in place. It takes the
// datadir lock, so a running node fails the call instead of losing its db.
func CompactDatadir(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	dirs, l, err := dirs.MustFlock()
	if err != nil {
		return err
	}
	defer func() {
		if err := l.Unlock(); err != nil {
			logger.Error("failed to unlock datadir", "err", err)
		}
	}()

	dbs, err := datadirDBs(dirs)
	if err != nil {
		return err
	}
	if len(dbs) == 0 {
		return fmt.Errorf("no mdbx database found under %s", dirs.DataDir)
	}
	for _, db := range dbs {
		if err := CompactInPlace(ctx, db.path, db.label, logger); err != nil {
			return fmt.Errorf("compacting %s: %w", db.path, err)
		}
	}
	return nil
}
