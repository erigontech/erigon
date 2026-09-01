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
	"fmt"
	"os"
	"path/filepath"

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
	for _, db := range dbs {
		if err := CompactInPlace(ctx, db.path, db.label, logger); err != nil {
			return fmt.Errorf("compacting %s: %w", db.path, err)
		}
	}
	return nil
}
