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
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	snaptype2 "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var EnableBlocksRecompress = false

var RecompressBlocksFiles = Migration{
	Name: "blocks_recompress",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		ctx := context.Background()

		if !EnableBlocksRecompress {
			log.Info("[recompress_migration] disabled")
			return db.Update(ctx, func(tx kv.RwTx) error {
				return BeforeCommit(tx, nil, true)
			})
		}

		logEvery := time.NewTicker(10 * time.Second)
		defer logEvery.Stop()
		t := time.Now()
		defer func() {
			log.Info("[recompress_migration] done", "took", time.Since(t))
		}()

		log.Info("[recompress_migration] start")
		files, err := dir.ListFiles(dirs.Snap, ".seg")
		if err != nil {
			return err
		}
		for _, to := range files {
			good := strings.Contains(to, snaptype2.Transactions.Name()) ||
				strings.Contains(to, snaptype2.Headers.Name())
			if !good {
				continue
			}
			_, name := filepath.Split(to)
			in, _, ok := snaptype.ParseFileName(dirs.Snap, name)
			if !ok {
				continue
			}
			good = in.To-in.From == snaptype.Erigon2OldMergeLimit || in.To-in.From == snaptype.Erigon2MergeLimit
			if !good {
				continue
			}
			tempFileCopy := filepath.Join(dirs.Snap, name)
			if err := datadir.CopyFile(to, tempFileCopy); err != nil {
				return err
			}
			if err := freezeblocks.SqeezeBlocks(ctx, dirs, tempFileCopy, to, logger); err != nil {
				return err
			}
			_ = os.Remove(strings.ReplaceAll(to, ".seg", ".seg.torrent"))
			_ = os.Remove(strings.ReplaceAll(to, ".seg", ".idx"))
			_ = os.Remove(strings.ReplaceAll(to, ".seg", ".idx.torrent"))
		}
		return db.Update(ctx, func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}
