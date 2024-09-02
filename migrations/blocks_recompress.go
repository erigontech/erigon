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
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/state"
	snaptype2 "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	snaptype3 "github.com/erigontech/erigon/polygon/bor/snaptype"
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
		files, err := blocksFiles(dirs)
		if err != nil {
			return err
		}
		for _, from := range files {
			good := strings.Contains(from, snaptype2.Transactions.Name()) ||
				strings.Contains(from, snaptype2.Headers.Name()) ||
				strings.Contains(from, snaptype3.BorEvents.Name())
			if !good {
				continue
			}
			in, _, ok := snaptype.ParseFileName("", from)
			if !ok {
				continue
			}
			good = in.To-in.From == snaptype.Erigon2OldMergeLimit || in.To-in.From == snaptype.Erigon2MergeLimit
			if !good {
				continue
			}
			to := from
			if err := recompressBlocks(ctx, dirs, from, to, logger); err != nil {
				return err
			}
			_ = os.Remove(strings.ReplaceAll(to, ".seg", ".idx"))
		}
		return db.Update(ctx, func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}

func recompressBlocks(ctx context.Context, dirs datadir.Dirs, from, to string, logger log.Logger) error {
	logger.Info("[recompress] file", "f", to)
	decompressor, err := seg.NewDecompressor(from)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.EnableReadAhead().DisableReadAhead()
	r := state.NewArchiveGetter(decompressor.MakeGetter(), state.DetectCompressType(decompressor.MakeGetter()))

	compressCfg := freezeblocks.BlockCompressCfg
	compressCfg.Workers = estimate.CompressSnapshot.Workers()
	c, err := seg.NewCompressor(ctx, "recompress", to, dirs.Tmp, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	w := state.NewArchiveWriter(c, state.CompressKeys|state.CompressVals)
	var k, v []byte
	var i int
	for r.HasNext() {
		i++
		k, _ = r.Next(k[:0])
		v, _ = r.Next(v[:0])
		if err = w.AddWord(k); err != nil {
			return err
		}
		if err = w.AddWord(v); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := c.Compress(); err != nil {
		return err
	}

	return nil
}

func blocksFiles(dirs datadir.Dirs) ([]string, error) {
	return dir.ListFiles(dirs.Snap, ".seg")
}
