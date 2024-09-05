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
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
)

var EnableCodeRecompress = false

var RecompressCodeFiles = Migration{
	Name: "code_recompress",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		ctx := context.Background()

		if !EnableCodeRecompress {
			log.Info("[recompress_code_migration] disabled")
			return db.Update(ctx, func(tx kv.RwTx) error {
				return BeforeCommit(tx, nil, true)
			})
		}

		logEvery := time.NewTicker(10 * time.Second)
		defer logEvery.Stop()
		t := time.Now()
		defer func() {
			log.Info("[sqeeze_migration] done", "took", time.Since(t))
		}()

		agg, err := state.NewAggregator(ctx, dirs, config3.HistoryV3AggregationStep, db, nil, logger)
		if err != nil {
			return err
		}
		defer agg.Close()
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())

		log.Info("[sqeeze_migration] start")
		for _, f := range domainFiles(dirs, kv.CodeDomain) {
			_, fileName := filepath.Split(f)
			fromStep, toStep, err := state.ParseStepsFromFileName(fileName)
			if err != nil {
				return err
			}
			if toStep-fromStep < state.DomainMinStepsToCompress {
				continue
			}
			from := filepath.Join(dirs.Tmp, fileName)
			to := filepath.Join(dirs.Snap, fileName)
			if err := agg.Sqeeze(ctx, kv.CodeDomain, from, to); err != nil {
				return err
			}
			_ = os.Remove(strings.ReplaceAll(to, ".kv", ".bt"))
			_ = os.Remove(strings.ReplaceAll(to, ".kv", ".kvei"))
			_ = os.Remove(strings.ReplaceAll(to, ".kv", ".bt.torrent"))
			_ = os.Remove(strings.ReplaceAll(to, ".kv", ".kv.torrent"))
		}
		return db.Update(ctx, func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}
