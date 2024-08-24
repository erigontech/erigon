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
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
)

var EnableSqueezeCommitmentFiles = false

var SqueezeCommitmentFiles = Migration{
	Name: "squeeze_commit_files",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		ctx := context.Background()

		if !EnableSqueezeCommitmentFiles || !libstate.AggregatorSqueezeCommitmentValues { //nolint:staticcheck
			log.Info("[sqeeze_migration] disabled")
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

		agg, err := libstate.NewAggregator(ctx, dirs, config3.HistoryV3AggregationStep, db, nil, logger)
		if err != nil {
			return err
		}
		defer agg.Close()
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
		if err = agg.OpenFolder(); err != nil {
			return err
		}
		if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		ac := agg.BeginFilesRo()
		defer ac.Close()

		existsV2Dir, err := dir.Exist(dirs.SnapDomain + "_old")
		if err != nil {
			return err
		}
		if !existsV2Dir {
			log.Info("[sqeeze_migration] normal mode start")
			if err = ac.SqueezeCommitmentFiles(ac); err != nil {
				return err
			}
			ac.Close()
			if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
				return err
			}
			return db.Update(ctx, func(tx kv.RwTx) error {
				return BeforeCommit(tx, nil, true)
			})
		}

		{
			log.Info("[sqeeze_migration] `domain_old` folder found, using it as a target `domain`")
			dirs2 := dirs
			dirs2.SnapDomain += "_old"
			aggOld, err := libstate.NewAggregator(ctx, dirs2, config3.HistoryV3AggregationStep, db, nil, logger)
			if err != nil {
				panic(err)
			}
			defer aggOld.Close()
			if err = aggOld.OpenFolder(); err != nil {
				panic(err)
			}
			aggOld.SetCompressWorkers(estimate.CompressSnapshot.Workers())
			if err := aggOld.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
				return err
			}

			acOld := aggOld.BeginFilesRo()
			defer acOld.Close()

			if err = acOld.SqueezeCommitmentFiles(ac); err != nil {
				return err
			}
			acOld.Close()
			ac.Close()
			if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
				return err
			}
		}

		return db.Update(ctx, func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}
