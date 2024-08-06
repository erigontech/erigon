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

	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	libstate "github.com/erigontech/erigon-lib/state"
)

var EnableSqueezeCommitmentFiles = false

var SqueezeCommitmentFiles = Migration{
	Name: "squeeze_commit_files",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		ctx := context.Background()

		if !EnableSqueezeCommitmentFiles || !libstate.AggregatorSqueezeCommitmentValues { //nolint:staticcheck
			return db.Update(ctx, func(tx kv.RwTx) error {
				return BeforeCommit(tx, nil, true)
			})
		}
		logger.Info("File migration is disabled", "name", "squeeze_commit_files")

		logEvery := time.NewTicker(10 * time.Second)
		defer logEvery.Stop()

		agg, err := libstate.NewAggregator(ctx, dirs, config3.HistoryV3AggregationStep, db, nil, logger)
		if err != nil {
			return err
		}
		defer agg.Close()
		if err = agg.OpenFolder(); err != nil {
			return err
		}

		ac := agg.BeginFilesRo()
		defer ac.Close()
		if err = ac.SqueezeCommitmentFiles(); err != nil {
			return err
		}
		return db.Update(ctx, func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}
