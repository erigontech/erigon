package migrations

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/log/v3"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
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
