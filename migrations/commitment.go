package migrations

import (
	"context"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
)

var EnableSqueezeCommitmentFiles = false

var SqueezeCommitmentFiles = Migration{
	Name: "squeeze_commit_files",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		ctx := context.Background()
		log.Warn("[dbg] aaaa", "EnableSqueezeCommitmentFiles", EnableSqueezeCommitmentFiles, "libstate.AggregatorV3SqueezeCommitmentValues", libstate.AggregatorV3SqueezeCommitmentValues)
		if !EnableSqueezeCommitmentFiles || !libstate.AggregatorV3SqueezeCommitmentValues || !kvcfg.HistoryV3.FromDB(db) { //nolint:staticcheck
			return db.Update(ctx, func(tx kv.RwTx) error {
				return BeforeCommit(tx, nil, true)
			})
		}
		logger.Info("File migration is disabled", "name", "squeeze_commit_files")

		logEvery := time.NewTicker(10 * time.Second)
		defer logEvery.Stop()

		agg, err := libstate.NewAggregatorV3(ctx, dirs, ethconfig.HistoryV3AggregationStep, db, logger)
		if err != nil {
			return err
		}
		defer agg.Close()
		if err = agg.OpenFolder(false); err != nil {
			return err
		}

		ac := agg.MakeContext()
		defer ac.Close()
		if err = ac.SqueezeCommitmentFiles(); err != nil {
			return err
		}
		return db.Update(ctx, func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}
