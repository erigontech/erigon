package commands

import (
	"context"
	"path/filepath"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core/state/historyv2read"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/torquem-ch/mdbx-go/mdbx"
	"golang.org/x/sync/semaphore"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
)

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for Erigon",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if chaindata == "" {
			chaindata = filepath.Join(datadirCli, "chaindata")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		defer debug.Exit()
	},
}

func RootCommand() *cobra.Command {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)
	return rootCmd
}

func dbCfg(label kv.Label, path string) kv2.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	opts := kv2.NewMDBX(log.New()).Path(path).Label(label).RoTxsLimiter(limiterB)
	if label == kv.ChainDB {
		opts = opts.MapSize(8 * datasize.TB)
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}
	return opts
}

func openDB(opts kv2.MdbxOpts, applyMigrations bool, logger log.Logger) (kv.RwDB, error) {
	// integration tool don't intent to create db, then easiest way to open db - it's pass mdbx.Accede flag, which allow
	// to read all options from DB, instead of overriding them
	opts = opts.Flags(func(f uint) uint { return f | mdbx.Accede })

	db := opts.MustOpen()
	if applyMigrations {
		migrator := migrations.NewMigrator(opts.GetLabel())
		has, err := migrator.HasPendingMigrations(db)
		if err != nil {
			return nil, err
		}
		if has {
			logger.Info("Re-Opening DB in exclusive mode to apply DB migrations")
			db.Close()
			db = opts.Exclusive().MustOpen()
			if err := migrator.Apply(db, datadirCli, logger); err != nil {
				return nil, err
			}
			db.Close()
			db = opts.MustOpen()
		}
	}

	if opts.GetLabel() == kv.ChainDB {
		var h3 bool
		var err error
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			h3, err = kvcfg.HistoryV3.Enabled(tx)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, err
		}
		if h3 {
			_, agg := allSnapshots(context.Background(), db)
			tdb, err := temporal.New(db, agg, accounts.ConvertV3toV2, historyv2read.RestoreCodeHash, accounts.DecodeIncarnationFromStorage, systemcontracts.SystemContractCodeLookup[chain])
			if err != nil {
				return nil, err
			}
			db = tdb
		}
	}

	return db, nil
}
