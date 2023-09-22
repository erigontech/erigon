package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
)

func expandHomeDir(dirpath string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return dirpath
	}
	prefix := fmt.Sprintf("~%c", os.PathSeparator)
	if strings.HasPrefix(dirpath, prefix) {
		return filepath.Join(home, dirpath[len(prefix):])
	} else if dirpath == "~" {
		return home
	}
	return dirpath
}

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for Erigon",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		datadirCli = expandHomeDir(datadirCli)
		if chaindata == "" {
			chaindata = filepath.Join(datadirCli, "chaindata")
		} else {
			chaindata = expandHomeDir(chaindata)
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
	const (
		ThreadsLimit = 9_000
		DBSizeLimit  = 3 * datasize.TB
		DBPageSize   = 8 * datasize.KB
		GrowthStep   = 2 * datasize.GB
	)
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	opts := kv2.NewMDBX(log.New()).Path(path).Label(label).RoTxsLimiter(limiterB)
	if label == kv.ChainDB {
		opts = opts.MapSize(DBSizeLimit)
		opts = opts.PageSize(DBPageSize.Bytes())
		opts = opts.GrowthStep(GrowthStep)
	} else {
		opts = opts.GrowthStep(16 * datasize.MB)
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}

	// if db is not exists, we dont want to pass this flag since it will create db with maplimit of 1mb
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		// integration tool don't intent to create db, then easiest way to open db - it's pass mdbx.Accede flag, which allow
		// to read all options from DB, instead of overriding them
		opts = opts.Flags(func(f uint) uint { return f | mdbx.Accede })
	}

	return opts
}

func openDBDefault(opts kv2.MdbxOpts, applyMigrations, enableV3IfDBNotExists bool, logger log.Logger) (kv.RwDB, error) {
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
		if enableV3IfDBNotExists {
			logger.Info("history V3 is forcibly enabled")
			err := db.Update(context.Background(), func(tx kv.RwTx) error {
				if err := snap.ForceSetFlags(tx, ethconfig.BlocksFreezing{Enabled: true}); err != nil {
					return err
				}
				return kvcfg.HistoryV3.ForceWrite(tx, true)
			})
			if err != nil {
				return nil, err
			}
		}

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
			_, _, agg := allSnapshots(context.Background(), db, logger)
			tdb, err := temporal.New(db, agg, systemcontracts.SystemContractCodeLookup[chain])
			if err != nil {
				return nil, err
			}
			db = tdb
		}
	}
	return db, nil
}

func openDB(opts kv2.MdbxOpts, applyMigrations bool, logger log.Logger) (kv.RwDB, error) {
	return openDBDefault(opts, applyMigrations, ethconfig.EnableHistoryV3InTest, logger)
}

func openDBWithDefaultV3(opts kv2.MdbxOpts, applyMigrations bool, logger log.Logger) (kv.RwDB, error) {
	return openDBDefault(opts, applyMigrations, true, logger)
}
