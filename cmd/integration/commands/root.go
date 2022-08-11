package commands

import (
	"path/filepath"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/torquem-ch/mdbx-go/mdbx"
)

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for Erigon",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := utils.SetupCobra(cmd); err != nil {
			panic(err)
		}
		if chaindata == "" {
			chaindata = filepath.Join(datadirCli, "chaindata")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		defer utils.StopDebug()
	},
}

func RootCommand() *cobra.Command {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
	return rootCmd
}

func dbCfg(label kv.Label, logger log.Logger, path string) kv2.MdbxOpts {
	opts := kv2.NewMDBX(logger).Path(path).Label(label)
	if label == kv.ChainDB {
		opts = opts.MapSize(8 * datasize.TB)
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}
	return opts
}

func openDB(opts kv2.MdbxOpts, applyMigrations bool) kv.RwDB {
	// integration tool don't intent to create db, then easiest way to open db - it's pass mdbx.Accede flag, which allow
	// to read all options from DB, instead of overriding them
	opts = opts.Flags(func(f uint) uint { return f | mdbx.Accede })
	db := opts.MustOpen()
	if applyMigrations {
		migrator := migrations.NewMigrator(opts.GetLabel())
		has, err := migrator.HasPendingMigrations(db)
		if err != nil {
			panic(err)
		}
		if has {
			log.Info("Re-Opening DB in exclusive mode to apply DB migrations")
			db.Close()
			db = opts.Exclusive().MustOpen()
			if err := migrator.Apply(db, datadirCli, snapdirCli); err != nil {
				panic(err)
			}
			db.Close()
			db = opts.MustOpen()
		}
	}
	return db
}
