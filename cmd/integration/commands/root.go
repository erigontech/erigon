package commands

import (
	"path"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/ethdb"
	kv2 "github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for Erigon",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := utils.SetupCobra(cmd); err != nil {
			panic(err)
		}
		if chaindata == "" {
			chaindata = path.Join(datadir, "erigon", "chaindata")
		}
		if snapshotDir == "" {
			snapshotDir = path.Join(datadir, "erigon", "snapshot")
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

func openDB(path string, applyMigrations bool) ethdb.RwKV {
	label := ethdb.Chain
	db := openKV(label, path, false)
	if applyMigrations {
		has, err := migrations.NewMigrator(label).HasPendingMigrations(db)
		if err != nil {
			panic(err)
		}
		if has {
			log.Info("Re-Opening DB in exclusive mode to apply DB migrations")
			db.Close()
			db = openKV(label, path, true)
			if err := migrations.NewMigrator(label).Apply(db, datadir); err != nil {
				panic(err)
			}
			db.Close()
			db = openKV(label, path, false)
		}
	}
	return db
}

func openKV(label ethdb.Label, path string, exclusive bool) ethdb.RwKV {
	opts := kv2.NewMDBX().Path(path).Label(label)
	if exclusive {
		opts = opts.Exclusive()
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(ethdb.DBVerbosityLvl(databaseVerbosity))
	}
	kv := opts.MustOpen()
	return kv
}
