package commands

import (
	"path"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for turbo-geth",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := utils.SetupCobra(cmd); err != nil {
			panic(err)
		}
		if chaindata == "" {
			chaindata = path.Join(datadir, "tg", "chaindata")
		}
		if snapshotDir == "" {
			snapshotDir = path.Join(datadir, "tg", "snapshot")
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

func openDatabase2(path string, applyMigrations bool, snapshotDir string, snapshotMode snapshotsync.SnapshotMode) *ethdb.ObjectDatabase {
	db := ethdb.NewObjectDatabase(openKV(path, false))
	if applyMigrations {
		has, err := migrations.NewMigrator().HasPendingMigrations(db)
		if err != nil {
			panic(err)
		}
		if has {
			log.Info("Re-Opening DB in exclusive mode to apply DB migrations")
			db.Close()
			db = ethdb.NewObjectDatabase(openKV(path, true))
			if err := migrations.NewMigrator().Apply(db, datadir); err != nil {
				panic(err)
			}
			db.Close()
			db = ethdb.NewObjectDatabase(openKV(path, false))
		}
	}
	metrics.AddCallback(db.RwKV().CollectMetrics)
	return db
}

func openDatabase(path string, applyMigrations bool) *ethdb.ObjectDatabase {
	mode, err := snapshotsync.SnapshotModeFromString(snapshotMode)
	if err != nil {
		panic(err)
	}
	return openDatabase2(path, applyMigrations, snapshotDir, mode)
}

func openKV(path string, exclusive bool) ethdb.RwKV {
	if database == "lmdb" {
		opts := ethdb.NewLMDB().Path(path)
		if exclusive {
			opts = opts.Exclusive()
		}
		if mapSizeStr != "" {
			var mapSize datasize.ByteSize
			must(mapSize.UnmarshalText([]byte(mapSizeStr)))
			opts = opts.MapSize(mapSize)
		}
		if databaseVerbosity != -1 {
			opts = opts.DBVerbosity(ethdb.DBVerbosityLvl(databaseVerbosity))
		}
		kv := opts.MustOpen()
		metrics.AddCallback(kv.CollectMetrics)
		return kv
	}

	opts := ethdb.NewMDBX().Path(path)
	if exclusive {
		opts = opts.Exclusive()
	}
	if mapSizeStr != "" {
		var mapSize datasize.ByteSize
		must(mapSize.UnmarshalText([]byte(mapSizeStr)))
		opts = opts.MapSize(mapSize)
	}
	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(ethdb.DBVerbosityLvl(databaseVerbosity))
	}
	kv := opts.MustOpen()
	metrics.AddCallback(kv.CollectMetrics)
	return kv
}
