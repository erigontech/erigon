package commands

import (
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/migrations"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for turbo-geth",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := utils.SetupCobra(cmd); err != nil {
			panic(err)
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

func openDatabase(path string, applyMigrations bool) *ethdb.ObjectDatabase {
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
	if err := SetSnapshotKV(db, snapshotDir, snapshotMode); err != nil {
		panic(err)
	}
	return db
}

func openKV(path string, exclusive bool) ethdb.KV {
	if database == "mdbx" {
		opts := ethdb.NewMDBX().Path(path)
		if exclusive {
			opts = opts.Exclusive()
		}
		if mapSizeStr != "" {
			var mapSize datasize.ByteSize
			must(mapSize.UnmarshalText([]byte(mapSizeStr)))
			opts = opts.MapSize(mapSize)
		}
		if freelistReuse > 0 {
			opts = opts.MaxFreelistReuse(uint(freelistReuse))
		}
		return opts.MustOpen()
	}

	opts := ethdb.NewLMDB().Path(path)
	if exclusive {
		opts = opts.Exclusive()
	}
	if mapSizeStr != "" {
		var mapSize datasize.ByteSize
		must(mapSize.UnmarshalText([]byte(mapSizeStr)))
		opts = opts.MapSize(mapSize)
	}
	if freelistReuse > 0 {
		opts = opts.MaxFreelistReuse(uint(freelistReuse))
	}
	kv := opts.MustOpen()
	metrics.AddCallback(kv.CollectMetrics)
	return kv
}
