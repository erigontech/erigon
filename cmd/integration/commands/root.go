package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/migrations"
	"github.com/ledgerwatch/turbo-geth/turbo/torrent"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for turbo-geth",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := utils.SetupCobra(cmd); err != nil {
			panic(err)
		}

		if len(chaindata) > 0 {
			db := ethdb.MustOpen(chaindata)
			defer db.Close()
			if err := migrations.NewMigrator().Apply(db, ""); err != nil {
				panic(err)
			}
			if len(snapshotMode) > 0 && len(snapshotDir) > 0 {

				mode, err := torrent.SnapshotModeFromString(snapshotMode)
				if err != nil {
					panic(err)
				}
				snapshotKV := db.KV()
				if mode.Bodies {
					snapshotKV = ethdb.NewSnapshotKV().SnapshotDB(ethdb.NewLMDB().Path(snapshotDir+"/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
						return dbutils.BucketsCfg{
							dbutils.BlockBodyPrefix:    dbutils.BucketConfigItem{},
							dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
						}
					}).ReadOnly().MustOpen()).
						For(dbutils.BlockBodyPrefix, dbutils.BucketConfigItem{}).
						For(dbutils.SnapshotInfoBucket, dbutils.BucketConfigItem{}).
						DB(snapshotKV).MustOpen()
				}
				if mode.Headers {
					snapshotKV = ethdb.NewSnapshotKV().SnapshotDB(ethdb.NewLMDB().Path(snapshotDir+"/headers").ReadOnly().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
						return dbutils.BucketsCfg{
							dbutils.HeaderPrefix:       dbutils.BucketConfigItem{},
							dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
						}
					}).MustOpen()).
						For(dbutils.HeaderPrefix, dbutils.BucketConfigItem{}).
						For(dbutils.SnapshotInfoBucket, dbutils.BucketConfigItem{}).
						DB(snapshotKV).MustOpen()
				}
				db.SetKV(snapshotKV)
			}
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
