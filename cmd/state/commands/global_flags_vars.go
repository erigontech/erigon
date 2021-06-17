package commands

import (
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/spf13/cobra"
)

var (
	datadir         string
	chaindata       string
	snapshotDir     string
	snapshotMode    string
	statsfile       string
	block           uint64
	changeSetBucket string
	indexBucket     string
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 1, "specifies a block number for operation")
}

func withDatadir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadir, "datadir", paths.DefaultDataDir(), "data directory for temporary ELT files")
	must(cmd.MarkFlagDirname("datadir"))

	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))

	cmd.Flags().StringVar(&snapshotMode, "snapshot.mode", "", "set of snapshots to use")
	cmd.Flags().StringVar(&snapshotDir, "snapshot.dir", "", "snapshot dir")
	must(cmd.MarkFlagDirname("snapshot.dir"))
}

func withStatsfile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&statsfile, "statsfile", "stateless.csv", "path where to write the stats file")
	must(cmd.MarkFlagFilename("statsfile", "csv"))
}

func withCSBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&changeSetBucket, "changeset-bucket", dbutils.AccountChangeSetBucket, dbutils.AccountChangeSetBucket+" for account and "+dbutils.StorageChangeSetBucket+" for storage")
}

func withIndexBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&indexBucket, "index-bucket", dbutils.AccountsHistoryBucket, dbutils.AccountsHistoryBucket+" for account and "+dbutils.StorageHistoryBucket+" for storage")
}
