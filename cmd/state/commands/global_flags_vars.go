package commands

import (
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
)

var (
	datadirCli      string
	chaindata       string
	statsfile       string
	block           uint64
	changeSetBucket string
	indexBucket     string
	snapshotsCli    bool
	chain           string
	logdir          string
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 0, "specifies a block number for operation")
}

func withDataDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadirCli, "datadir", paths.DefaultDataDir(), "data directory for temporary ELT files")
	must(cmd.MarkFlagDirname("datadir"))

	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))
}

func withStatsfile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&statsfile, "statsfile", "stateless.csv", "path where to write the stats file")
	must(cmd.MarkFlagFilename("statsfile", "csv"))
}

func withCSBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&changeSetBucket, "changeset-bucket", kv.AccountChangeSet, kv.AccountChangeSet+" for account and "+kv.StorageChangeSet+" for storage")
}

func withIndexBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&indexBucket, "index-bucket", kv.AccountsHistory, kv.AccountsHistory+" for account and "+kv.StorageHistory+" for storage")
}

func withSnapshotBlocks(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&snapshotsCli, "snapshots", true, utils.SnapshotFlag.Usage)
}

func withChain(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chain, "chain", "", "pick a chain to assume (mainnet, sepolia, etc.)")
}
