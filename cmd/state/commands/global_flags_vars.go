package commands

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/spf13/cobra"
)

var (
	chaindata       string
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

func withChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chaindata, "chaindata", "chaindata", "path to the chaindata file used as input to analysis")
	must(cmd.MarkFlagFilename("chaindata", ""))
}

func withStatsfile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&statsfile, "statsfile", "stateless.csv", "path where to write the stats file")
	must(cmd.MarkFlagFilename("statsfile", "csv"))
}

func withCSBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&changeSetBucket, "changeset-bucket", dbutils.PlainAccountChangeSetBucket, dbutils.PlainAccountChangeSetBucket+" for account and "+dbutils.PlainStorageChangeSetBucket+" for storage")
}

func withIndexBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&indexBucket, "index-bucket", dbutils.AccountsHistoryBucket, dbutils.AccountsHistoryBucket+" for account and "+dbutils.StorageHistoryBucket+" for storage")
}
