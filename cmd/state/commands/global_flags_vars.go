package commands

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/spf13/cobra"
)

var (
	chaindata       string
	statsfile       string
	block           uint64
	privateRpcAddr  string
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

func withPrivateRpc(cmd *cobra.Command) {
	cmd.Flags().StringVar(&privateRpcAddr, "private.rpc.addr", "", "remote db rpc address")
}

func withCSBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&changeSetBucket, "changeset-bucket", string(dbutils.AccountChangeSetBucket), string(dbutils.AccountChangeSetBucket)+" for account and "+string(dbutils.StorageChangeSetBucket)+" for storage")
}

func withIndexBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&indexBucket, "index-bucket", string(dbutils.AccountsHistoryBucket), string(dbutils.AccountsHistoryBucket)+" for account and "+string(dbutils.StorageHistoryBucket)+" for storage")
}
