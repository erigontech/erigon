package commands

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/spf13/cobra"
)

var (
	chaindata       string
	statsfile       string
	snapshotFile    string
	block           uint64
	privateApiAddr  string
	changeSetBucket string
	indexBucket     string
	snapshotMode    string
	snapshotDir     string
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 1, "specifies a block number for operation")
}
func withSnapshotData(cmd *cobra.Command) {
	cmd.Flags().StringVar(&snapshotMode, "snapshotMode", "", "set of snapshots to use")
	cmd.Flags().StringVar(&snapshotDir, "snapshotDir", "", "snapshot dir")
}

func withChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chaindata, "chaindata", "chaindata", "path to the chaindata file used as input to analysis")
	must(cmd.MarkFlagFilename("chaindata", ""))
}

func withStatsfile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&statsfile, "statsfile", "stateless.csv", "path where to write the stats file")
	must(cmd.MarkFlagFilename("statsfile", "csv"))
}

func withPrivateApi(cmd *cobra.Command) {
	cmd.Flags().StringVar(&privateApiAddr, "private.api.addr", "", "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
}

func withCSBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&changeSetBucket, "changeset-bucket", string(dbutils.AccountChangeSetBucket), string(dbutils.AccountChangeSetBucket)+" for account and "+string(dbutils.StorageChangeSetBucket)+" for storage")
}

func withIndexBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&indexBucket, "index-bucket", string(dbutils.AccountsHistoryBucket), string(dbutils.AccountsHistoryBucket)+" for account and "+string(dbutils.StorageHistoryBucket)+" for storage")
}

func withSnapshotFile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&snapshotFile, "snapshot", "", "path where to write the snapshot file")
}
