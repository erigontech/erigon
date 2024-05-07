package commands

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/state/verify"
	"github.com/spf13/cobra"
)

func init() {
	withDataDir(checkIndexCMD)
	withIndexBucket(checkIndexCMD)
	withCSBucket(checkIndexCMD)
	rootCmd.AddCommand(checkIndexCMD)
}

var checkIndexCMD = &cobra.Command{
	Use:   "checkIndex",
	Short: "Index checker",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common.RootContext()
		return verify.CheckIndex(ctx, chaindata, changeSetBucket, indexBucket)
	},
}
