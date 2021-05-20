package commands

import (
	"github.com/ledgerwatch/erigon/cmd/state/verify"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(checkIndexCMD)
	withIndexBucket(checkIndexCMD)
	withCSBucket(checkIndexCMD)
	rootCmd.AddCommand(checkIndexCMD)
}

var checkIndexCMD = &cobra.Command{
	Use:   "checkIndex",
	Short: "Index checker",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		return verify.CheckIndex(ctx, chaindata, changeSetBucket, indexBucket)
	},
}
