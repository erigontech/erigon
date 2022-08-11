package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/state/verify"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(checkIndexCMD)
	withIndexBucket(checkIndexCMD)
	withCSBucket(checkIndexCMD)
	withBlock(checkIndexCMD)
	rootCmd.AddCommand(checkIndexCMD)
}

var checkIndexCMD = &cobra.Command{
	Use:   "checkIndex",
	Short: "Index checker",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		err := verify.CheckIndex(ctx, chaindata, changeSetBucket, indexBucket, block)
		if err != nil {
			fmt.Println("Error in CheckIndex:", err)
		}
		return err
	},
}
