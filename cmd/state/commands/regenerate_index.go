package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(regenerateIndexCmd)
	withIndexBucket(regenerateIndexCmd)
	rootCmd.AddCommand(regenerateIndexCmd)
}

var regenerateIndexCmd = &cobra.Command{
	Use:   "checkChangeSets",
	Short: "Re-executes historical transactions in read-only mode and checks that their outputs match the database ChangeSets",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stateless.RegenerateIndex(chaindata, []byte(indexBucket), []byte(changeSetBucket))
	},
}
