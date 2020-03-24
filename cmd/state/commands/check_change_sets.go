package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(checkChangeSetsCmd)
	withChaindata(checkChangeSetsCmd)
	checkChangeSetsCmd.Flags().StringVar(&statefile, "statefile", "state", "path to the file where the state will be periodically written during the analysis")
	rootCmd.AddCommand(checkChangeSetsCmd)
}

var checkChangeSetsCmd = &cobra.Command{
	Use:   "checkChangeSets",
	Short: "Re-executes historical transactions in read-only mode and checks that their outputs match the database ChangeSets",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stateless.CheckChangeSets(block, chaindata, statefile)
	},
}
