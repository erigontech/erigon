package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(verifySnapshotCmd)
	withChaindata(verifySnapshotCmd)
	rootCmd.AddCommand(verifySnapshotCmd)
}

var verifySnapshotCmd = &cobra.Command{
	Use:   "verifySnapshot",
	Short: "Verifies snapshots made by the 'stateless' action",
	RunE: func(cmd *cobra.Command, args []string) error {
		stateless.VerifySnapshot(block, chaindata)
		return nil
	},
}
