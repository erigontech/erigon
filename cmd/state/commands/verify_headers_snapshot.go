package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/verify"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(verifyHeadersSnapshotCmd)
	rootCmd.AddCommand(verifyHeadersSnapshotCmd)
}

var verifyHeadersSnapshotCmd = &cobra.Command{
	Use:   "verifyHeadersSnapshot",
	Short: "Verify headers snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		if chaindata == "" && len(args) > 0 {
			chaindata = args[0]
		}
		return verify.HeadersSnapshot(chaindata)
	},
}
