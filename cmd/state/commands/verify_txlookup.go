package commands

import (
	"github.com/ledgerwatch/erigon/cmd/state/verify"
	"github.com/spf13/cobra"
)

func init() {
	withDataDir(verifyTxLookupCmd)
	rootCmd.AddCommand(verifyTxLookupCmd)
}

var verifyTxLookupCmd = &cobra.Command{
	Use:   "verifyTxLookup",
	Short: "Generate tx lookup index",
	RunE: func(cmd *cobra.Command, args []string) error {
		return verify.ValidateTxLookups(chaindata)
	},
}
