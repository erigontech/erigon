package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/verify"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(regenerateTxLookupCmd)
	rootCmd.AddCommand(regenerateTxLookupCmd)
}

var verifyTxLookupCmd = &cobra.Command{
	Use:   "regenerateTxLookup",
	Short: "Generate tx lookup index",
	RunE: func(cmd *cobra.Command, args []string) error {
		return verify.ValidateTxLookups(chaindata)
	},
}
