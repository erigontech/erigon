package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(regenerateTxLookupCmd)
	rootCmd.AddCommand(regenerateTxLookupCmd)
}

var regenerateTxLookupCmd = &cobra.Command{
	Use:   "regenerateTxLookup",
	Short: "Generate tx lookup index",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.RegenerateTxLookup(chaindata)
	},
}
