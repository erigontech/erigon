package commands

import (
	"github.com/ledgerwatch/erigon/cmd/state/verify"
	"github.com/spf13/cobra"
)

func init() {
	withDataDir(checkEncCmd)
	withStatsfile(checkEncCmd)
	rootCmd.AddCommand(checkEncCmd)
}

var checkEncCmd = &cobra.Command{
	Use:   "checkEnc",
	Short: "Check changesets Encoding",
	RunE: func(cmd *cobra.Command, args []string) error {
		return verify.CheckEnc(chaindata)
	},
}
