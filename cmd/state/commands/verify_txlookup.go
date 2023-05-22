package commands

import (
	"github.com/ledgerwatch/erigon/cmd/state/verify"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/log/v3"
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
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "verify_txlookup"); err != nil {
			logger.Error("Setting up", "error", err)
			return err
		}
		return verify.ValidateTxLookups(chaindata, logger)
	},
}
