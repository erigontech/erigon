package commands

import (
	"github.com/ledgerwatch/erigon/cmd/state/verify"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(verifyHeadersSnapshotCmd)
	rootCmd.AddCommand(verifyHeadersSnapshotCmd)
}

var verifyHeadersSnapshotCmd = &cobra.Command{
	Use:   "verifyHeadersSnapshot",
	Short: "Verify headers snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		if chaindata == "" && len(args) > 0 {
			chaindata = args[0]
		}
		logger := log.New()
		return verify.HeadersSnapshot(logger, chaindata)
	},
}
