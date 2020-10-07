package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(generateHeadersSnapshotCmd)
	withSnapshotFile(generateHeadersSnapshotCmd)
	withBlock(generateHeadersSnapshotCmd)
	rootCmd.AddCommand(generateHeadersSnapshotCmd)
}

var generateHeadersSnapshotCmd = &cobra.Command{
	Use:   "headersSnapshot",
	Short: "Generate  snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.HeaderSnapshot(chaindata, snapshotFile, block)
	},
}
