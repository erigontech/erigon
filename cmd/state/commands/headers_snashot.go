package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(generateHeadersSnapshotCmd)
	withSnapshotFile(generateHeadersSnapshotCmd)
	rootCmd.AddCommand(generateHeadersSnapshotCmd)
}

var generateHeadersSnapshotCmd = &cobra.Command{
	Use:   "headerSnapshot",
	Short: "Generate  snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.GenerateBittorrentHeaderSnapshot(chaindata, snapshotFile, block)
	},
}

