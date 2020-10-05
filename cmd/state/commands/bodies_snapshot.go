package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(generateBodiesSnapshotCmd)
	withSnapshotFile(generateBodiesSnapshotCmd)
	withBlock(generateBodiesSnapshotCmd)
	rootCmd.AddCommand(generateBodiesSnapshotCmd)
}

var generateBodiesSnapshotCmd = &cobra.Command{
	Use:   "bodiesSnapshot",
	Short: "Generate bodies snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.BodySnapshot(chaindata, snapshotFile, block)
	},
}
