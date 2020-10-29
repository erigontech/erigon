package commands

import (
	"github.com/spf13/cobra"
)

func init() {
	//commands.withChaindata(generateBodiesSnapshotCmd)
	//commands.withSnapshotFile(generateBodiesSnapshotCmd)
	//commands.withSnapshotData(generateBodiesSnapshotCmd)
	//commands.withBlock(generateBodiesSnapshotCmd)
	//commands.rootCmd.AddCommand(generateBodiesSnapshotCmd)
}

var generateBodiesSnapshotCmd = &cobra.Command{
	Use:   "bodies",
	Short: "Generate bodies snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return BodySnapshot(chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}
