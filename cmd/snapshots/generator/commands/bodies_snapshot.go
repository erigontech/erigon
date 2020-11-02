package commands

import (
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(generateBodiesSnapshotCmd)
	withSnapshotFile(generateBodiesSnapshotCmd)
	withSnapshotData(generateBodiesSnapshotCmd)
	withBlock(generateBodiesSnapshotCmd)
	rootCmd.AddCommand(generateHeadersSnapshotCmd)

}

var generateBodiesSnapshotCmd = &cobra.Command{
	Use:   "bodies",
	Short: "Generate bodies snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return BodySnapshot(chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 1, "specifies a block number for operation")
}
func withSnapshotData(cmd *cobra.Command) {
	cmd.Flags().StringVar(&snapshotMode, "snapshotMode", "", "set of snapshots to use")
	cmd.Flags().StringVar(&snapshotDir, "snapshotDir", "", "snapshot dir")
}

func withChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chaindata, "chaindata", "chaindata", "path to the chaindata file used as input to analysis")
	must(cmd.MarkFlagFilename("chaindata", ""))
}

func withSnapshotFile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&snapshotFile, "snapshot", "", "path where to write the snapshot file")
}
