package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

var (
	snapshotPath string
)

func init() {
	verifySnapshotCmd.Flags().StringVar(&snapshotPath, "snapshot", "snapshot", "Path to the snapshot to verify")
	if err := verifySnapshotCmd.MarkFlagFilename("snapshot", ""); err != nil {
		panic(err)
	}
	if err := verifySnapshotCmd.MarkFlagRequired("snapshot"); err != nil {
		panic(err)
	}
	rootCmd.AddCommand(verifySnapshotCmd)
}

var verifySnapshotCmd = &cobra.Command{
	Use:   "verifySnapshot",
	Short: "Verifies snapshots made by the 'stateless' action",
	RunE: func(cmd *cobra.Command, args []string) error {
		stateless.VerifySnapshot(snapshotPath)
		return nil
	},
}
