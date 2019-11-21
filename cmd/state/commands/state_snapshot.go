package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(stateSnapshotCmd)
	rootCmd.AddCommand(stateSnapshotCmd)
}

var stateSnapshotCmd = &cobra.Command{
	Use:   "stateSnapshot",
	Short: "stateSnapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stateless.StateSnapshot(block)
	},
}
