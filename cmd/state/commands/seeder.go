package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(seedSnapshotCmd)
}

var seedSnapshotCmd = &cobra.Command{
	Use:   "seedSnapshots",
	Short: "Seed snapshots",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.Seed(args)
	},
}
