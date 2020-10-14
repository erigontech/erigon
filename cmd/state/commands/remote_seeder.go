package commands

import (
	"errors"
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(remoteSeedSnapshotCmd)
}

var remoteSeedSnapshotCmd = &cobra.Command{
	Use:   "remoteSeedSnapshots",
	Short: "Seed snapshots",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("empty path")
		}
		return generate.SeedSnapshots(args[0])
	},
}
