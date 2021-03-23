package commands

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(snapshotMetainfoCmd)
}

var snapshotMetainfoCmd = &cobra.Command{
	Use:   "snapshotMetainfo",
	Short: "Calculate snapshot metainfo",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("empty path")
		}
		return generate.MetaInfoHash(args[0])
	},
}
