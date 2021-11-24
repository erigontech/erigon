package commands

import (
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(erigon2Cmd)
	withDatadir(erigon2Cmd)
	rootCmd.AddCommand(erigon2Cmd)
}

var erigon2Cmd = &cobra.Command{
	Use:   "erigon2",
	Short: "Exerimental command to re-execute blocks from beginning using erigon2 state representation",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Erigon2(logger, block, datadir)
	},
}

func Erigon2(logger log.Logger, blockNum uint64, datadir string) error {
	return nil
}
