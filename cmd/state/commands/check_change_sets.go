package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

var (
	historyfile       string
)

func init() {
	withBlock(checkChangeSetsCmd)
	withChaindata(checkChangeSetsCmd)
	checkChangeSetsCmd.Flags().StringVar(&historyfile, "historyfile", chaindata, "path to the file where the changesets and history are expected to be. If omitted, the same as --chaindata")
	rootCmd.AddCommand(checkChangeSetsCmd)
}

var checkChangeSetsCmd = &cobra.Command{
	Use:   "checkChangeSets",
	Short: "Re-executes historical transactions in read-only mode and checks that their outputs match the database ChangeSets",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stateless.CheckChangeSets(block, chaindata, historyfile)
	},
}
