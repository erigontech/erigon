package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

var (
	historyfile   string
	nocheck       bool
	writeReceipts bool
)

func init() {
	withBlock(checkChangeSetsCmd)
	withChaindata(checkChangeSetsCmd)
	checkChangeSetsCmd.Flags().StringVar(&historyfile, "historyfile", "", "path to the file where the changesets and history are expected to be. If omitted, the same as --chaindata")
	checkChangeSetsCmd.Flags().BoolVar(&nocheck, "nocheck", false, "set to turn off the changeset checking and only execute transaction (for performance testing)")
	checkChangeSetsCmd.Flags().BoolVar(&writeReceipts, "writeReceipts", false, "set to turn off writing receipts as the exection ongoing")
	rootCmd.AddCommand(checkChangeSetsCmd)
}

var checkChangeSetsCmd = &cobra.Command{
	Use:   "checkChangeSets",
	Short: "Re-executes historical transactions in read-only mode and checks that their outputs match the database ChangeSets",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stateless.CheckChangeSets(genesis, block, chaindata, historyfile, nocheck, writeReceipts)
	},
}
