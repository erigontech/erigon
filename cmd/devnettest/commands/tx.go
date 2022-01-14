package commands

import (
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(txPoolCmd)
}

var txPoolCmd = &cobra.Command{
	Use:   "txpool-content",
	Short: "Gets content of txpool",
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer clearDevDB()
		}
		requests.TxpoolContent(reqId)
	},
}
