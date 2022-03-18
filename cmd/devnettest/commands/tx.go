package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
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
			defer services.ClearDevDB()
		}
		if err := requests.TxpoolContent(reqId); err != nil {
			fmt.Printf("error getting txpool content: %v", err)
		}
	},
}
