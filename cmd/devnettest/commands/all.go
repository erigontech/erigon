package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(allCmd)
}

var allCmd = &cobra.Command{
	Use:   "all",
	Short: "Runs all the simulation tests for erigon devnet",
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer services.ClearDevDB()
		}
		blockNum = "latest"

		callGetBalance(sendAddr, blockNum)
		fmt.Println()
		callSendRegularTxAndSearchBlock()
		fmt.Println()
		callGetBalance(sendAddr, blockNum)
		callGetTransactionCount(devAddress, blockNum)
	},
}