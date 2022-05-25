package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/erigon"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
	"time"
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

		callGetBalance(sendAddr, blockNum)
		fmt.Println()
		callSendRegularTxAndSearchBlock(sendValue, devAddress, sendAddr, true)
		fmt.Println()
		callGetBalance(sendAddr, blockNum)
		fmt.Println()
		callGetTransactionCount(devAddress, blockNum)
		fmt.Println()
		go callLogs()
		time.Sleep(erigon.DevPeriod * 2 * time.Second)
		callGetTransactionCount(devAddress, blockNum)
		fmt.Println()
		fmt.Println("Mocking get requests to JSON RPC...")
		callMockGetRequest()
		fmt.Println()
		fmt.Println("Confirming the tx pool is empty...")
		showTxPoolContent()
	},
}
