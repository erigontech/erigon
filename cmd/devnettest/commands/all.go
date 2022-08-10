package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(allCmd)
}

var allCmd = &cobra.Command{
	Use:   "all",
	Short: "Runs all the simulation tests for erigon devnet",
	Run: func(cmd *cobra.Command, args []string) {
		// Test connection to JSON RPC
		fmt.Println("Mocking get requests to JSON RPC...")
		callMockGetRequest()
		fmt.Println()

		// First get balance of the receiver's account
		callGetBalance(recvAddr, blockNum, 0)
		fmt.Println()

		// Send a token from the dev address to the receiver's address
		callSendRegularTxAndSearchBlock(sendValue, devAddress, recvAddr, true)
		fmt.Println()

		// Check the balance to make sure the receiver received such token
		callGetBalance(recvAddr, blockNum, sendValue)
		fmt.Println()

		// Get the nonce of the devAddress, it should be 1
		callGetTransactionCount(devAddress, blockNum, 1)
		fmt.Println()

		// Create a contract transaction signed by the dev address and emit a log for it
		// callContractTx()
		// time.Sleep(erigon.DevPeriod * 2 * time.Second)
		// fmt.Println()

		// Get the nonce of the devAddress, check that it is 3
		// callGetTransactionCount(devAddress, blockNum, 3)
		// fmt.Println()

		// Confirm that the txpool is empty (meaning all txs have been mined)
		fmt.Println("Confirming the tx pool is empty...")
		showTxPoolContent()
	},
}
