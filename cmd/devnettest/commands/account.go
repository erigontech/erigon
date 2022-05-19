package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/ledgerwatch/erigon/common"
	"github.com/spf13/cobra"
)

var (
	devAddress = "0x67b1d87101671b127f5f8714789C7192f7ad340e"
	blockNum   = "latest"
)

func init() {
	rootCmd.AddCommand(getBalanceCmd)
	rootCmd.AddCommand(getTransactionCountCmd)
}

var getBalanceCmd = &cobra.Command{
	Use:   "get-balance",
	Short: fmt.Sprintf("Checks balance for the address: %q", devAddress),
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer services.ClearDevDB()
		}
		blockNum = "latest"
		callGetBalance(devAddress, blockNum)
	},
}

var getTransactionCountCmd = &cobra.Command{
	Use:   "get-transaction-count",
	Short: fmt.Sprintf("Gets nonce for the address: %q", devAddress),
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer services.ClearDevDB()
		}
		callGetTransactionCount(devAddress, blockNum)
	},
}

func callGetBalance(addr, blockNum string) {
	address := common.HexToAddress(addr)
	bal, err := requests.GetBalance(reqId, address, blockNum)
	if err != nil {
		fmt.Printf("could not get balance: %v\n", err)
	}
	fmt.Printf("Balance for account with address %q is: %s\n", addr, bal)
}

func callGetTransactionCount(addr, blockNum string) {
	address := common.HexToAddress(addr)
	nonce, err := requests.GetTransactionCountCmd(reqId, address, blockNum)
	if err != nil {
		fmt.Printf("could not get transaction count: %v\n", err)
	}
	fmt.Printf("Nonce for account with address %q is: %s\n", addr, nonce)
}