package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/ledgerwatch/erigon/common"
	"github.com/spf13/cobra"
)

const (
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
	fmt.Printf("Getting balance for address: %q...\n", addr)
	address := common.HexToAddress(addr)
	bal, err := requests.GetBalance(reqId, address, blockNum)
	if err != nil {
		fmt.Printf("FAILURE => %v\n", err)
	}
	fmt.Printf("SUCCESS => Balance: %s\n", bal)
}

func callGetTransactionCount(addr, blockNum string) {
	fmt.Printf("Getting nonce for address: %q...\n", addr)
	address := common.HexToAddress(addr)
	nonce, err := requests.GetTransactionCountCmd(reqId, address, blockNum)
	if err != nil {
		fmt.Printf("could not get transaction count: %v\n", err)
	}
	fmt.Printf("SUCCESS => Nonce: %s\n", nonce)
}
