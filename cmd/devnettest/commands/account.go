package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
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
		callGetBalance(devAddress, blockNum, 0)
	},
}

var getTransactionCountCmd = &cobra.Command{
	Use:   "get-transaction-count",
	Short: fmt.Sprintf("Gets nonce for the address: %q", devAddress),
	Run: func(cmd *cobra.Command, args []string) {
		callGetTransactionCount(devAddress, blockNum, 0)
	},
}

func callGetBalance(addr, blockNum string, checkBal uint64) {
	fmt.Printf("Getting balance for address: %q...\n", addr)
	address := common.HexToAddress(addr)
	bal, err := requests.GetBalance(reqId, address, blockNum)
	if err != nil {
		fmt.Printf("FAILURE => %v\n", err)
		return
	}

	if checkBal > 0 && checkBal != bal {
		fmt.Printf("FAILURE => Balance should be %d, got %d\n", checkBal, bal)
		return
	}

	fmt.Printf("SUCCESS => Balance: %d\n", bal)
}

func callGetTransactionCount(addr, blockNum string, checkNonce uint64) {
	fmt.Printf("Getting nonce for address: %q...\n", addr)
	address := common.HexToAddress(addr)
	nonce, err := requests.GetTransactionCountCmd(reqId, address, blockNum)
	if err != nil {
		fmt.Printf("FAILURE => %v\n", err)
		return
	}

	if checkNonce > 0 && checkNonce != nonce {
		fmt.Printf("FAILURE => Nonce should be %d, got %d\n", checkNonce, nonce)
		return
	}

	fmt.Printf("SUCCESS => Nonce: %d\n", nonce)
}
