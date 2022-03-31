package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"

	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/common"
	"github.com/spf13/cobra"
)

var (
	accountAddr string
	blockNum    string
)

func init() {
	getBalanceCmd.Flags().StringVar(&accountAddr, "addr", "", "String address to check")
	getBalanceCmd.MarkFlagRequired("addr")
	getBalanceCmd.Flags().StringVar(&blockNum, "block-num", "latest", "String denoting block number")
	rootCmd.AddCommand(getBalanceCmd)

	getTransactionCountCmd.Flags().StringVar(&accountAddr, "addr", "", "String address to check")
	getTransactionCountCmd.MarkFlagRequired("addr")
	getTransactionCountCmd.Flags().StringVar(&blockNum, "block-num", "latest", "String denoting block number")
	rootCmd.AddCommand(getTransactionCountCmd)
}

var getBalanceCmd = &cobra.Command{
	Use:   "get-balance",
	Short: "Checks balance by address",
	Args: func(cmd *cobra.Command, args []string) error {
		switch blockNum {
		case "pending", "latest", "earliest":
		default:
			return fmt.Errorf("block number must be 'pending', 'latest' or 'earliest'")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer services.ClearDevDB()
		}
		if !common.IsHexAddress(accountAddr) {
			fmt.Printf("address: %v, is not a valid hex address\n", accountAddr)
			return
		}
		address := common.HexToAddress(accountAddr)
		if err := requests.GetBalance(reqId, address, blockNum); err != nil {
			fmt.Printf("could not get balance: %v\n", err)
		}
	},
}

var getTransactionCountCmd = &cobra.Command{
	Use:   "get-transaction-count",
	Short: "Gets the total number of transactions sent out by an account, the nonce",
	Args: func(cmd *cobra.Command, args []string) error {
		switch blockNum {
		case "pending", "latest", "earliest":
		default:
			return fmt.Errorf("block number must be 'pending', 'latest' or 'earliest'")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer services.ClearDevDB()
		}
		if !common.IsHexAddress(accountAddr) {
			fmt.Printf("address: %v, is not a valid hex address\n", accountAddr)
			return
		}
		address := common.HexToAddress(accountAddr)
		if err := requests.GetTransactionCountCmd(reqId, address, blockNum); err != nil {
			fmt.Printf("could not get transaction count: %v\n", err)
		}
	},
}
