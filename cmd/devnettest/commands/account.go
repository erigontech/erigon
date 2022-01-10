package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/common"
	"github.com/spf13/cobra"
)

var (
	addr     string
	blockNum string
)

func init() {
	getBalanceCmd.Flags().StringVar(&addr, "addr", "", "String address to check")
	getBalanceCmd.MarkFlagRequired("addr")
	getBalanceCmd.Flags().StringVar(&blockNum, "block-num", "latest", "String denoting block number")

	rootCmd.AddCommand(getBalanceCmd)
}

var getBalanceCmd = &cobra.Command{
	Use:   "get-balance",
	Short: "Checks balance by address",
	Args: func(cmd *cobra.Command, args []string) error {
		var err error
		switch blockNum {
		case "pending", "latest", "earliest":
		default:
			err = fmt.Errorf("block number must be 'pending', 'latest' or 'earliest'")
		}
		if err != nil {
			return err
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer clearDevDB()
		}
		toAddress := common.HexToAddress(addr)
		requests.GetBalance(reqId, toAddress, blockNum)
	},
}
