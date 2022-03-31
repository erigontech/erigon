package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/common"

	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
)

var (
	sendAddr    string
	sendValue   uint64
	searchBlock bool
	txType      string
)

func init() {
	sendTxCmd.Flags().StringVar(&txType, "tx-type", "", "type of transaction, specify 'contract' or 'regular'")
	sendTxCmd.MarkFlagRequired("tx-type")

	sendTxCmd.Flags().StringVar(&sendAddr, "addr", "", "String address to send to")
	sendTxCmd.Flags().Uint64Var(&sendValue, "value", 0, "Uint64 Value to send")
	sendTxCmd.Flags().BoolVar(&searchBlock, "search-block", false, "Boolean look for tx in mined blocks")

	rootCmd.AddCommand(sendTxCmd)
}

var sendTxCmd = &cobra.Command{
	Use:   "send-tx",
	Short: "Sends a transaction",
	Args: func(cmd *cobra.Command, args []string) error {
		if txType != "regular" && txType != "contract" {
			return fmt.Errorf("tx type to create must either be 'contract' or 'regular'")
		}
		if txType == "regular" {
			if sendValue == 0 {
				return fmt.Errorf("value must be > 0")
			}
			if sendAddr == "" {
				return fmt.Errorf("string address to send to must be present")
			}
			if !common.IsHexAddress(sendAddr) {
				return fmt.Errorf("address: %v, is not a valid hex address\n", sendAddr)
			}
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer services.ClearDevDB()
		}

		nonce, err := services.GetNonce(reqId)
		if err != nil {
			fmt.Printf("failed to get latest nonce: %v\n", err)
			return
		}

		// subscriptionContract is the handler to the contract for further operations
		signedTx, address, subscriptionContract, transactOpts, err := services.CreateTransaction(txType, sendAddr, sendValue, nonce, searchBlock)
		if err != nil {
			fmt.Printf("failed to create transaction: %v\n", err)
			return
		}

		hash, err := requests.SendTx(reqId, signedTx)
		if err != nil {
			fmt.Printf("failed to send transaction: %v\n", err)
			return
		}

		if searchBlock {
			if _, err := services.SearchBlockForTx(*hash); err != nil {
				fmt.Printf("error searching block for tx: %v\n", err)
				return
			}
		}

		// if the contract is not nil, then the initial transaction created a contract. Emit an event
		if subscriptionContract != nil {
			if err := services.EmitEventAndGetLogs(reqId, subscriptionContract, transactOpts, address); err != nil {
				fmt.Printf("failed to emit events: %v\n", err)
				return
			}
		} else {
			err := services.ApplyTransaction(context.Background(), *signedTx)
			if err != nil {
				fmt.Printf("failed to apply transaction: %v\n", err)
				return
			}
		}
	},
}
