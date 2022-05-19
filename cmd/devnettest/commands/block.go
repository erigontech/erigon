package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
)

var (
	sendAddr   = "0x71562b71999873DB5b286dF957af199Ec94617F7"
	sendValue uint64 = 10000
	searchBlock bool
)

func init() {
	rootCmd.AddCommand(sendTxCmd)
}

var sendTxCmd = &cobra.Command{
	Use:   "send-tx",
	Short: "Sends a transaction",
	Run: func(cmd *cobra.Command, args []string) {
		if clearDev {
			defer services.ClearDevDB()
		}

		callSendRegularTxAndSearchBlock()

		//nonce, err := services.GetNonce(reqId)
		//if err != nil {
		//	fmt.Printf("failed to get latest nonce: %v\n", err)
		//	return
		//}
		//
		//// subscriptionContract is the handler to the contract for further operations
		//signedTx, address, subscriptionContract, transactOpts, err := services.CreateTransaction(txType, sendAddr, sendValue, nonce, searchBlock)
		//if err != nil {
		//	fmt.Printf("failed to create transaction: %v\n", err)
		//	return
		//}
		//
		//res, hash, err := requests.SendTx(reqId, signedTx)
		//if err != nil {
		//	fmt.Printf("failed to send transaction: %v\n", err)
		//	return
		//}
		//
		//fmt.Printf(res)
		//
		//if searchBlock {
		//	if _, err := services.SearchBlockForTx(*hash); err != nil {
		//		fmt.Printf("error searching block for tx: %v\n", err)
		//		return
		//	}
		//}
		//
		//// if the contract is not nil, then the initial transaction created a contract. Emit an event
		//if subscriptionContract != nil {
		//	if err := services.EmitEventAndGetLogs(reqId, subscriptionContract, transactOpts, address); err != nil {
		//		fmt.Printf("failed to emit events: %v\n", err)
		//		return
		//	}
		//} else {
		//	err := services.ApplyTransaction(context.Background(), *signedTx)
		//	if err != nil {
		//		fmt.Printf("failed to apply transaction: %v\n", err)
		//		return
		//	}
		//}
	},
}

func callSendRegularTxAndSearchBlock() {
	fmt.Printf("Sending %d ETH from %q to %q\n", sendValue, devAddress, sendAddr)

	nonce, err := services.GetNonce(reqId)
	if err != nil {
		fmt.Printf("failed to get latest nonce: %v\n", err)
		return
	}

	// subscriptionContract is the handler to the contract for further operations
	signedTx, address, subscriptionContract, transactOpts, err := services.CreateTransaction("regular", sendAddr, sendValue, nonce)
	if err != nil {
		fmt.Printf("failed to create transaction: %v\n", err)
		return
	}

	hash, err := requests.SendTx(reqId, signedTx)
	if err != nil {
		fmt.Printf("failed to send transaction: %v\n", err)
		return
	}

	fmt.Printf("Transaction submitted with hash: %v\n\n", hash)

	showTxPoolContent()

	if _, err := services.SearchBlockForTx(*hash); err != nil {
		fmt.Printf("error searching block for tx: %v\n", err)
		return
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
}