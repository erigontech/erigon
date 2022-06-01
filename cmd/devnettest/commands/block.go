package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cmd/devnettest/erigon"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
)

const (
	recvAddr         = "0x71562b71999873DB5b286dF957af199Ec94617F7"
	sendValue uint64 = 10000
)

func init() {
	rootCmd.AddCommand(sendTxCmd)
}

var sendTxCmd = &cobra.Command{
	Use:   "send-tx",
	Short: "Sends a transaction",
	Run: func(cmd *cobra.Command, args []string) {
		callSendRegularTxAndSearchBlock(sendValue, recvAddr, devAddress, true)
	},
}

func callSendRegularTxAndSearchBlock(value uint64, fromAddr, toAddr string, search bool) {
	fmt.Printf("Sending %d ETH from %q to %q...\n", value, fromAddr, toAddr)

	nonce, err := services.GetNonce(reqId)
	if err != nil {
		fmt.Printf("failed to get latest nonce: %v\n", err)
		return
	}

	// subscriptionContract is the handler to the contract for further operations
	signedTx, _, _, _, err := services.CreateTransaction("regular", toAddr, value, nonce)
	if err != nil {
		fmt.Printf("failed to create transaction: %v\n", err)
		return
	}

	hash, err := requests.SendTx(reqId, signedTx)
	if err != nil {
		fmt.Printf("failed to send transaction: %v\n", err)
		return
	}

	fmt.Printf("SUCCESS => Tx submitted, adding tx with hash %q to txpool\n", hash)

	if search {
		if _, err := services.SearchBlockForTx(*hash); err != nil {
			fmt.Printf("error searching block for tx: %v\n", err)
			return
		}
	}

	err = services.ApplyTransaction(context.Background(), *signedTx)
	if err != nil {
		fmt.Printf("failed to apply transaction: %v\n", err)
		return
	}
}

func callContractTx() {
	nonce, err := services.GetNonce(reqId)
	if err != nil {
		fmt.Printf("failed to get latest nonce: %v\n", err)
		return
	}

	// subscriptionContract is the handler to the contract for further operations
	signedTx, address, subscriptionContract, transactOpts, err := services.CreateTransaction("contract", "", 0, nonce)
	if err != nil {
		fmt.Printf("failed to create transaction: %v\n", err)
		return
	}

	fmt.Println("Creating contract tx...")
	hash, err := requests.SendTx(reqId, signedTx)
	if err != nil {
		fmt.Printf("failed to send transaction: %v\n", err)
		return
	}
	fmt.Printf("SUCCESS => Tx submitted, adding tx with hash %q to txpool\n", hash)

	time.Sleep(erigon.DevPeriod * time.Second)

	if err := services.EmitEventAndGetLogs(reqId, subscriptionContract, transactOpts, address); err != nil {
		fmt.Printf("failed to emit events: %v\n", err)
		return
	}
}
