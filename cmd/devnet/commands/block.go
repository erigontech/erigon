package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
)

const (
	recipientAddress        = "0x71562b71999873DB5b286dF957af199Ec94617F7"
	sendValue        uint64 = 10000
)

func callSendTx(value uint64, fromAddr, toAddr string) {
	fmt.Printf("Sending %d ETH from %q to %q...\n", value, fromAddr, toAddr)

	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(models.ReqId)
	if err != nil {
		fmt.Printf("failed to get latest nonce: %s\n", err)
		return
	}

	// create a non-contract transaction and sign it
	signedTx, _, err := services.CreateTransaction(models.NonContractTx, toAddr, value, nonce)
	if err != nil {
		fmt.Printf("failed to create a transaction: %s\n", err)
	}

	// send the signed transaction
	hash, err := requests.SendTransaction(models.ReqId, signedTx)
	if err != nil {
		fmt.Printf("failed to send transaction: %s\n", err)
		return
	}

	fmt.Printf("SUCCESS => Tx submitted, adding tx with hash %q to txpool\n", hash)
}
