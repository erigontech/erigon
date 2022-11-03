package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
)

const (
	recipientAddress        = "0x71562b71999873DB5b286dF957af199Ec94617F7"
	sendValue        uint64 = 10000
)

func callSendTx(value uint64, toAddr, fromAddr string) (*common.Hash, error) {
	fmt.Printf("Sending %d ETH to %q from %q...\n", value, toAddr, fromAddr)

	// get the latest nonce for the next transaction
	nonce, err := services.GetNonce(models.ReqId, common.HexToAddress(fromAddr))
	if err != nil {
		fmt.Printf("failed to get latest nonce: %s\n", err)
		return nil, err
	}

	// create a non-contract transaction and sign it
	signedTx, _, err := services.CreateTransaction(models.NonContractTx, toAddr, value, nonce)
	if err != nil {
		fmt.Printf("failed to create a transaction: %s\n", err)
		return nil, err
	}

	// send the signed transaction
	hash, err := requests.SendTransaction(models.ReqId, signedTx)
	if err != nil {
		fmt.Printf("failed to send transaction: %s\n", err)
		return nil, err
	}

	fmt.Printf("SUCCESS => Tx submitted, adding tx with hash %q to txpool\n", hash)

	return hash, nil
}
