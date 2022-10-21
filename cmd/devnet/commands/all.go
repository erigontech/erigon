package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
)

// ExecuteAllMethods runs all the simulation tests for erigon devnet
func ExecuteAllMethods() {
	// test connection to JSON RPC
	fmt.Println("Mocking get requests to JSON RPC...")
	pingErigonRpc()
	fmt.Println()

	// get balance of the receiver's account
	callGetBalance(addr, models.Latest, 0)
	fmt.Println()

	// confirm that the txpool is empty
	fmt.Println("Confirming the tx pool is empty...")
	callTxPoolContent()

	// send a token from the dev address to the recipient address
	callSendTx(sendValue, models.DevAddress, recipientAddress)
	fmt.Println()

	// confirm that the txpool has this transaction in the queue
	fmt.Println("Confirming the tx pool has the latest transaction...")
	callTxPoolContent()
}
