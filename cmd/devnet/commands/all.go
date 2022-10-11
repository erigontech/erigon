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
	callGetBalance(addr, models.BlockNumLatest, 0)
	fmt.Println()

	fmt.Println("Confirming the tx pool is empty...")
	callTxPoolContent()
}
