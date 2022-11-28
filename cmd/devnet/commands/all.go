package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
)

// ExecuteAllMethods runs all the simulation tests for erigon devnet
func ExecuteAllMethods() {
	// test connection to JSON RPC
	fmt.Printf("\nPINGING JSON RPC...\n")
	if err := pingErigonRpc(); err != nil {
		return
	}
	fmt.Println()

	// get balance of the receiver's account
	callGetBalance(addr, models.Latest, 0)
	fmt.Println()

	// confirm that the txpool is empty
	fmt.Println("CONFIRMING TXPOOL IS EMPTY BEFORE SENDING TRANSACTION...")
	checkTxPoolContent(0, 0)
	fmt.Println()

	/*
	* Cannot run contract tx after running regular tx because contract tx simulates a new backend
	* and it expects the nonce to be 0.
	* So it is best to run them separately by commenting and uncommenting the different code blocks.
	 */

	// send a token from the dev address to the recipient address
	//nonContractHash, err := callSendTx(sendValue, recipientAddress, models.DevAddress)
	//if err != nil {
	//	fmt.Printf("callSendTx error: %v\n", err)
	//	return
	//}
	//fmt.Println()

	//// confirm that the txpool has this transaction in the pending queue
	//fmt.Println("CONFIRMING TXPOOL HAS THE LATEST TRANSACTION...")
	//checkTxPoolContent(1, 0)
	//fmt.Println()
	//
	//// look for the transaction hash in the newly mined block
	//fmt.Println("LOOKING FOR TRANSACTION IN THE LATEST BLOCK...")
	//callSubscribeToNewHeads(*nonContractHash)
	//fmt.Println()

	// initiate a contract transaction
	fmt.Println("INITIATING A CONTRACT TRANSACTION...")
	_, err := callContractTx()
	if err != nil {
		fmt.Printf("callContractTx error: %v\n", err)
		return
	}
	fmt.Println()

	// confirm that the transaction has been moved from the pending queue and the txpool is empty once again
	fmt.Println("CONFIRMING TXPOOL IS EMPTY ONCE AGAIN...")
	checkTxPoolContent(0, 0)
}
