package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/log/v3"
)

// ExecuteAllMethods runs all the simulation tests for erigon devnet
func ExecuteAllMethods(logger log.Logger) {
	// test connection to JSON RPC
	logger.Info("PINGING JSON RPC...")
	if err := pingErigonRpc(logger); err != nil {
		return
	}
	logger.Info("")

	// get balance of the receiver's account
	callGetBalance(addr, models.Latest, 0, logger)
	logger.Info("")

	// confirm that the txpool is empty
	logger.Info("CONFIRMING TXPOOL IS EMPTY BEFORE SENDING TRANSACTION...")
	services.CheckTxPoolContent(0, 0, 0, logger)
	logger.Info("")

	/*
	* Cannot run contract tx after running regular tx because contract tx simulates a new backend
	* and it expects the nonce to be 0.
	* So it is best to run them separately by commenting and uncommenting the different code blocks.
	 */

	// send a token from the dev address to the recipient address
	//_, err := callSendTx(sendValue, recipientAddress, models.DevAddress)
	//if err != nil {
	//	fmt.Printf("callSendTx error: %v\n", err)
	//	return
	//}
	//fmt.Println()

	_, err := callSendTxWithDynamicFee(recipientAddress, models.DevAddress, logger)
	if err != nil {
		logger.Error("callSendTxWithDynamicFee", "error", err)
		return
	}
	fmt.Println()

	// initiate a contract transaction
	//fmt.Println("INITIATING A CONTRACT TRANSACTION...")
	//_, err := callContractTx()
	//if err != nil {
	//	fmt.Printf("callContractTx error: %v\n", err)
	//	return
	//}
	//fmt.Println()

	logger.Info("SEND SIGNAL TO QUIT ALL RUNNING NODES")
	models.QuitNodeChan <- true
}
