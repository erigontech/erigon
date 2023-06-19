package commands

import (
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	scenarios.RegisterStepHandlers(
		scenarios.StepHandler(services.CheckTxPoolContent),
		scenarios.StepHandler(services.InitSubscriptions),
	)
}

// ExecuteAllMethods runs all the simulation tests for erigon devnet
func ExecuteAllMethods(nw *devnet.Network, logger log.Logger) {
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

	// initiate a contract transaction
	//fmt.Println("INITIATING A CONTRACT TRANSACTION...")
	//_, err := callContractTx()
	//if err != nil {
	//	fmt.Printf("callContractTx error: %v\n", err)
	//	return
	//}
	//fmt.Println()
}
