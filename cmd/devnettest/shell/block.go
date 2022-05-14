package shell

import (
	"context"
	"github.com/abiosoft/ishell/v2"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"strconv"
)

func sendTx(ctx *ishell.Context, s *ishell.Shell) {
	var searchBlock bool
	txType := "regular"

	ctx.Print("Value: ")
	val := ctx.ReadLine()
	sendValue, err := strconv.ParseInt(val, 10, 64)
	if err != nil || sendValue <= 0 {
		ctx.Printf("invalid type for value, must be a positive number\n", err)
	}
	ctx.Printf("Attempting to send %d ETH\n", sendValue)

	ctx.Print("Addr: ")
	sendAddr := ctx.ReadLine()
	ctx.Printf("Address to send %d ETH to is %s\n", sendValue, sendAddr)

	//erigon.StartProcess(&utils.RPCFlags{WebsocketEnabled: false})

	nonce, err := services.GetNonce(reqId)
	if err != nil {
		ctx.Printf("failed to get latest nonce: %v\n", err)
		return
	}

	// subscriptionContract is the handler to the contract for further operations
	signedTx, address, subscriptionContract, transactOpts, err := services.CreateTransaction(txType, sendAddr, uint64(sendValue), nonce, searchBlock)
	if err != nil {
		ctx.Printf("failed to create transaction: %v\n", err)
		return
	}

	res, hash, err := requests.SendTx(reqId, signedTx)
	if err != nil {
		ctx.Printf("failed to send transaction: %v\n", err)
		return
	}
	ctx.Printf(res)

	if searchBlock {
		if _, err := services.SearchBlockForTx(*hash); err != nil {
			ctx.Printf("error searching block for tx: %v\n", err)
			return
		}
	}

	// if the contract is not nil, then the initial transaction created a contract. Emit an event
	if subscriptionContract != nil {
		if err := services.EmitEventAndGetLogs(reqId, subscriptionContract, transactOpts, address); err != nil {
			ctx.Printf("failed to emit events: %v\n", err)
			return
		}
	} else {
		err := services.ApplyTransaction(context.Background(), *signedTx)
		if err != nil {
			ctx.Printf("failed to apply transaction: %v\n", err)
			return
		}
	}

	//erigon.Stop()
}
