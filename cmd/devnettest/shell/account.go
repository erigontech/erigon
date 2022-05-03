package shell

import (
	"github.com/abiosoft/ishell/v2"
	"github.com/ledgerwatch/erigon/cmd/devnettest/console"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/cmd/devnettest/utils"
	"github.com/ledgerwatch/erigon/common"
)

func getBalance(ctx *ishell.Context, s *ishell.Shell) {
	ctx.Print("Addr: ")
	addr := ctx.ReadLine()
	ctx.Printf("Address to get balance for: %s\n", addr)

	blockNum := "latest"

	if !common.IsHexAddress(addr) {
		ctx.Printf("address: %v, is not a valid hex address\n", addr)
		s.Close()
		return
	}
	address := common.HexToAddress(addr)
	console.StartProcess(&utils.RPCFlags{WebsocketEnabled: false})
	if err := requests.GetBalance(reqId, address, blockNum); err != nil {
		ctx.Printf("could not get balance: %v\n", err)
		s.Close()
	}
}

func sendTx(ctx *ishell.Context, s *ishell.Shell) {
	ctx.Print("Value: ")
	val := ctx.ReadLine()
	ctx.Printf("Attempting to send %s ETH\n", val)

	ctx.Print("Addr: ")
	addr := ctx.ReadLine()
	ctx.Printf("Address to send %s ETH to is %s\n", val, addr)
}
