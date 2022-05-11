package shell

import (
	"fmt"
	"github.com/abiosoft/ishell/v2"
	"github.com/ledgerwatch/erigon/cmd/devnettest/erigon"
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
	erigon.StartProcess(&utils.RPCFlags{WebsocketEnabled: false})
	res, err := requests.GetBalance(reqId, address, blockNum)
	if err != nil {
		res = fmt.Sprintf("could not get balance: %v\n", err)
	}
	ctx.Printf(res)
	//erigon.Stop()
}
