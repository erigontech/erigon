package main

import (
	"github.com/ledgerwatch/erigon/cmd/devnettest/erigon"
	"github.com/ledgerwatch/erigon/cmd/devnettest/shell"
	"github.com/ledgerwatch/erigon/cmd/devnettest/utils"
)

func main() {
	erigon.StartProcess(&utils.RPCFlags{WebsocketEnabled: false})
	//err := commands.Execute()
	//if err != nil {
	//	panic(err)
	//}
	shell.Execute()
}
