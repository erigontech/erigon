package main

import (
	"github.com/ledgerwatch/turbo-geth/cmd/restapi/commands"
	"github.com/ledgerwatch/turbo-geth/log"
)

func main() {
	log.SetupDefaultTerminalLogger(log.LvlInfo)

	commands.Execute()
}
