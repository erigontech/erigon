package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/cmd/restapi/commands"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/log"
)

func main() {
	log.SetupDefaultTerminalLogger(log.LvlInfo, "", "")

	if err := commands.RootCommand().ExecuteContext(utils.RootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
