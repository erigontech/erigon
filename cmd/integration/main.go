package main

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/integration/commands"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"os"
)

func main() {
	rootCmd := commands.RootCommand()
	if err := utils.SetupCobra(rootCmd); err != nil {
		panic(err)
	}
	defer utils.StopDebug()

	if err := rootCmd.ExecuteContext(utils.RootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
