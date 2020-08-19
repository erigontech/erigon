package main

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/integration/commands"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"os"
)

func main() {
	rootCmd := commands.GetRootCommand()
	if err := debug.SetupCobra(rootCmd); err != nil {
		panic(err)
	}

	if err := rootCmd.ExecuteContext(utils.RootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
