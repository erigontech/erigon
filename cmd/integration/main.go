package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cmd/integration/commands"
	"github.com/ledgerwatch/erigon/cmd/utils"
)

func main() {
	rootCmd := commands.RootCommand()
	ctx, _ := utils.RootContext()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
