package main

import (
	"fmt"
	"os"

	_ "github.com/ledgerwatch/erigon/core/snaptype" //hack

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/integration/commands"
)

func main() {
	rootCmd := commands.RootCommand()
	ctx, _ := common.RootContext()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
