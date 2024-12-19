package main

import (
	"fmt"
	"os"

	_ "github.com/erigontech/erigon/core/snaptype"        //hack
	_ "github.com/erigontech/erigon/polygon/bor/snaptype" //hack

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cmd/integration/commands"
)

func main() {
	rootCmd := commands.RootCommand()
	ctx, _ := common.RootContext()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
