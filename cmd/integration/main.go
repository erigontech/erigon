package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/disk"
	"github.com/ledgerwatch/erigon-lib/common/mem"
	"github.com/ledgerwatch/erigon/cmd/integration/commands"
	"github.com/ledgerwatch/log/v3"
)

func main() {
	rootCmd := commands.RootCommand()
	ctx, _ := common.RootContext()

	go mem.LogMemStats(ctx, log.New())
	go disk.UpdateDiskStats(ctx, log.New())

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
