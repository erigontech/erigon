package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/node"

	turbocli "github.com/ledgerwatch/turbo-geth/turbo/cli"

	"github.com/urfave/cli"
)

var (
	// gitCommit is injected through the build flags (see Makefile)
	gitCommit string
)

func main() {
	// creating a turbo-api app with all defaults
	app := turbocli.MakeApp(runTurboGeth, turbocli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func runTurboGeth(ctx *cli.Context) {
	// creating staged sync with all default parameters
	sync := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
	)

	// initializing the node and providing the current git commit there
	tg := node.New(ctx, sync, node.Params{GitCommit: gitCommit})
	// running the node
	err := tg.Serve()

	if err != nil {
		log.Error("error while serving a turbo-geth node", "err", err)
	}
}
