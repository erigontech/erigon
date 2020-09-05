package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	turbocli "github.com/ledgerwatch/turbo-geth/turbo/cli"

	"github.com/urfave/cli"
)

func main() {
	app := turbocli.MakeApp(runTurboGeth, turbocli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runTurboGeth(ctx *cli.Context) {
	sync := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
	)

	tg := turbocli.New(ctx, sync)
	tg.Serve()
}
