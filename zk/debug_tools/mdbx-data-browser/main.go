package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/params"
	cli2 "github.com/erigontech/erigon/turbo/cli"
	"github.com/erigontech/erigon/turbo/logging"
)

func main() {
	app := cli2.NewApp(params.GitCommit, "MDBX data browser")
	app.Commands = []*cli.Command{
		getBatchByNumberCmd,
		getBlockByNumberCmd,
		getBatchAffiliationCmd,
	}

	logging.SetupLogger("mdbx data browser")

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
