package main

import (
	"fmt"
	"os"
	"path/filepath"
	dbg "runtime/debug"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cmd/devnet/commands"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/node"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/urfave/cli/v2"
)

var DataDirFlag = flags.DirectoryFlag{
	Name:     "datadir",
	Usage:    "Data directory for the devnet",
	Value:    flags.DirectoryString(""),
	Required: true,
}

type PanicHandler struct {
}

func (ph PanicHandler) Log(r *log.Record) error {
	fmt.Printf("Stack: %s\n", dbg.Stack())
	os.Exit(1)
	return nil
}

func main() {

	debug.RaiseFdLimit()

	app := cli.NewApp()
	app.Version = params.VersionWithCommit(params.GitCommit)
	app.Action = func(ctx *cli.Context) error {
		return action(ctx)
	}
	app.Flags = []cli.Flag{
		&DataDirFlag,
	}

	app.After = func(ctx *cli.Context) error {
		// unsubscribe from all the subscriptions made
		services.UnsubscribeAll()
		return nil
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func action(ctx *cli.Context) error {
	dataDir := ctx.String("datadir")
	logsDir := filepath.Join(dataDir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return err
	}
	logger := logging.SetupLoggerCtx("devnet", ctx, false /* rootLogger */)
	reqGen := requests.NewRequestGenerator(logger)

	// Make root logger fail
	log.Root().SetHandler(PanicHandler{})

	// clear all the dev files
	if err := devnetutils.ClearDevDB(dataDir, logger); err != nil {
		return err
	}
	// wait group variable to prevent main function from terminating until routines are finished
	var wg sync.WaitGroup

	// start the first erigon node in a go routine
	node.Start(reqGen, &wg, dataDir, logger)

	// send a quit signal to the quit channels when done making checks
	node.QuitOnSignal(&wg)

	// sleep for seconds to allow the nodes fully start up
	time.Sleep(time.Second * 10)

	// start up the subscription services for the different sub methods
	services.InitSubscriptions([]models.SubMethod{models.ETHNewHeads}, logger)

	// execute all rpc methods amongst the two nodes
	commands.ExecuteAllMethods(reqGen, logger)

	// wait for all goroutines to complete before exiting
	wg.Wait()
	return nil
}
