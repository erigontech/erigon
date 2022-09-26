package node

import (
	"fmt"
	"github.com/ledgerwatch/erigon/turbo/node"
	"os"
	"sync"

	"github.com/urfave/cli"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/log/v3"
)

// StartNode starts an erigon node on the dev chain
func StartNode(wg *sync.WaitGroup) {
	// catch any errors and avoid panics if an error occurs
	defer func() {
		panicResult := recover()
		if panicResult == nil {
			fmt.Println("Panic is nil")
			wg.Done()
			return
		}

		log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
		wg.Done()
		os.Exit(1)
	}()

	dataDir, _ := models.ParameterFromArgument(models.DataDirArg, models.DataDirParam)
	chainType, _ := models.ParameterFromArgument(models.ChainArg, models.ChainParam)
	devPeriod, _ := models.ParameterFromArgument(models.DevPeriodArg, models.DevPeriodParam)
	verbosity, _ := models.ParameterFromArgument(models.VerbosityArg, models.VerbosityParam)

	args := []string{models.BuildDirArg, dataDir, chainType, models.Mine, devPeriod, verbosity}
	app := erigonapp.MakeApp(runNode, erigoncli.DefaultFlags)
	if err := app.Run(args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Error writing app run error to stderr", "err", printErr)
		}
		wg.Done()
		os.Exit(1)
	}
}

func runNode(ctx *cli.Context) {
	logger := log.New()

	// Initializing the node and providing the current git commit there
	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	nodeCfg := node.NewNodConfigUrfave(ctx)
	ethCfg := node.NewEthConfigUrfave(ctx, nodeCfg)

	ethNode, err := node.New(nodeCfg, ethCfg, logger)
	if err != nil {
		log.Error("Devnet startup", "err", err)
		return
	}

	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving Devnet node", "err", err)
	}
}
