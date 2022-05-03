package erigon

import (
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/devnettest/utils"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli"
	"os"
)

func RunNode() {
	defer func() {
		panicResult := recover()
		if panicResult == nil {
			return
		}

		log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
		os.Exit(1)
	}()

	app := erigonapp.MakeApp(runDevnet, utils.DefaultFlags)
	app.Flags = append(app.Flags, utils.DebugFlags...)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runDevnet(cliCtx *cli.Context) {
	logger := log.New()
	// initializing the node and providing the current git commit there
	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	nodeCfg := node.NewNodConfigUrfave(cliCtx)
	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg)

	ethNode, err := node.New(nodeCfg, ethCfg, logger)
	if err != nil {
		log.Error("Devnet startup", "err", err)
		return
	}
	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving a Devnet node", "err", err)
	}
}
