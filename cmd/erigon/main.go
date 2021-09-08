package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/params"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli"
)

func main() {
	defer debug.LogPanic()
	app := erigoncli.MakeApp(runErigon, erigoncli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) {
	logger := log.New()
	// initializing the node and providing the current git commit there
	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
	nodeCfg := node.NewNodConfigUrfave(cliCtx)
	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg)

	ethNode, err := node.New(nodeCfg, ethCfg, logger)
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return
	}
	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
}
