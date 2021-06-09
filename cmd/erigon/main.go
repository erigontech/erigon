package main

import (
	"fmt"
	"net"
	"os"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/log"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/urfave/cli"
)

var (
	// Following vars are injected through the build flags (see Makefile)
	gitCommit string
	gitBranch string
	gitTag    string
)

func main() {
	// creating a erigon-api app with all defaults
	app := erigoncli.MakeApp(runErigon, erigoncli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) {
	// creating staged sync with all default parameters

	ctx, _ := utils.RootContext()

	// initializing the node and providing the current git commit there
	log.Info("Build info", "git_branch", gitBranch, "git_tag", gitTag, "git_commit", gitCommit)
	eri := node.New(cliCtx, node.Params{GitCommit: gitCommit, GitBranch: gitBranch})
	eri.SetP2PListenFunc(func(network, addr string) (net.Listener, error) {
		var lc net.ListenConfig
		return lc.Listen(ctx, network, addr)
	})
	// running the node
	err := eri.Serve()

	if err != nil {
		log.Error("error while serving a Erigon node", "err", err)
	}
}
