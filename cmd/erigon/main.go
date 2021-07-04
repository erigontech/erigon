package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/urfave/cli"
)

func main() {
	defer debug.LogPanic()
	// creating a erigon-api app with all defaults
	app := erigoncli.MakeApp(runErigon, erigoncli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) {
	// initializing the node and providing the current git commit there
	log.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
	nodeCfg := node.NewNodConfigUrfave(cliCtx)
	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg)
	if cliCtx.GlobalIsSet(utils.DataDirFlag.Name) {
		// Check if we have an already initialized chain and fall back to
		// that if so. Otherwise we need to generate a new genesis spec.
		chaindb := utils.MakeChainDatabase(nodeCfg)
		if err := chaindb.View(context.Background(), func(tx ethdb.Tx) error {
			h, err := rawdb.ReadCanonicalHash(tx, 0)
			if err != nil {
				panic(err)
			}
			if h != (common.Hash{}) {
				ethCfg.Genesis = nil // fallback to db content
			}
			return nil
		}); err != nil {
			panic(err)
		}
		chaindb.Close()
	}

	err := node.New(nodeCfg, ethCfg).Serve()
	if err != nil {
		log.Error("error while serving a Erigon node", "err", err)
	}
}
