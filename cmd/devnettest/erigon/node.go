package erigon

import (
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/devnettest/rpcdaemon"
	"github.com/ledgerwatch/erigon/cmd/devnettest/utils"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli"
	"os"
	"time"
)

var started bool

func RunNode() {
	defer func() {
		panicResult := recover()
		if panicResult == nil {
			return
		}

		log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
		os.Exit(1)
	}()

	fmt.Println("args 0: ", os.Args)
	app := erigonapp.MakeApp(runDevnet, utils.DefaultFlags)
	flags := GetCmdLineFlags()
	args := append(os.Args, flags...)
	fmt.Println("args 1: ", args)
	if err := app.Run(args); err != nil {
		fmt.Println("Error here")
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

func StartProcess(rpcFlags *utils.RPCFlags) {
	go RunNode()
	time.Sleep(10 * time.Second)
	go rpcdaemon.RunDaemon(rpcFlags)
	//var wg sync.WaitGroup
	//wg.Add(1)
	//wg.Wait()
}

func GetCmdLineFlags() []string {
	flags := []string{"--datadir=~/dev", "--chain=dev"} // TODO: change to ./dev
	if IsRunning() {
		fmt.Println("Node is running")
		return flags
	}
	fmt.Println("Node is not running")
	Start()
	return append(flags, "--verbosity=0")
}

func Start() {
	started = true
}

func Stop() {
	started = false
}

func IsRunning() bool {
	return started
}