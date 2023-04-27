package node

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
)

// Holds the number id of each node on the network, the first node is node 0
var nodeNumber int

// Start starts the process for two erigon nodes running on the dev chain
func Start(wg *sync.WaitGroup) {
	// add one goroutine to the wait-list
	wg.Add(1)

	// start the first node
	go StartNode(wg, miningNodeArgs())

	// sleep for a while to allow first node to start
	time.Sleep(time.Second * 10)

	// get the enode of the first node
	enode, err := getEnode()
	if err != nil {
		// TODO: Log the error, it means node did not start well
		fmt.Printf("error starting the node: %s\n", err)
	}

	// add one goroutine to the wait-list
	wg.Add(1)

	// start the second node, connect it to the mining node with the enode
	go StartNode(wg, nonMiningNodeArgs(2, enode))
}

// StartNode starts an erigon node on the dev chain
func StartNode(wg *sync.WaitGroup, args []string) {
	fmt.Printf("\nRunning node %d with flags ==> %v\n", nodeNumber, args)

	// catch any errors and avoid panics if an error occurs
	defer func() {
		panicResult := recover()
		if panicResult == nil {
			wg.Done()
			return
		}

		log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
		wg.Done()
		os.Exit(1)
	}()

	app := erigonapp.MakeApp("devnet", runNode, erigoncli.DefaultFlags)
	nodeNumber++ // increment the number of nodes on the network
	if err := app.Run(args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Error writing app run error to stderr", "err", printErr)
		}
		wg.Done()
		os.Exit(1)
	}
}

// runNode configures, creates and serves an erigon node
func runNode(ctx *cli.Context) error {
	// Initializing the node and providing the current git commit there
	log.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	nodeCfg := node.NewNodConfigUrfave(ctx)
	ethCfg := node.NewEthConfigUrfave(ctx, nodeCfg)

	ethNode, err := node.New(nodeCfg, ethCfg)
	if err != nil {
		log.Error("Devnet startup", "err", err)
		return err
	}

	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving Devnet node", "err", err)
	}
	return err
}

// miningNodeArgs returns custom args for starting a mining node
func miningNodeArgs() []string {
	dataDir, _ := models.ParameterFromArgument(models.DataDirArg, models.DataDirParam+fmt.Sprintf("%d", nodeNumber))
	chainType, _ := models.ParameterFromArgument(models.ChainArg, models.ChainParam)
	devPeriod, _ := models.ParameterFromArgument(models.DevPeriodArg, models.DevPeriodParam)
	privateApiAddr, _ := models.ParameterFromArgument(models.PrivateApiAddrArg, models.PrivateApiParamMine)
	httpApi, _ := models.ParameterFromArgument(models.HttpApiArg, models.HttpApiParam)
	ws := models.WSArg
	consoleVerbosity, _ := models.ParameterFromArgument(models.ConsoleVerbosityArg, models.ConsoleVerbosityParam)
	logDir, _ := models.ParameterFromArgument(models.LogDirArg, models.LogDirParam+"/node_1")

	return []string{models.BuildDirArg, dataDir, chainType, privateApiAddr, models.Mine, httpApi, ws, devPeriod, consoleVerbosity, logDir}
}

// nonMiningNodeArgs returns custom args for starting a non-mining node
func nonMiningNodeArgs(nodeNumber int, enode string) []string {
	dataDir, _ := models.ParameterFromArgument(models.DataDirArg, models.DataDirParam+fmt.Sprintf("%d", nodeNumber))
	chainType, _ := models.ParameterFromArgument(models.ChainArg, models.ChainParam)
	privateApiAddr, _ := models.ParameterFromArgument(models.PrivateApiAddrArg, models.PrivateApiParamNoMine)
	staticPeers, _ := models.ParameterFromArgument(models.StaticPeersArg, enode)
	consoleVerbosity, _ := models.ParameterFromArgument(models.ConsoleVerbosityArg, models.ConsoleVerbosityParam)
	logDir, _ := models.ParameterFromArgument(models.LogDirArg, models.LogDirParam+"/node_2")
	torrentPort, _ := models.ParameterFromArgument(models.TorrentPortArg, models.TorrentPortParam)

	return []string{models.BuildDirArg, dataDir, chainType, privateApiAddr, staticPeers, models.NoDiscover, consoleVerbosity, logDir, torrentPort}
}

// getEnode returns the enode of the mining node
func getEnode() (string, error) {
	nodeInfo, err := requests.AdminNodeInfo(0)
	if err != nil {
		return "", err
	}

	enode, err := devnetutils.UniqueIDFromEnode(nodeInfo.Enode)
	if err != nil {
		return "", err
	}

	return enode, nil
}

// QuitOnSignal stops the node goroutines after all checks have been made on the devnet
func QuitOnSignal(wg *sync.WaitGroup) {
	models.QuitNodeChan = make(chan bool)
	go func() {
		for <-models.QuitNodeChan {
			wg.Done()
			wg.Done()
		}
	}()
}
