package node

import (
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
)

// Start starts the process for two erigon nodes running on the dev chain
func Start(wg *sync.WaitGroup, dataDir string, logger log.Logger) {
	// add one goroutine to the wait-list
	wg.Add(1)

	// start the first node
	go StartNode(wg, miningNodeArgs(dataDir, 1), 1, logger)

	// sleep for a while to allow first node to start
	time.Sleep(time.Second * 10)

	// get the enode of the first node
	enode, err := getEnode()
	if err != nil {
		logger.Error("Starting the node", "error", err)
	}

	// add one goroutine to the wait-list
	wg.Add(1)

	// start the second node, connect it to the mining node with the enode
	go StartNode(wg, nonMiningNodeArgs(dataDir, 2, enode), 2, logger)
}

// StartNode starts an erigon node on the dev chain
func StartNode(wg *sync.WaitGroup, args []string, nodeNumber int, logger log.Logger) {
	logger.Info("Running node", "number", nodeNumber, "args", args)

	// catch any errors and avoid panics if an error occurs
	defer func() {
		panicResult := recover()
		if panicResult == nil {
			wg.Done()
			return
		}

		logger.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
		wg.Done()
		os.Exit(1)
	}()

	app := erigonapp.MakeApp("devnet", runNode, erigoncli.DefaultFlags)
	if err := app.Run(args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			logger.Warn("Error writing app run error to stderr", "err", printErr)
		}
		wg.Done()
		os.Exit(1)
	}
}

// runNode configures, creates and serves an erigon node
func runNode(ctx *cli.Context) error {
	// Initializing the node and providing the current git commit there

	var logger log.Logger
	var err error
	if logger, err = debug.Setup(ctx, false /* rootLogger */); err != nil {
		return err
	}
	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	nodeCfg := node.NewNodConfigUrfave(ctx, logger)
	ethCfg := node.NewEthConfigUrfave(ctx, nodeCfg)

	ethNode, err := node.New(nodeCfg, ethCfg, logger)
	if err != nil {
		logger.Error("Devnet startup", "err", err)
		return err
	}

	err = ethNode.Serve()
	if err != nil {
		logger.Error("error while serving Devnet node", "err", err)
	}
	return err
}

// miningNodeArgs returns custom args for starting a mining node
func miningNodeArgs(dataDir string, nodeNumber int) []string {
	nodeDataDir := filepath.Join(dataDir, fmt.Sprintf("%d", nodeNumber))
	dataDirArg, _ := models.ParameterFromArgument(models.DataDirArg, nodeDataDir)
	chainType, _ := models.ParameterFromArgument(models.ChainArg, models.ChainParam)
	devPeriod, _ := models.ParameterFromArgument(models.DevPeriodArg, models.DevPeriodParam)
	privateApiAddr, _ := models.ParameterFromArgument(models.PrivateApiAddrArg, models.PrivateApiParamMine)
	httpApi, _ := models.ParameterFromArgument(models.HttpApiArg, models.HttpApiParam)
	ws := models.WSArg
	consoleVerbosity, _ := models.ParameterFromArgument(models.ConsoleVerbosityArg, models.ConsoleVerbosityParam)
	p2pProtocol, _ := models.ParameterFromArgument("--p2p.protocol", "68")
	downloaderArg, _ := models.ParameterFromArgument("--no-downloader", "true")

	return []string{models.BuildDirArg, dataDirArg, chainType, privateApiAddr, models.Mine, httpApi, ws, devPeriod, consoleVerbosity, p2pProtocol, downloaderArg}
}

// nonMiningNodeArgs returns custom args for starting a non-mining node
func nonMiningNodeArgs(dataDir string, nodeNumber int, enode string) []string {
	nodeDataDir := filepath.Join(dataDir, fmt.Sprintf("%d", nodeNumber))
	dataDirArg, _ := models.ParameterFromArgument(models.DataDirArg, nodeDataDir)
	chainType, _ := models.ParameterFromArgument(models.ChainArg, models.ChainParam)
	privateApiAddr, _ := models.ParameterFromArgument(models.PrivateApiAddrArg, models.PrivateApiParamNoMine)
	staticPeers, _ := models.ParameterFromArgument(models.StaticPeersArg, enode)
	consoleVerbosity, _ := models.ParameterFromArgument(models.ConsoleVerbosityArg, models.ConsoleVerbosityParam)
	torrentPort, _ := models.ParameterFromArgument(models.TorrentPortArg, models.TorrentPortParam)
	p2pProtocol, _ := models.ParameterFromArgument("--p2p.protocol", "68")
	downloaderArg, _ := models.ParameterFromArgument("--no-downloader", "true")

	return []string{models.BuildDirArg, dataDirArg, chainType, privateApiAddr, staticPeers, models.NoDiscover, consoleVerbosity, torrentPort, p2pProtocol, downloaderArg}
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
