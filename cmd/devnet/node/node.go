package node

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
)

type NetworkNode interface {
	Start(nw *Network, nodeNumber int) error
	getEnode() (string, error)
	node() *Node
}

type Node struct {
	*requests.RequestGenerator `arg:"-"`
	BuildDir                   string `arg:"positional" default:"./build/bin/devnet"`
	DataDir                    string `arg:"--datadir" default:"./dev"`
	Chain                      string `arg:"--chain" default:"dev"`
	ConsoleVerbosity           string `arg:"--log.console.verbosity" default:"0"`
	DirVerbosity               string `arg:"--log.dir.verbosity"`
	LogDirPath                 string `arg:"--log.dir.path"` //default:"./cmd/devnet/debug_logs"
	P2PProtocol                string `arg:"--p2p.protocol" default:"68"`
	Downloader                 string `arg:"--no-downloader" default:"true"`
	PrivateApiAddr             string `arg:"--private.api.addr" default:"localhost:9090"`
	HttpPort                   int    `arg:"--http.port" default:"8545"`
	AuthRpcPort                int    `arg:"--authrpc.port" default:"8551"`
	WSPort                     int    `arg:"-" default:"8546"` // flag not defined
	GRPCPort                   int    `arg:"-" default:"8547"` // flag not defined
	TCPPort                    int    `arg:"-" default:"8548"` // flag not defined
	StaticPeers                string `arg:"--staticpeers"`
	WithoutHeimdall            bool   `arg:"--bor.withoutheimdall" flag:"" default:"false"`
}

// getEnode returns the enode of the mining node
func (node Node) getEnode() (string, error) {
	reqCount := 0

	for {
		nodeInfo, err := node.AdminNodeInfo()

		if err != nil {
			if reqCount < 10 {
				var urlErr *url.Error
				if errors.As(err, &urlErr) {
					var opErr *net.OpError
					if errors.As(urlErr.Err, &opErr) {
						var callErr *os.SyscallError
						if errors.As(opErr.Err, &callErr) {
							if callErr.Syscall == "connectex" {
								reqCount++
								delay, _ := rand.Int(rand.Reader, big.NewInt(4))
								time.Sleep(time.Duration(delay.Int64()+1) * time.Second)
								continue
							}
						}
					}
				}
			}

			return "", err
		}

		enode, err := devnetutils.UniqueIDFromEnode(nodeInfo.Enode)

		if err != nil {
			return "", err
		}

		return enode, nil
	}
}

func (node *Node) configure(nw *Network, nodeNumber int) (err error) {
	node.DataDir = filepath.Join(nw.DataDir, fmt.Sprintf("%d", nodeNumber))

	//TODO add a log.dir.prefix arg and set it to the node name (node-%d)
	//node.LogDirPath = filepath.Join(nw.DataDir, "logs")

	node.Chain = nw.Chain

	node.StaticPeers = strings.Join(nw.peers, ",")

	node.PrivateApiAddr, _, err = portFromBase(nw.BasePrivateApiAddr, nodeNumber, 1)

	if err != nil {
		return err
	}

	httpApiAddr, apiPort, err := portFromBase(nw.BaseRPCAddr, nodeNumber, 5)

	if err != nil {
		return err
	}

	node.HttpPort = apiPort
	node.WSPort = apiPort + 1
	node.GRPCPort = apiPort + 2
	node.TCPPort = apiPort + 3
	node.AuthRpcPort = apiPort + 4

	node.RequestGenerator = requests.NewRequestGenerator("http://"+httpApiAddr, nw.Logger)

	return nil
}

type Miner struct {
	Node
	Mine      bool   `arg:"--mine" flag:"true"`
	DevPeriod string `arg:"--dev.period" default:"30"`
	HttpApi   string `arg:"--http.api" default:"admin,eth,erigon,web3,net,debug,trace,txpool,parity,ots"`
	WS        string `arg:"--ws" flag:"" default:"true"`
}

func (node *Miner) node() *Node {
	return &node.Node
}

func (node *Miner) Start(nw *Network, nodeNumber int) (err error) {
	err = node.configure(nw, nodeNumber)

	if err != nil {
		return err
	}

	switch node.Chain {
	case networkname.BorDevnetChainName:
		node.WithoutHeimdall = true
	}

	args, err := devnetutils.AsArgs(node)

	if err != nil {
		return err
	}

	nw.wg.Add(1)

	go startNode(&nw.wg, args, nodeNumber, nw.Logger)

	return nil
}

type NonMiner struct {
	Node
	HttpApi     string `arg:"--http.api" default:"eth,debug,net,trace,web3,erigon"`
	TorrentPort string `arg:"--torrent.port" default:"42070"`
	NoDiscover  string `arg:"--nodiscover" flag:"" default:"true"`
}

func (node *NonMiner) node() *Node {
	return &node.Node
}

func (node *NonMiner) Start(nw *Network, nodeNumber int) (err error) {
	err = node.configure(nw, nodeNumber)

	if err != nil {
		return err
	}

	args, err := devnetutils.AsArgs(node)

	if err != nil {
		return err
	}

	nw.wg.Add(1)

	go startNode(&nw.wg, args, nodeNumber, nw.Logger)

	return nil
}

func portFromBase(baseAddr string, increment int, portCount int) (string, int, error) {
	apiHost, apiPort, err := net.SplitHostPort(baseAddr)

	if err != nil {
		return "", -1, err
	}

	portNo, err := strconv.Atoi(apiPort)

	if err != nil {
		return "", -1, err
	}

	portNo += (increment * portCount)

	return fmt.Sprintf("%s:%d", apiHost, portNo), portNo, nil
}

// startNode starts an erigon node on the dev chain
func startNode(wg *sync.WaitGroup, args []string, nodeNumber int, logger log.Logger) {
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
		//TODO - this should be at process end not here wg should be enough
		os.Exit(1)
	}()

	app := erigonapp.MakeApp(fmt.Sprintf("node-%d", nodeNumber), runNode, erigoncli.DefaultFlags)

	if err := app.Run(args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			logger.Warn("Error writing app run error to stderr", "err", printErr)
		}
		wg.Done()
		//TODO - this should be at process end not here wg should be enough
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
	ethCfg := node.NewEthConfigUrfave(ctx, nodeCfg, logger)

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
