// Package node contains classes for running a turbo-geth node.
package node

import (
	"net"
	"time"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/eth"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/params"
	turbocli "github.com/ledgerwatch/erigon/turbo/cli"

	"github.com/urfave/cli"
)

// TurboGethNode represents a single node, that runs sync and p2p network.
// it also can export the private endpoint for RPC daemon, etc.
type TurboGethNode struct {
	stack   *node.Node
	backend *eth.Ethereum
}

func (tg *TurboGethNode) SetP2PListenFunc(listenFunc func(network, addr string) (net.Listener, error)) {
	tg.stack.SetP2PListenFunc(listenFunc)
}

// Serve runs the node and blocks the execution. It returns when the node is existed.
func (tg *TurboGethNode) Serve() error {
	defer tg.stack.Close()

	tg.run()

	tg.stack.Wait()

	return nil
}

func (tg *TurboGethNode) run() {
	utils.StartNode(tg.stack)
	// we don't have accounts locally and we don't do mining
	// so these parts are ignored
	// see cmd/geth/main.go#startNode for full implementation
}

// Params contains optional parameters for creating a node.
// * GitCommit is a commit from which then node was built.
// * CustomBuckets is a `map[string]dbutils.BucketConfigItem`, that contains bucket name and its properties.
//
// NB: You have to declare your custom buckets here to be able to use them in the app.
type Params struct {
	GitCommit     string
	GitBranch     string
	CustomBuckets dbutils.BucketsCfg
}

// New creates a new `TurboGethNode`.
// * ctx - `*cli.Context` from the main function. Necessary to be able to configure the node based on the command-line flags
// * sync - `stagedsync.StagedSync`, an instance of staged sync, setup just as needed.
// * optionalParams - additional parameters for running a node.
func New(
	ctx *cli.Context,
	sync *stagedsync.StagedSync,
	optionalParams Params,
) *TurboGethNode {
	prepareBuckets(optionalParams.CustomBuckets)
	prepare(ctx)

	nodeConfig := NewNodeConfig(optionalParams)
	utils.SetNodeConfig(ctx, nodeConfig)
	turbocli.ApplyFlagsForNodeConfig(ctx, nodeConfig)

	node := makeConfigNode(nodeConfig)
	ethConfig := makeEthConfig(ctx, node)

	ethConfig.StagedSync = sync

	ethereum := RegisterEthService(node, ethConfig, optionalParams.GitCommit)

	metrics.AddCallback(ethereum.ChainKV().CollectMetrics)

	return &TurboGethNode{stack: node, backend: ethereum}
}

// RegisterEthService adds an Ethereum client to the stack.
func RegisterEthService(stack *node.Node, cfg *ethconfig.Config, gitCommit string) *eth.Ethereum {
	backend, err := eth.New(stack, cfg, gitCommit)
	if err != nil {
		panic(err)
	}
	return backend
}

func makeEthConfig(ctx *cli.Context, node *node.Node) *ethconfig.Config {
	ethConfig := &ethconfig.Defaults
	utils.SetEthConfig(ctx, node, ethConfig)
	turbocli.ApplyFlagsForEthConfig(ctx, ethConfig)
	return ethConfig
}

func NewNodeConfig(p Params) *node.Config {
	nodeConfig := node.DefaultConfig
	// see simiar changes in `cmd/geth/config.go#defaultNodeConfig`
	if commit := p.GitCommit; commit != "" {
		nodeConfig.Version = params.VersionWithCommit(commit, "")
	} else {
		nodeConfig.Version = params.Version
	}
	nodeConfig.IPCPath = "" // force-disable IPC endpoint
	nodeConfig.Name = "turbo-geth"
	return &nodeConfig
}

func makeConfigNode(config *node.Config) *node.Node {
	stack, err := node.New(config)
	if err != nil {
		utils.Fatalf("Failed to create turbo-geth node: %v", err)
	}

	return stack
}

// prepare manipulates memory cache allowance and setups metric system.
// This function should be called before launching devp2p stack.
func prepare(ctx *cli.Context) {
	// If we're running a known preset, log it for convenience.
	chain := ctx.GlobalString(utils.ChainFlag.Name)
	switch chain {
	case params.RopstenChainName:
		log.Info("Starting Turbo-Geth on Ropsten testnet...")

	case params.RinkebyChainName:
		log.Info("Starting Turbo-Geth on Rinkeby testnet...")

	case params.GoerliChainName:
		log.Info("Starting Turbo-Geth on Görli testnet...")

	case params.DevChainName:
		log.Info("Starting Turbo-Geth in ephemeral dev mode...")

	case "", params.MainnetChainName:
		if !ctx.GlobalIsSet(utils.NetworkIdFlag.Name) {
			log.Info("Starting Turbo-Geth on Ethereum mainnet...")
		}
	default:
		log.Info("Starting Turbo-Geth on", "devnet", chain)
	}

	// Start system runtime metrics collection
	go metrics.CollectProcessMetrics(10 * time.Second)
}
