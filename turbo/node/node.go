// Package node contains classes for running a Erigon node.
package node

import (
	"net"
	"time"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/eth"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/params"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"

	"github.com/urfave/cli"
)

// ErigonNode represents a single node, that runs sync and p2p network.
// it also can export the private endpoint for RPC daemon, etc.
type ErigonNode struct {
	stack   *node.Node
	backend *eth.Ethereum
}

func (eri *ErigonNode) SetP2PListenFunc(listenFunc func(network, addr string) (net.Listener, error)) {
	eri.stack.SetP2PListenFunc(listenFunc)
}

// Serve runs the node and blocks the execution. It returns when the node is existed.
func (eri *ErigonNode) Serve() error {
	defer eri.stack.Close()

	eri.run()

	eri.stack.Wait()

	return nil
}

func (eri *ErigonNode) run() {
	utils.StartNode(eri.stack)
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
	CustomBuckets dbutils.BucketsCfg
}

// New creates a new `ErigonNode`.
// * ctx - `*cli.Context` from the main function. Necessary to be able to configure the node based on the command-line flags
// * sync - `stagedsync.StagedSync`, an instance of staged sync, setup just as needed.
// * optionalParams - additional parameters for running a node.
func New(
	ctx *cli.Context,
	optionalParams Params,
) *ErigonNode {
	prepareBuckets(optionalParams.CustomBuckets)
	prepare(ctx)

	nodeConfig := NewNodeConfig()
	utils.SetNodeConfig(ctx, nodeConfig)
	erigoncli.ApplyFlagsForNodeConfig(ctx, nodeConfig)

	node := makeConfigNode(nodeConfig)
	ethConfig := makeEthConfig(ctx, node)

	ethereum := RegisterEthService(node, ethConfig)

	metrics.AddCallback(ethereum.ChainKV().CollectMetrics)

	return &ErigonNode{stack: node, backend: ethereum}
}

// RegisterEthService adds an Ethereum client to the stack.
func RegisterEthService(stack *node.Node, cfg *ethconfig.Config) *eth.Ethereum {
	backend, err := eth.New(stack, cfg)
	if err != nil {
		panic(err)
	}
	return backend
}

func makeEthConfig(ctx *cli.Context, node *node.Node) *ethconfig.Config {
	ethConfig := &ethconfig.Defaults
	utils.SetEthConfig(ctx, node, ethConfig)
	erigoncli.ApplyFlagsForEthConfig(ctx, ethConfig)
	return ethConfig
}

func NewNodeConfig() *node.Config {
	nodeConfig := node.DefaultConfig
	// see simiar changes in `cmd/geth/config.go#defaultNodeConfig`
	if commit := params.GitCommit; commit != "" {
		nodeConfig.Version = params.VersionWithCommit(commit, "")
	} else {
		nodeConfig.Version = params.Version
	}
	nodeConfig.IPCPath = "" // force-disable IPC endpoint
	nodeConfig.Name = "erigon"
	return &nodeConfig
}

func makeConfigNode(config *node.Config) *node.Node {
	stack, err := node.New(config)
	if err != nil {
		utils.Fatalf("Failed to create Erigon node: %v", err)
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
		log.Info("Starting Erigon on Ropsten testnet...")

	case params.RinkebyChainName:
		log.Info("Starting Erigon on Rinkeby testnet...")

	case params.GoerliChainName:
		log.Info("Starting Erigon on Görli testnet...")

	case params.DevChainName:
		log.Info("Starting Erigon in ephemeral dev mode...")

	case "", params.MainnetChainName:
		if !ctx.GlobalIsSet(utils.NetworkIdFlag.Name) {
			log.Info("Starting Erigon on Ethereum mainnet...")
		}
	default:
		log.Info("Starting Erigon on", "devnet", chain)
	}

	// Start system runtime metrics collection
	go metrics.CollectProcessMetrics(10 * time.Second)
}
