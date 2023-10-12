// Package node contains classes for running a Erigon node.
package node

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/eth"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
)

// ErigonNode represents a single node, that runs sync and p2p network.
// it also can export the private endpoint for RPC daemon, etc.
type ErigonNode struct {
	stack   *node.Node
	backend *eth.Ethereum
}

// Serve runs the node and blocks the execution. It returns when the node is existed.
func (eri *ErigonNode) Serve() error {
	defer eri.Close()

	eri.run()

	eri.stack.Wait()

	return nil
}

func (eri *ErigonNode) Backend() *eth.Ethereum {
	return eri.backend
}

func (eri *ErigonNode) Node() *node.Node {
	return eri.stack
}

func (eri *ErigonNode) Close() {
	eri.stack.Close()
}

func (eri *ErigonNode) run() {
	node.StartNode(eri.stack)
	// we don't have accounts locally and we don't do mining
	// so these parts are ignored
	// see cmd/geth/daemon.go#startNode for full implementation
}

// Params contains optional parameters for creating a node.
// * GitCommit is a commit from which then node was built.
// * CustomBuckets is a `map[string]dbutils.TableCfgItem`, that contains bucket name and its properties.
//
// NB: You have to declare your custom buckets here to be able to use them in the app.
type Params struct {
	CustomBuckets kv.TableCfg
}

func NewNodConfigUrfave(ctx *cli.Context, logger log.Logger) *nodecfg.Config {
	// If we're running a known preset, log it for convenience.
	chain := ctx.String(utils.ChainFlag.Name)
	switch chain {
	case networkname.HoleskyChainName:
		logger.Info("Starting Erigon on Holesky testnet...")
	case networkname.SepoliaChainName:
		logger.Info("Starting Erigon on Sepolia testnet...")
	case networkname.GoerliChainName:
		logger.Info("Starting Erigon on Görli testnet...")
	case networkname.DevChainName:
		logger.Info("Starting Erigon in ephemeral dev mode...")
	case networkname.MumbaiChainName:
		logger.Info("Starting Erigon on Mumbai testnet...")
	case networkname.BorMainnetChainName:
		logger.Info("Starting Erigon on Bor Mainnet...")
	case networkname.BorDevnetChainName:
		logger.Info("Starting Erigon on Bor Devnet...")
	case "", networkname.MainnetChainName:
		if !ctx.IsSet(utils.NetworkIdFlag.Name) {
			logger.Info("Starting Erigon on Ethereum mainnet...")
		}
	default:
		logger.Info("Starting Erigon on", "devnet", chain)
	}

	nodeConfig := NewNodeConfig()
	utils.SetNodeConfig(ctx, nodeConfig, logger)
	erigoncli.ApplyFlagsForNodeConfig(ctx, nodeConfig, logger)
	return nodeConfig
}
func NewEthConfigUrfave(ctx *cli.Context, nodeConfig *nodecfg.Config, logger log.Logger) *ethconfig.Config {
	ethConfig := ethconfig.Defaults // Needs to be a copy, not pointer
	utils.SetEthConfig(ctx, nodeConfig, &ethConfig, logger)
	erigoncli.ApplyFlagsForEthConfig(ctx, &ethConfig, logger)

	return &ethConfig
}

// New creates a new `ErigonNode`.
// * ctx - `*cli.Context` from the main function. Necessary to be able to configure the node based on the command-line flags
// * sync - `stagedsync.StagedSync`, an instance of staged sync, setup just as needed.
// * optionalParams - additional parameters for running a node.
func New(
	ctx context.Context,
	nodeConfig *nodecfg.Config,
	ethConfig *ethconfig.Config,
	logger log.Logger,
) (*ErigonNode, error) {
	//prepareBuckets(optionalParams.CustomBuckets)
	node, err := node.New(ctx, nodeConfig, logger)
	if err != nil {
		utils.Fatalf("Failed to create Erigon node: %v", err)
	}

	ethereum, err := eth.New(ctx, node, ethConfig, logger)
	if err != nil {
		return nil, err
	}
	err = ethereum.Init(node, ethConfig)
	if err != nil {
		return nil, err
	}
	return &ErigonNode{stack: node, backend: ethereum}, nil
}

func NewNodeConfig() *nodecfg.Config {
	nodeConfig := nodecfg.DefaultConfig
	// see simiar changes in `cmd/geth/config.go#defaultNodeConfig`
	if commit := params.GitCommit; commit != "" {
		nodeConfig.Version = params.VersionWithCommit(commit)
	} else {
		nodeConfig.Version = params.Version
	}
	nodeConfig.IPCPath = "" // force-disable IPC endpoint
	nodeConfig.Name = "erigon"
	return &nodeConfig
}
