// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

// Package node contains classes for running an Erigon node.
package node

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/gdbme"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/params"
	erigoncli "github.com/erigontech/erigon/turbo/cli"
	"github.com/urfave/cli/v2"
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

func NewNodConfigUrfave(ctx *cli.Context, logger log.Logger) (*nodecfg.Config, error) {
	// If we're running a known preset, log it for convenience.
	chain := ctx.String(utils.ChainFlag.Name)
	switch chain {
	case networkname.Holesky:
		logger.Info("Starting Erigon on Holesky testnet...")
	case networkname.Sepolia:
		logger.Info("Starting Erigon on Sepolia testnet...")
	case networkname.Hoodi:
		logger.Info("Starting Erigon on Hoodi testnet...")
	case networkname.Dev:
		logger.Info("Starting Erigon in ephemeral dev mode...")
	case networkname.Amoy:
		logger.Info("Starting Erigon on Amoy testnet...")
	case networkname.BorMainnet:
		logger.Info("Starting Erigon on Bor Mainnet...")
	case networkname.BorDevnet:
		logger.Info("Starting Erigon on Bor Devnet...")
	case networkname.Gnosis:
		logger.Info("Starting Erigon on Gnosis Mainnet...")
	case networkname.Chiado:
		logger.Info("Starting Erigon on Chiado testnet...")
	case "", networkname.Mainnet:
		if !ctx.IsSet(utils.NetworkIdFlag.Name) {
			logger.Info("Starting Erigon on Ethereum mainnet...")
		}
	default:
		logger.Info("Starting Erigon on", "devnet", chain)
	}

	nodeConfig := NewNodeConfig()
	if err := utils.SetNodeConfig(ctx, nodeConfig, logger); err != nil {
		return nil, err
	}
	erigoncli.ApplyFlagsForNodeConfig(ctx, nodeConfig, logger)

	if ctx.Bool(utils.GDBMeFlag.Name) {
		gdbme.RestartUnderGDB()
	}

	return nodeConfig, nil
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
	tracer *tracers.Tracer,
) (*ErigonNode, error) {
	//prepareBuckets(optionalParams.CustomBuckets)
	node, err := node.New(ctx, nodeConfig, logger)
	if err != nil {
		utils.Fatalf("Failed to create Erigon node: %v", err)
	}

	ethereum, err := eth.New(ctx, node, ethConfig, logger, tracer)
	if err != nil {
		return nil, err
	}
	err = ethereum.Init(node, ethConfig, ethereum.ChainConfig())
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
