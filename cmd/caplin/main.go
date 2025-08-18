// Copyright 2022 The Erigon Authors
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

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/c2h5oh/datasize"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/disk"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	execution_client2 "github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/cmd/caplin/caplincli"
	"github.com/erigontech/erigon/cmd/caplin/caplinflags"
	"github.com/erigontech/erigon/cmd/sentinel/sentinelflags"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/diagnostics/mem"
	"github.com/erigontech/erigon/turbo/app"
	"github.com/erigontech/erigon/turbo/debug"
)

func main() {
	app := app.MakeApp("caplin", runCaplinNode, append(caplinflags.CliFlags, sentinelflags.CliFlags...))
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runCaplinNode(cliCtx *cli.Context) error {
	cfg, err := caplincli.SetupCaplinCli(cliCtx)
	if err != nil {
		log.Error("[Phase1] Could not initialize caplin", "err", err)
		return err
	}
	if _, _, _, _, err := debug.Setup(cliCtx, true /* root logger */); err != nil {
		return err
	}
	rcfg := beacon_router_configuration.RouterConfiguration{
		Protocol:         cfg.BeaconProtocol,
		Address:          cfg.BeaconAddr,
		ReadTimeTimeout:  cfg.BeaconApiReadTimeout,
		WriteTimeout:     cfg.BeaconApiWriteTimeout,
		IdleTimeout:      cfg.BeaconApiWriteTimeout,
		AllowedOrigins:   cfg.AllowedOrigins,
		AllowedMethods:   cfg.AllowedMethods,
		AllowCredentials: cfg.AllowCredentials,
	}
	if err := rcfg.UnwrapEndpointsList(cfg.AllowedEndpoints); err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(cfg.LogLvl), log.StderrHandler))
	log.Info("[Phase1]", "chain", cliCtx.String(utils.ChainFlag.Name))
	log.Info("[Phase1] Running Caplin")

	// setup periodic logging and prometheus updates
	go mem.LogMemStats(cliCtx.Context, log.Root())
	go disk.UpdateDiskStats(cliCtx.Context, log.Root())

	// Either start from genesis or a checkpoint
	ctx, cn := context.WithCancel(cliCtx.Context)
	defer cn()

	var executionEngine execution_client2.ExecutionEngine
	if cfg.RunEngineAPI {
		cc, err := execution_client2.NewExecutionClientRPC(cfg.JwtSecret, cfg.EngineAPIAddr, cfg.EngineAPIPort)
		if err != nil {
			log.Error("could not start engine api", "err", err)
		}
		log.Info("Started Engine API RPC Client", "addr", cfg.EngineAPIAddr)
		executionEngine = cc
	}
	chainName := cliCtx.String(utils.ChainFlag.Name)
	_, _, networkId, err := clparams.GetConfigsByNetworkName(chainName)
	if err != nil {
		log.Info("Could not get network id from chain name, setting it to custom network id")
		networkId = clparams.CustomNetwork
	}

	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))

	return caplin1.RunCaplinService(ctx, executionEngine, clparams.CaplinConfig{
		CaplinDiscoveryAddr:       cfg.Addr,
		CaplinDiscoveryPort:       uint64(cfg.Port),
		CaplinDiscoveryTCPPort:    uint64(cfg.ServerTcpPort),
		BeaconAPIRouter:           rcfg,
		NetworkId:                 networkId,
		MevRelayUrl:               cfg.MevRelayUrl,
		CustomConfigPath:          cfg.CustomConfig,
		CustomGenesisStatePath:    cfg.CustomGenesisState,
		MaxPeerCount:              cfg.MaxPeerCount,
		MaxInboundTrafficPerPeer:  datasize.MB,
		MaxOutboundTrafficPerPeer: datasize.MB,
	}, cfg.Dirs, nil, nil, nil, blockSnapBuildSema)
}
