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

	"github.com/urfave/cli/v2"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/disk"
	"github.com/erigontech/erigon-lib/common/mem"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	execution_client2 "github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/eth/ethconfig"

	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/cmd/caplin/caplincli"
	"github.com/erigontech/erigon/cmd/caplin/caplinflags"
	"github.com/erigontech/erigon/cmd/sentinel/sentinelflags"
	"github.com/erigontech/erigon/cmd/utils"
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
	if _, _, _, err := debug.Setup(cliCtx, true /* root logger */); err != nil {
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
	var state *state.CachingBeaconState
	if cfg.InitialSync {
		state = cfg.InitalState
	} else {
		state, err = checkpoint_sync.NewRemoteCheckpointSync(cfg.BeaconCfg, cfg.NetworkType).GetLatestBeaconState(ctx)
		if err != nil {
			return err
		}
	}

	ethClock := eth_clock.NewEthereumClock(state.GenesisTime(), state.GenesisValidatorsRoot(), cfg.BeaconCfg)

	// sentinel, err := service.StartSentinelService(&sentinel.SentinelConfig{
	// 	IpAddr:        cfg.Addr,
	// 	Port:          int(cfg.Port),
	// 	TCPPort:       cfg.ServerTcpPort,
	// 	GenesisConfig: cfg.GenesisCfg,
	// 	NetworkConfig: cfg.NetworkCfg,
	// 	BeaconConfig:  cfg.BeaconCfg,
	// 	NoDiscovery:   cfg.NoDiscovery,
	// 	EnableBlocks:  true,
	// }, nil, nil, &service.ServerConfig{Network: cfg.ServerProtocol, Addr: cfg.ServerAddr}, nil, &cltypes.Status{
	// 	ForkDigest:     forkDigest,
	// 	FinalizedRoot:  state.FinalizedCheckpoint().BlockRoot(),
	// 	FinalizedEpoch: state.FinalizedCheckpoint().Epoch(),
	// 	HeadSlot:       state.FinalizedCheckpoint().Epoch() * cfg.BeaconCfg.SlotsPerEpoch,
	// 	HeadRoot:       state.FinalizedCheckpoint().BlockRoot(),
	// }, log.Root())
	// if err != nil {
	// 	log.Error("Could not start sentinel", "err", err)
	// }

	// log.Info("Sentinel started", "addr", cfg.ServerAddr)

	if err != nil {
		log.Error("[Checkpoint Sync] Failed", "reason", err)
		return err
	}
	var executionEngine execution_client2.ExecutionEngine
	if cfg.RunEngineAPI {
		cc, err := execution_client2.NewExecutionClientRPC(cfg.JwtSecret, cfg.EngineAPIAddr, cfg.EngineAPIPort)
		if err != nil {
			log.Error("could not start engine api", "err", err)
		}
		log.Info("Started Engine API RPC Client", "addr", cfg.EngineAPIAddr)
		executionEngine = cc
	}

	indiciesDB, blobStorage, err := caplin1.OpenCaplinDatabase(ctx, cfg.BeaconCfg, ethClock, cfg.Dirs.CaplinIndexing, cfg.Dirs.CaplinBlobs, executionEngine, false, 100_000)
	if err != nil {
		return err
	}

	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))

	var options []caplin1.CaplinOption
	// builder option
	if rcfg.Builder {
		if cfg.MevRelayUrl == "" {
			log.Warn("builder mode requires mev_relay_url, but it is not set. Skipping builder mode")
			rcfg.Builder = false
		} else {
			log.Info("Starting with builder mode")
			options = append(options, caplin1.WithBuilder(cfg.MevRelayUrl, cfg.BeaconCfg))
		}
	}

	return caplin1.RunCaplinPhase1(ctx, executionEngine, &ethconfig.Config{
		CaplinDiscoveryAddr:    cfg.Addr,
		CaplinDiscoveryPort:    uint64(cfg.Port),
		CaplinDiscoveryTCPPort: uint64(cfg.ServerTcpPort),
		BeaconRouter:           rcfg,
		CaplinConfig:           clparams.CaplinConfig{},
	}, cfg.NetworkCfg, cfg.BeaconCfg, ethClock, state, cfg.Dirs, nil, nil, indiciesDB, blobStorage, nil, blockSnapBuildSema, options...)
}
