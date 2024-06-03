// Copyright 2022 Erigon-Lightclient contributors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/common/disk"
	"github.com/ledgerwatch/erigon-lib/common/mem"
	"github.com/ledgerwatch/erigon/cl/beacon/beacon_router_configuration"
	"github.com/ledgerwatch/erigon/cl/persistence/db_config"
	"github.com/ledgerwatch/erigon/cl/phase1/core"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	execution_client2 "github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/ledgerwatch/erigon/eth/ethconfig"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/caplin/caplin1"
	"github.com/ledgerwatch/erigon/cmd/caplin/caplincli"
	"github.com/ledgerwatch/erigon/cmd/caplin/caplinflags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinelflags"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/erigon/turbo/debug"
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
		state, err = core.RetrieveBeaconState(ctx, cfg.BeaconCfg, cfg.CheckpointUri)
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

	indiciesDB, blobStorage, err := caplin1.OpenCaplinDatabase(ctx, db_config.DefaultDatabaseConfiguration, cfg.BeaconCfg, ethClock, cfg.Dirs.CaplinIndexing, cfg.Dirs.CaplinBlobs, executionEngine, false, 100_000)
	if err != nil {
		return err
	}

	return caplin1.RunCaplinPhase1(ctx, executionEngine, &ethconfig.Config{
		LightClientDiscoveryAddr:    cfg.Addr,
		LightClientDiscoveryPort:    uint64(cfg.Port),
		LightClientDiscoveryTCPPort: uint64(cfg.ServerTcpPort),
		BeaconRouter:                rcfg,
	}, cfg.NetworkCfg, cfg.BeaconCfg, ethClock, state, cfg.Dirs, nil, nil, false, false, false, indiciesDB, blobStorage, nil)
}
