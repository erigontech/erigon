/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/beacon"
	"github.com/ledgerwatch/erigon/cl/beacon/handler"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cmd/caplin-phase1/caplin1"
	lcCli "github.com/ledgerwatch/erigon/cmd/sentinel/cli"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/service"
	lightclientapp "github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/erigon/turbo/debug"
)

func main() {
	app := lightclientapp.MakeApp("caplin-phase1", runCaplinNode, flags.CLDefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runCaplinNode(cliCtx *cli.Context) error {
	ctx, cn := context.WithCancel(context.Background())
	defer cn()

	cfg, err := lcCli.SetupConsensusClientCfg(cliCtx)
	if err != nil {
		log.Error("[Phase1] Could not initialize caplin", "err", err)
	}
	if _, err := debug.Setup(cliCtx, true /* root logger */); err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(cfg.LogLvl), log.StderrHandler))
	log.Info("[Phase1]", "chain", cliCtx.String(flags.Chain.Name))
	log.Info("[Phase1] Running Caplin")
	// Either start from genesis or a checkpoint
	var state *state.CachingBeaconState
	if cfg.InitialSync {
		state = cfg.InitalState
	} else {
		state, err = core.RetrieveBeaconState(ctx, cfg.BeaconCfg, cfg.GenesisCfg, cfg.CheckpointUri)
		if err != nil {
			return err
		}
	}

	forkDigest, err := fork.ComputeForkDigest(cfg.BeaconCfg, cfg.GenesisCfg)
	if err != nil {
		return err
	}

	sentinel, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:        cfg.Addr,
		Port:          int(cfg.Port),
		TCPPort:       cfg.ServerTcpPort,
		GenesisConfig: cfg.GenesisCfg,
		NetworkConfig: cfg.NetworkCfg,
		BeaconConfig:  cfg.BeaconCfg,
		NoDiscovery:   cfg.NoDiscovery,
	}, nil, &service.ServerConfig{Network: cfg.ServerProtocol, Addr: cfg.ServerAddr}, nil, &cltypes.Status{
		ForkDigest:     forkDigest,
		FinalizedRoot:  state.FinalizedCheckpoint().BlockRoot(),
		FinalizedEpoch: state.FinalizedCheckpoint().Epoch(),
		HeadSlot:       state.FinalizedCheckpoint().Epoch() * cfg.BeaconCfg.SlotsPerEpoch,
		HeadRoot:       state.FinalizedCheckpoint().BlockRoot(),
	}, log.Root())
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
	}

	log.Info("Sentinel started", "addr", cfg.ServerAddr)

	if err != nil {
		log.Error("[Checkpoint Sync] Failed", "reason", err)
		return err
	}
	var executionEngine execution_client.ExecutionEngine
	if cfg.RunEngineAPI {
		fmt.Println(cfg.EngineAPIAddr)
		cc, err := execution_client.NewExecutionClientRPC(ctx, cfg.JwtSecret, cfg.EngineAPIAddr, cfg.EngineAPIPort)
		if err != nil {
			log.Error("could not start engine api", "err", err)
		}
		log.Info("Started Engine API RPC Client", "addr", cfg.EngineAPIAddr)
		executionEngine = cc
	}

	if !cfg.NoBeaconApi {
		apiHandler := handler.NewApiHandler(cfg.GenesisCfg, cfg.BeaconCfg)
		go beacon.ListenAndServe(apiHandler, &beacon.RouterConfiguration{
			Protocol:        cfg.BeaconProtocol,
			Address:         cfg.BeaconAddr,
			ReadTimeTimeout: cfg.BeaconApiReadTimeout,
			WriteTimeout:    cfg.BeaconApiWriteTimeout,
			IdleTimeout:     cfg.BeaconApiWriteTimeout,
		})
		log.Info("Beacon API started", "addr", cfg.BeaconAddr)
	}

	var caplinFreezer freezer.Freezer
	if cfg.RecordMode {
		caplinFreezer = &freezer.RootPathOsFs{
			Root: cfg.RecordDir,
		}
	}

	return caplin1.RunCaplinPhase1(ctx, sentinel, cfg.BeaconCfg, cfg.GenesisCfg, executionEngine, state, caplinFreezer, cfg.DataDir)
}
