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

	"github.com/erigontech/erigon-lib/common/disk"
	"github.com/erigontech/erigon-lib/common/mem"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/sentinel/service"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cmd/sentinel/sentinelcli"
	"github.com/erigontech/erigon/cmd/sentinel/sentinelflags"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/log/v3"

	sentinelapp "github.com/erigontech/erigon/turbo/app"
)

func main() {
	app := sentinelapp.MakeApp("sentinel", runSentinelNode, sentinelflags.CliFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runSentinelNode(cliCtx *cli.Context) error {
	cfg, err := sentinelcli.SetupSentinelCli(cliCtx)
	if err != nil {
		return err
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(cfg.LogLvl), log.StderrHandler))
	log.Info("[Sentinel] running sentinel with configuration", "cfg", cfg)

	// setup periodic logging and prometheus updates
	go mem.LogMemStats(cliCtx.Context, log.Root())
	go disk.UpdateDiskStats(cliCtx.Context, log.Root())

	bs, err := checkpoint_sync.NewRemoteCheckpointSync(cfg.BeaconCfg, cfg.NetworkType).GetLatestBeaconState(cliCtx.Context)
	if err != nil {
		return err
	}
	_, err = service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:         cfg.Addr,
		Port:           int(cfg.Port),
		TCPPort:        cfg.ServerTcpPort,
		NetworkConfig:  cfg.NetworkCfg,
		BeaconConfig:   cfg.BeaconCfg,
		NoDiscovery:    cfg.NoDiscovery,
		LocalDiscovery: cfg.LocalDiscovery,
		EnableBlocks:   false,
	}, nil, nil, nil, &service.ServerConfig{Network: cfg.ServerProtocol, Addr: cfg.ServerAddr}, eth_clock.NewEthereumClock(bs.GenesisTime(), bs.GenesisValidatorsRoot(), cfg.BeaconCfg), nil, log.Root())
	if err != nil {
		log.Error("[Sentinel] Could not start sentinel", "err", err)
		return err
	}
	log.Info("[Sentinel] Sentinel started", "addr", cfg.ServerAddr)

	<-context.Background().Done()
	return nil
}
