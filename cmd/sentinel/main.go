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

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	lcCli "github.com/ledgerwatch/erigon/cmd/sentinel/cli"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/service"
	sentinelapp "github.com/ledgerwatch/erigon/turbo/app"
)

func main() {
	app := sentinelapp.MakeApp("sentinel", runSentinelNode, flags.CLDefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runSentinelNode(cliCtx *cli.Context) error {
	cfg, _ := lcCli.SetupConsensusClientCfg(cliCtx)

	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(cfg.LogLvl), log.StderrHandler))
	log.Info("[Sentinel] running sentinel with configuration", "cfg", cfg)
	_, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:         cfg.Addr,
		Port:           int(cfg.Port),
		TCPPort:        cfg.ServerTcpPort,
		GenesisConfig:  cfg.GenesisCfg,
		NetworkConfig:  cfg.NetworkCfg,
		BeaconConfig:   cfg.BeaconCfg,
		NoDiscovery:    cfg.NoDiscovery,
		LocalDiscovery: cfg.LocalDiscovery,
	}, nil, &service.ServerConfig{Network: cfg.ServerProtocol, Addr: cfg.ServerAddr}, nil, nil, log.Root())
	if err != nil {
		log.Error("[Sentinel] Could not start sentinel", "err", err)
		return err
	}
	log.Info("[Sentinel] Sentinel started", "addr", cfg.ServerAddr)

	<-context.Background().Done()
	return nil
}
