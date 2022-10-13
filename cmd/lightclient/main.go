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

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/lightclient"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/service"
	"github.com/ledgerwatch/log/v3"
)

var (
	defaultIpAddr  = "127.0.0.1" // Localhost
	defaultPort    = 8080
	defaultTcpPort = uint(9000)
)

const DefaultUri = "https://beaconstate.ethstaker.cc/eth/v2/debug/beacon/states/finalized"

func main() {
	ctx := context.Background()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	addr := "localhost:7777"
	genesisCfg, networkCfg, beaconCfg := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	sentinel, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:        defaultIpAddr,
		Port:          defaultPort,
		TCPPort:       defaultTcpPort,
		GenesisConfig: genesisCfg,
		NetworkConfig: networkCfg,
		BeaconConfig:  beaconCfg,
	}, &service.ServerConfig{Network: "tcp", Addr: "localhost:7777"})
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
	}
	log.Info("Sentinel started", "addr", addr)

	bs, err := lightclient.RetrieveBeaconState(ctx, DefaultUri)

	if err != nil {
		log.Error("[Checkpoint Sync] Failed", "reason", err)
		return
	}
	log.Info("Finalized Checkpoint", "Epoch", bs.FinalizedCheckpoint.Epoch)
	lc := lightclient.NewLightClient(nil, sentinel)
	if err := lc.BootstrapCheckpoint(ctx, bs.FinalizedCheckpoint.Root); err != nil {
		log.Error("[Bootstrap] failed to bootstrap", "err", err)
		return
	}

}
