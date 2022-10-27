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

	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/handlers"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/service"
	lcCli "github.com/ledgerwatch/erigon/cmd/sentinel_node/cli"
	"github.com/ledgerwatch/erigon/cmd/sentinel_node/cli/flags"
	sentinelapp "github.com/ledgerwatch/erigon/turbo/app"
	"github.com/urfave/cli"

	"github.com/ledgerwatch/log/v3"
)

func main() {
	app := sentinelapp.MakeApp(runSentinelNode, flags.LightClientDefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runSentinelNode(cliCtx *cli.Context) {
	lcCfg, _ := lcCli.SetUpLightClientCfg(cliCtx)
	ctx := context.Background()

	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(lcCfg.LogLvl), log.StderrHandler))
	log.Info("[Sentinel] running sentinel with configuration", "cfg", lcCfg)
	s, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:        lcCfg.Addr,
		Port:          int(lcCfg.Port),
		TCPPort:       lcCfg.ServerTcpPort,
		GenesisConfig: lcCfg.GenesisCfg,
		NetworkConfig: lcCfg.NetworkCfg,
		BeaconConfig:  lcCfg.BeaconCfg,
		NoDiscovery:   lcCfg.NoDiscovery,
	}, &service.ServerConfig{Network: lcCfg.ServerProtocol, Addr: lcCfg.ServerAddr})
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
		return
	}
	log.Info("Sentinel started", "addr", lcCfg.ServerAddr)

	log.Info("Sending test request for LightClientFinalityUpdateV1")
	debugReqResp(ctx, s)
}

func debugGossip(ctx context.Context, s lightrpc.SentinelClient) {
	subscription, err := s.SubscribeGossip(ctx, &lightrpc.EmptyRequest{})
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
		return
	}
	for {
		data, err := subscription.Recv()
		if err != nil {
			return
		}
		if data.Type != lightrpc.GossipType_AggregateAndProofGossipType {
			continue
		}
		block := &cltypes.SignedAggregateAndProof{}
		if err := block.UnmarshalSSZ(data.Data); err != nil {
			log.Error("Error", "err", err)
			continue
		}
		log.Info("Received", "msg", block)
	}
}

// Debug function to recieve raw packets on the req/resp domain.s
func debugReqResp(ctx context.Context, s lightrpc.SentinelClient) {
	message, err := s.SendRequest(ctx, &lightrpc.RequestData{
		Topic: handlers.LightClientFinalityUpdateV1,
	})
	if err != nil {
		log.Error(err.Error())
		return
	}
	if message.Error {
		log.Error("received error", "err", string(message.Data))
		return
	}

	log.Info("Finality update response received: %b", message.Data)
}
