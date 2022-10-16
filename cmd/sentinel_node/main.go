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

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
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
	lcCfg := lcCli.SetUpLightClientCfg(cliCtx)
	ctx := context.Background()

	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(lcCfg.LogLvl), log.StderrHandler))

	log.Info("[LC-Sentinel] running sentinel with configuration", "cfg", lcCfg)
	_, err := service.StartSentinelService(&sentinel.SentinelConfig{
		IpAddr:         lcCfg.Addr,
		Port:           int(lcCfg.Port),
		TCPPort:        lcCfg.ServerTcpPort,
		GenesisConfig:  lcCfg.GenesisCfg,
		NetworkConfig:  lcCfg.NetworkCfg,
		BeaconConfig:   lcCfg.BeaconCfg,
		IsDiscoverable: lcCfg.IsDiscoverable,
	}, &service.ServerConfig{Network: lcCfg.ServerProtocol, Addr: lcCfg.ServerAddr})
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
	}

	log.Info("Sentinel started", "addr", lcCfg.ServerAddr)
	<-ctx.Done()
}

/*
func main() {
	ctx := context.Background()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	genesisCfg, networkCfg, beaconCfg := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	sentinelClient, err := service.StartSentinelService(&sentinel.SentinelConfig{
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
	log.Info("Sentinel started")

	logInterval := time.NewTicker(5 * time.Second)
	sendReqInterval := time.NewTicker(500 * time.Millisecond)

	stream, err := sentinelClient.SubscribeGossip(ctx, &lightrpc.EmptyRequest{})
	if err != nil {
		log.Error("Could not start stream", "err", err)
		return
	}

	go gossipHandler(stream)

	for {
		select {
		case <-logInterval.C:
			count, err := sentinelClient.GetPeers(ctx, &lightrpc.EmptyRequest{})
			if err != nil {
				log.Error("Could not get peer count", "err", err)
				return
			}

			log.Info("[Lightclient] Networking Report", "peers", count.Amount)
		case <-sendReqInterval.C:
			go func() {
				var err error
				if _, err = rpc.SendPingReqV1(ctx, &cltypes.Ping{Id: 10}, sentinelClient); err != nil {
					log.Debug("failed to send ping request", "err", err)
				}

				if _, err = rpc.SendMetadataReqV1(ctx, sentinelClient); err != nil {
					log.Debug("failed to send ping request", "err", err)
				}

				var lol *cltypes.LightClientUpdate
				if lol, err = rpc.SendLightClientUpdatesReqV1(ctx, 597, sentinelClient); err != nil {
					log.Warn("failed to send updates by range request", "err", err)
				}
				if lol != nil {
					log.Info("Lightclient responded", "msg", lol)
				}
			}()
		}
	}
}

func gossipHandler(stream lightrpc.Sentinel_SubscribeGossipClient) {
	for {
		gossipData, err := stream.Recv()
		if err != nil {
			log.Warn("ending consensus gossip", "err", err)
			return
		}
		pkt, err := rpc.DecodeGossipData(gossipData)
		if err != nil {
			log.Warn("cannot decode gossip", "err", err)
			return
		}
		switch u := pkt.(type) {
		case *cltypes.SignedBeaconBlockBellatrix:
			log.Debug("[Gossip] beacon_block",
				"Slot", u.Block.Slot,
				"Signature", hex.EncodeToString(u.Signature[:]),
				"graffiti", string(u.Block.Body.Graffiti),
				"eth1_blockhash", hex.EncodeToString(u.Block.Body.Eth1Data.BlockHash[:]),
				"stateRoot", hex.EncodeToString(u.Block.StateRoot[:]),
				"parentRoot", hex.EncodeToString(u.Block.ParentRoot[:]),
				"proposerIdx", u.Block.ProposerIndex,
			)
		case *cltypes.LightClientFinalityUpdate:
			log.Info("[Gossip] Got Finalty Update", "sig", utils.BytesToHex(u.FinalizedHeader.Root[:]))
		case *cltypes.LightClientOptimisticUpdate:
			log.Info("[Gossip] Got Optimistic Update", "sig", utils.BytesToHex(u.SyncAggregate.SyncCommiteeSignature[:]))
		default:
		}
	}
}

*/
