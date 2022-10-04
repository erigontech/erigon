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
	"encoding/hex"
	"fmt"
	"reflect"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/log/v3"
)

var (
	defaultIpAddr  = "127.0.0.1" // Localhost
	defaultPort    = 8080
	defaultTcpPort = uint(9000)
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	discCfg, genesisCfg, networkCfg, beaconCfg, err := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	if err != nil {
		log.Error("error", "err", err)
		return
	}
	sent, err := sentinel.New(context.Background(), &sentinel.SentinelConfig{
		IpAddr:         defaultIpAddr,
		Port:           defaultPort,
		TCPPort:        defaultTcpPort,
		DiscoverConfig: *discCfg,
		GenesisConfig:  genesisCfg,
		NetworkConfig:  networkCfg,
		BeaconConfig:   beaconCfg,
	})
	if err != nil {
		log.Error("error", "err", err)
		return
	}
	if err := sent.Start(); err != nil {
		log.Error("failed to start sentinel", "err", err)
		return
	}
	log.Info("Sentinel started", "enr", sent.String())

	gossip_topics := []sentinel.GossipTopic{
		sentinel.BeaconBlockSsz,
		sentinel.LightClientFinalityUpdateSsz,
		sentinel.LightClientOptimisticUpdateSsz,
	}
	for _, v := range gossip_topics {
		// now lets separately connect to the gossip topics. this joins the room
		subscriber, err := sent.SubscribeGossip(v)
		if err != nil {
			log.Error("failed to start sentinel", "err", err)
		}
		// actually start the subscription, ala listening and sending packets to the sentinel recv channel
		err = subscriber.Listen()
		if err != nil {
			log.Error("failed to start sentinel", "err", err)
		}
	}

	logInterval := time.NewTicker(5 * time.Second)
	sendReqInterval := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-logInterval.C:
			sent.LogTopicPeers()
			log.Info("[Lightclient] Networking Report", "peers", sent.GetPeersCount())
		case pkt := <-sent.RecvGossip():
			handleGossipPacket(pkt)
		case <-sendReqInterval.C:
			go func() {
				if _, err := sent.SendPingReqV1(); err != nil {
					log.Debug("failed to send ping request", "err", err)
				}
				if _, err := sent.SendMetadataReqV1(); err != nil {
					log.Debug("failed to send metadata request", "err", err)
				}
			}()
		}
	}
}

func handleGossipPacket(pkt *proto.GossipContext) error {
	log.Info("[Gossip] Received Packet", "topic", pkt.Topic)
	fmt.Println(reflect.TypeOf(pkt.Packet))
	switch u := pkt.Packet.(type) {
	case *lightrpc.SignedBeaconBlockBellatrix:
		log.Info("[Gossip] beacon_block",
			"Slot", u.Block.Slot,
			"Signature", hex.EncodeToString(u.Signature),
			"graffiti", string(u.Block.Body.Graffiti),
			"eth1_blockhash", hex.EncodeToString(u.Block.Body.Eth1Data.BlockHash),
			"stateRoot", hex.EncodeToString(u.Block.StateRoot),
			"parentRoot", hex.EncodeToString(u.Block.ParentRoot),
			"proposerIdx", u.Block.ProposerIndex,
		)
		err := pkt.Codec.WritePacket(context.TODO(), pkt.Packet)
		if err != nil {
			log.Warn("[Gossip] Error Forwarding Packet", "err", err)
		}
	case *lightrpc.LightClientFinalityUpdate:
	case *lightrpc.LightClientOptimisticUpdate:
	default:
	}
	return nil
}
