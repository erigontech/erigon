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
	"os"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap/buffer"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	lcCli "github.com/ledgerwatch/erigon/cmd/sentinel/cli"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/handlers"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/service"
	"github.com/ledgerwatch/erigon/common"
	sentinelapp "github.com/ledgerwatch/erigon/turbo/app"
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

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func constructBodyFreeRequest(t string) *consensusrpc.RequestData {
	return &consensusrpc.RequestData{
		Topic: t,
	}
}

func constructRequest(t string, reqBody cltypes.ObjectSSZ) (*consensusrpc.RequestData, error) {
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, reqBody); err != nil {
		return nil, fmt.Errorf("unable to encode request body: %v", err)
	}

	data := common.CopyBytes(buffer.Bytes())
	return &consensusrpc.RequestData{
		Data:  data,
		Topic: t,
	}, nil
}

func runSentinelNode(cliCtx *cli.Context) error {
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
	}, nil, &service.ServerConfig{Network: lcCfg.ServerProtocol, Addr: lcCfg.ServerAddr}, nil)
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
		return err
	}
	log.Info("Sentinel started", "addr", lcCfg.ServerAddr)

	digest, err := fork.ComputeForkDigest(lcCfg.BeaconCfg, lcCfg.GenesisCfg)
	if err != nil {
		log.Error("Could not compute fork digeest", "err", err)
		return err
	}
	log.Info("Fork digest", "data", digest)

	log.Info("Sending test request")
	/*
		sendRequest(ctx, s, constructBodyFreeRequest(handlers.LightClientFinalityUpdateV1))
		sendRequest(ctx, s, constructBodyFreeRequest(handlers.MetadataProtocolV1))
		sendRequest(ctx, s, constructBodyFreeRequest(handlers.MetadataProtocolV2))
		sendRequest(ctx, s, constructBodyFreeRequest(handlers.LightClientOptimisticUpdateV1))

		lcUpdateReq := &cltypes.LightClientUpdatesByRangeRequest{
			Period: 604,
			Count:  1,
		}

		blocksByRangeReq := &cltypes.BeaconBlocksByRangeRequest{
			StartSlot: 5000005, // arbitrary slot (currently at ~5030000)
			Count:     3,
			Step:      1, // deprecated, must be set to 1.
		}

		roots := make([][32]byte, 3)
		rawRoot1, err := hex.DecodeString("57dd8e0ee7ed614283fbf80ca19229752839d4a4e232148efd128e85edee9b12")
		check(err)
		rawRoot2, err := hex.DecodeString("1d527f21e17897198752838431821558d1b9864654bcaf476232da55458ed5ce")
		check(err)
		rawRoot3, err := hex.DecodeString("830eab2e3de70cc7ebc80b6c950ebb2a7946d8a6e3bb653f8b6dafd6a402d49b")
		check(err)

		copy(roots[0][:], rawRoot1)
		copy(roots[1][:], rawRoot2)
		copy(roots[2][:], rawRoot3)

		var blocksByRootReq cltypes.BeaconBlocksByRootRequest = roots
	*/

	// Getting fork digest into bytes array.
	forkDigest := "4a26c58b"
	fdSlice, err := hex.DecodeString(forkDigest)
	check(err)
	var fdArr [4]byte
	copy(fdArr[:], fdSlice)

	// Getting CP block root into bytes array.
	cpRootRaw := "3657dbacf691e5c3787bf6b824bdd83bec8c3446797635370bdd0ed4b7c5b760"
	cpRootSlice, err := hex.DecodeString(cpRootRaw)
	check(err)
	var cpRootArr [32]byte
	copy(cpRootArr[:], cpRootSlice)

	// USING: https://beaconcha.in/slot/5101760 as the checkpoint & current block.
	statusReq := &cltypes.Status{
		ForkDigest:     fdArr,
		FinalizedRoot:  cpRootArr,
		FinalizedEpoch: 159430,
		HeadRoot:       cpRootArr,
		HeadSlot:       5101760,
	}
	req, err := constructRequest(handlers.StatusProtocolV1, statusReq)
	if err != nil {
		log.Error("could not construct request", "err", err)
		return err
	}
	sendRequest(ctx, s, req)
	return nil
}

func debugGossip(ctx context.Context, s consensusrpc.SentinelClient) {
	subscription, err := s.SubscribeGossip(ctx, &consensusrpc.EmptyRequest{})
	if err != nil {
		log.Error("Could not start sentinel", "err", err)
		return
	}
	for {
		data, err := subscription.Recv()
		if err != nil {
			return
		}
		if data.Type != consensusrpc.GossipType_AggregateAndProofGossipType {
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

// Debug function to recieve test packets on the req/resp domain.
func sendRequest(ctx context.Context, s consensusrpc.SentinelClient, req *consensusrpc.RequestData) {
	newReqTicker := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
		case <-newReqTicker.C:
			go func() {
				message, err := s.SendRequest(ctx, req)
				if err != nil {
					return
				}
				if message.Error {
					log.Error("received error", "err", string(message.Data))
					return
				}

				log.Info("Non-error response received", "data", message.Data)
				f, err := os.Create("out")
				if err != nil {
					panic(fmt.Sprintf("unable to open file: %v\n", err))
				}
				_, err = f.WriteString(hex.EncodeToString(message.Data))
				if err != nil {
					panic(fmt.Sprintf("unable to write to file: %v\n", err))
				}
				log.Info("Hex representation", "data", hex.EncodeToString(message.Data))
				f.Close()
			}()
		}
	}
}
