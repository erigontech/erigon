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

package lightclient

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/log/v3"
)

type LightClient struct {
	genesisConfig *clparams.GenesisConfig
	beaconConfig  *clparams.BeaconChainConfig

	sentinel  lightrpc.SentinelClient
	execution remote.ETHBACKENDServer
	store     *LightClientStore
}

func NewLightClient(genesisConfig *clparams.GenesisConfig, beaconConfig *clparams.BeaconChainConfig,
	execution remote.ETHBACKENDServer, sentinel lightrpc.SentinelClient) *LightClient {
	return &LightClient{
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
		sentinel:      sentinel,
		execution:     execution,
	}
}

func (l *LightClient) StartWithNoValidation(ctx context.Context) {
	stream, err := l.sentinel.SubscribeGossip(ctx, &lightrpc.EmptyRequest{})
	if err != nil {
		log.Warn("could not start lightclient", "reason", err)
		return
	}
	defer stream.CloseSend()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			data, err := stream.Recv()
			if err != nil {
				log.Warn("[Lightclient] block could not be ralayed :/", "reason", err)
				continue
			}
			if data.Type != lightrpc.GossipType_BeaconBlockGossipType {
				continue
			}
			block := &cltypes.SignedBeaconBlockBellatrix{}
			if err := block.UnmarshalSSZ(data.Data); err != nil {
				log.Warn("Could not unmarshall gossip", "reason", err)
			}
			if err := l.processBeaconBlock(ctx, block); err != nil {
				log.Warn("[Lightclient] block could not be executed :/", "reason", err)
				continue
			}
		}
	}
}

func (l *LightClient) Start(ctx context.Context) {
	if l.store == nil {
		log.Error("No trusted setup")
		return
	}
	for {
		start := time.Now()
		var (
			updates          = []*cltypes.LightClientUpdate{}
			finalizedPeriod  = utils.SlotToPeriod(l.store.finalizedHeader.Slot)
			optimisticPeriod = utils.SlotToPeriod(l.store.optimisticHeader.Slot)
			currentSlot      = utils.GetCurrentSlot(l.genesisConfig.GenesisTime, l.beaconConfig.SecondsPerSlot)
			currentPeriod    = utils.SlotToPeriod(currentSlot)
		)

		switch {
		// Clause 4 (i):
		// if finalized period == optimistic period and the next sync committee is unknown,
		// fetch the corresponding lightclient update for this cycle
		case finalizedPeriod == optimisticPeriod && l.store.nextSyncCommittee == nil:
			update, err := l.FetchUpdate(ctx, finalizedPeriod)
			if err != nil {
				log.Error("[LightClient] Could not fetch lightclient update", "reason", err)
			} else {
				updates = append(updates, update)
			}
		// Clause 4 (ii):
		// When finalized_period + 1 < current_period, the light client fetches a LightClientUpdate
		// for each sync committee period in range [finalized_period + 1, current_period)
		case finalizedPeriod+1 < currentPeriod:
			for period := finalizedPeriod + 1; period < currentPeriod; period++ {
				update, err := l.FetchUpdate(ctx, period)
				if err != nil {
					log.Error("[LightClient] Could not fetch lightclient update, truncating sync session...",
						"period", period, "reason", err)
					break
				} else {
					updates = append(updates, update)
				}
			}
		}
		// Push updates
		for _, update := range updates {
			err := l.processLightClientUpdate(update)
			if err != nil {
				log.Warn("Could not validate update", "err", err)
			}
		}
		if len(updates) > 0 {
			log.Info("[LightClient] Synced up", "elapsed", time.Since(start))
		}
		// do not have high CPU load
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			return
		}
	}

}
