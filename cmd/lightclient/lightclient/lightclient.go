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
	"bytes"
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	"github.com/ledgerwatch/erigon/cl/utils"
	cldb "github.com/ledgerwatch/erigon/cmd/erigon-cl/cl-core/cl-db"
	"github.com/ledgerwatch/log/v3"
)

const maxRecentHashes = 5 // 0.16 KB
const safetyRange = 8     // 8 block of safety

type LightClient struct {
	ctx           context.Context
	genesisConfig *clparams.GenesisConfig
	beaconConfig  *clparams.BeaconChainConfig
	chainTip      *ChainTipSubscriber

	verbose           bool
	highestSeen       uint64 // Highest ETH1 block seen
	recentHashesCache *lru.Cache
	db                kv.RwDB
	sentinel          consensusrpc.SentinelClient
	execution         remote.ETHBACKENDServer
	store             *LightClientStore
}

func NewLightClient(ctx context.Context, db kv.RwDB, genesisConfig *clparams.GenesisConfig, beaconConfig *clparams.BeaconChainConfig,
	execution remote.ETHBACKENDServer, sentinel consensusrpc.SentinelClient,
	highestSeen uint64, verbose bool) (*LightClient, error) {
	recentHashesCache, err := lru.New(maxRecentHashes)
	return &LightClient{
		ctx:               ctx,
		beaconConfig:      beaconConfig,
		genesisConfig:     genesisConfig,
		chainTip:          NewChainTipSubscriber(ctx, sentinel),
		recentHashesCache: recentHashesCache,
		sentinel:          sentinel,
		execution:         execution,
		verbose:           verbose,
		highestSeen:       highestSeen,
		db:                db,
	}, err
}

func (l *LightClient) Start() {
	if l.store == nil {
		log.Error("No trusted setup")
		return
	}
	tx, err := l.db.BeginRw(l.ctx)
	if err != nil {
		log.Error("Could not open MDBX transaction", "err", err)
		return
	}
	defer tx.Rollback()
	logPeers := time.NewTicker(time.Minute)
	go l.chainTip.StartLoop()
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
			update, err := l.FetchUpdate(l.ctx, finalizedPeriod)
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
				update, err := l.FetchUpdate(l.ctx, period)
				if err != nil {
					log.Error("[LightClient] Could not fetch lightclient update, truncating sync session...",
						"period", period, "reason", err)
					break
				} else {
					updates = append(updates, update)
				}
			}
		// Clause 4 (iii):
		// When finalized_period + 1 >= current_period, the light client keeps observing LightClientFinalityUpdate and LightClientOptimisticUpdate.
		// Received objects are passed to process_light_client_update. This ensures that finalized_header and
		// optimistic_header reflect the latest blocks.
		case finalizedPeriod+1 >= currentPeriod:
			newUpdate := l.chainTip.PopLastUpdate()
			if newUpdate != nil {
				updates = append(updates, newUpdate)
			}
		}

		// Push updates
		for _, update := range updates {
			err := l.processLightClientUpdate(update)
			if err != nil {
				log.Warn("Could not validate update", "err", err)
				updates = []*cltypes.LightClientUpdate{}
				break
			}
		}
		// log new validated segment
		if len(updates) > 0 {
			lastValidated := updates[len(updates)-1]
			// Save to Database
			if lastValidated.HasNextSyncCommittee() {
				if err := cldb.WriteLightClientUpdate(tx, lastValidated); err != nil {
					log.Warn("Could not write lightclient update to db", "err", err)
				}
			}
			if lastValidated.IsFinalityUpdate() {
				if err := cldb.WriteLightClientFinalityUpdate(tx, &cltypes.LightClientFinalityUpdate{
					AttestedHeader:  lastValidated.AttestedHeader,
					FinalizedHeader: lastValidated.FinalizedHeader,
					FinalityBranch:  lastValidated.FinalityBranch,
					SyncAggregate:   lastValidated.SyncAggregate,
					SignatureSlot:   lastValidated.SignatureSlot,
				}); err != nil {
					log.Warn("Could not write finality lightclient update to db", "err", err)
				}
			}
			if err := cldb.WriteLightClientOptimisticUpdate(tx, &cltypes.LightClientOptimisticUpdate{
				AttestedHeader: lastValidated.AttestedHeader,
				SyncAggregate:  lastValidated.SyncAggregate,
				SignatureSlot:  lastValidated.SignatureSlot,
			}); err != nil {
				log.Warn("Could not write optimistic lightclient update to db", "err", err)
			}

			if err := tx.Commit(); err != nil {
				log.Error("[LightClient] could not commit to database", "err", err)
				return
			}
			tx, err = l.db.BeginRw(l.ctx)
			if err != nil {
				log.Error("[LightClient] could not begin database transaction", "err", err)
				return
			}
			defer tx.Rollback()

			if l.verbose {
				log.Info("[LightClient] Validated Chain Segments",
					"elapsed", time.Since(start), "from", updates[0].AttestedHeader.Slot-1,
					"to", lastValidated.AttestedHeader.Slot)
			}
			prev, curr := l.chainTip.GetLastBlocks()
			if prev == nil {
				continue
			}
			// Skip if we went out of sync and weird network stuff happen
			if prev.Slot != lastValidated.AttestedHeader.Slot {
				continue
			}
			// Validate update against block N-1
			prevRoot, err := prev.Body.HashTreeRoot()
			if err != nil {
				log.Warn("[LightClient] Could not retrive body root of block N-1", "err", err)
				continue
			}
			if !bytes.Equal(prevRoot[:], lastValidated.AttestedHeader.BodyRoot[:]) {
				log.Warn("[LightClient] Could validate block N-1")
				continue
			}
			// Check if N.hash == (N-1).hash for ETH1, we really dont care about ETH2 validity at this point
			if !bytes.Equal(prev.Body.ExecutionPayload.BlockHash[:],
				curr.Body.ExecutionPayload.ParentHash[:]) {
				log.Warn("[LightClient] Wrong ETH1 hashes")
				continue
			}
			eth1Number := curr.Body.ExecutionPayload.BlockNumber
			if l.highestSeen > safetyRange && eth1Number < l.highestSeen-safetyRange {
				continue
			}
			// If all of the above is gud then do the push
			if err := l.processBeaconBlock(curr); err != nil {
				log.Warn("Could not send beacon block to ETH1", "err", err)
			} else {
				l.highestSeen = eth1Number
			}
		}
		// do not have high CPU load
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-timer.C:
		case <-logPeers.C:
			peers, err := l.sentinel.GetPeers(l.ctx, &consensusrpc.EmptyRequest{})
			if err != nil {
				log.Warn("could not read peers", "err", err)
				continue
			}
			log.Info("[LightClient] P2P", "peers", peers.Amount)
		case <-l.ctx.Done():
			return
		}
	}

}
