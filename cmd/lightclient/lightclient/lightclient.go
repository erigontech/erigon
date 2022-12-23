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
	"runtime"
	"time"

	lru "github.com/hashicorp/golang-lru"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
)

const (
	maxRecentHashes   = 5  // 0.16 KB
	safetyRange       = 16 // 16 block of safety
	maxChainExtension = 8  // 8 blocks of chain extension
)

type LightClient struct {
	ctx           context.Context
	genesisConfig *clparams.GenesisConfig
	beaconConfig  *clparams.BeaconChainConfig
	chainTip      *ChainTipSubscriber

	verbose              bool
	highestSeen          uint64      // Highest ETH1 block seen.
	highestValidated     uint64      // Highest ETH2 slot validated.
	highestProcessedRoot common.Hash // Highest processed ETH2 block root.
	lastEth2ParentRoot   common.Hash // Last ETH2 Parent root.
	recentHashesCache    *lru.Cache
	db                   kv.RwDB
	sentinel             sentinel.SentinelClient
	execution            remote.ETHBACKENDServer
	store                *LightClientStore
}

func NewLightClient(ctx context.Context, db kv.RwDB, genesisConfig *clparams.GenesisConfig, beaconConfig *clparams.BeaconChainConfig,
	execution remote.ETHBACKENDServer, sentinel sentinel.SentinelClient,
	highestSeen uint64, verbose bool) (*LightClient, error) {
	recentHashesCache, err := lru.New(maxRecentHashes)
	return &LightClient{
		ctx:               ctx,
		beaconConfig:      beaconConfig,
		genesisConfig:     genesisConfig,
		chainTip:          NewChainTipSubscriber(ctx, beaconConfig, genesisConfig, sentinel),
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

	updateStatusSentinel := time.NewTicker(2 * time.Minute)
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
			l.highestValidated = lastValidated.AttestedHeader.Slot
			l.highestProcessedRoot, err = lastValidated.AttestedHeader.HashTreeRoot()
			if err != nil {
				log.Warn("could not compute root", "err", err)
				continue
			}
			// Save to Database
			if lastValidated.HasNextSyncCommittee() {
				if err := rawdb.WriteLightClientUpdate(tx, lastValidated); err != nil {
					log.Warn("Could not write lightclient update to db", "err", err)
				}
			}
			if lastValidated.IsFinalityUpdate() {
				if err := rawdb.WriteLightClientFinalityUpdate(tx, &cltypes.LightClientFinalityUpdate{
					AttestedHeader:  lastValidated.AttestedHeader,
					FinalizedHeader: lastValidated.FinalizedHeader,
					FinalityBranch:  lastValidated.FinalityBranch,
					SyncAggregate:   lastValidated.SyncAggregate,
					SignatureSlot:   lastValidated.SignatureSlot,
				}); err != nil {
					log.Warn("Could not write finality lightclient update to db", "err", err)
				}
			}
			if err := rawdb.WriteLightClientOptimisticUpdate(tx, &cltypes.LightClientOptimisticUpdate{
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
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				log.Info("[LightClient] Validated Chain Segments",
					"elapsed", time.Since(start), "from", updates[0].AttestedHeader.Slot-1,
					"to", lastValidated.AttestedHeader.Slot, "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
			}
		}
		l.importBlockIfPossible()
		// do not have high CPU load
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-timer.C:
		case <-logPeers.C:
			peers, err := l.sentinel.GetPeers(l.ctx, &sentinel.EmptyMessage{})
			if err != nil {
				log.Warn("could not read peers", "err", err)
				continue
			}
			log.Info("[LightClient] P2P", "peers", peers.Amount)
		case <-updateStatusSentinel.C:
			if err := l.updateStatus(); err != nil {
				log.Error("Could not update sentinel status", "err", err)
				return
			}
		case <-l.ctx.Done():
			return
		}
	}
}

func (l *LightClient) importBlockIfPossible() {
	var err error
	curr := l.chainTip.GetLastBlock()
	if curr == nil {
		return
	}
	// Skip if we are too far ahead without validating
	if curr.Slot > l.highestValidated+maxChainExtension {
		return
	}
	currentRoot, err := curr.HashTreeRoot()
	if err != nil {
		log.Warn("Could not send beacon block to ETH1", "err", err)
		return
	}

	if l.lastEth2ParentRoot != l.highestProcessedRoot && l.highestProcessedRoot != curr.ParentRoot {
		l.lastEth2ParentRoot = curr.ParentRoot
		return
	}
	l.lastEth2ParentRoot = curr.ParentRoot
	l.highestProcessedRoot = currentRoot

	eth1Number := curr.Body.ExecutionPayload.BlockNumber
	if l.highestSeen != 0 && (l.highestSeen > safetyRange && eth1Number < l.highestSeen-safetyRange) {
		return
	}
	if l.verbose {
		log.Info("Processed block", "slot", curr.Body.ExecutionPayload.BlockNumber)
	}
	// If all of the above is gud then do the push
	if err := l.processBeaconBlock(curr); err != nil {
		log.Warn("Could not send beacon block to ETH1", "err", err)
	} else {
		l.highestSeen = eth1Number
	}
}

func (l *LightClient) updateStatus() error {
	forkDigest, err := fork.ComputeForkDigest(l.beaconConfig, l.genesisConfig)
	if err != nil {
		return err
	}
	finalizedRoot, err := l.store.finalizedHeader.HashTreeRoot()
	if err != nil {
		return err
	}
	headRoot, err := l.store.optimisticHeader.HashTreeRoot()
	if err != nil {
		return err
	}
	_, err = l.sentinel.SetStatus(l.ctx, &sentinel.Status{
		ForkDigest:     utils.Bytes4ToUint32(forkDigest),
		FinalizedRoot:  gointerfaces.ConvertHashToH256(finalizedRoot),
		FinalizedEpoch: l.store.finalizedHeader.Slot / 32,
		HeadRoot:       gointerfaces.ConvertHashToH256(headRoot),
		HeadSlot:       l.store.optimisticHeader.Slot,
	})
	return err
}
