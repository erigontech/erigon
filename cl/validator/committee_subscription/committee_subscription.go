// Copyright 2024 The Erigon Authors
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

package committee_subscription

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

var (
	ErrIgnore                   = errors.New("ignore")
	ErrCommitteeIndexOutOfRange = errors.New("committee index out of range")
	ErrWrongSubnet              = errors.New("attestation is for the wrong subnet")
	ErrNotInPropagationRange    = fmt.Errorf("attestation is not in propagation range. %w", ErrIgnore)
	ErrEpochMismatch            = errors.New("epoch mismatch")
	ErrExactlyOneBitSet         = errors.New("exactly one aggregation bit should be set")
	ErrAggregationBitsMismatch  = errors.New("aggregation bits mismatch committee size")
)

type CommitteeSubscribeMgmt struct {
	indiciesDB   kv.RoDB
	ethClock     eth_clock.EthereumClock
	beaconConfig *clparams.BeaconChainConfig
	netConfig    *clparams.NetworkConfig
	sentinel     sentinel.SentinelClient
	state        *state.CachingBeaconState
	syncedData   *synced_data.SyncedDataManager
	// subscriptions
	aggregationPool     aggregation.AggregationPool
	validatorSubsMutex  sync.RWMutex
	subnetSubscriptions map[uint64]*subscription // subnet -> subscription status
}

func NewCommitteeSubscribeManagement(
	ctx context.Context,
	indiciesDB kv.RoDB,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
	sentinel sentinel.SentinelClient,
	state *state.CachingBeaconState,
	aggregationPool aggregation.AggregationPool,
	syncedData *synced_data.SyncedDataManager,
) *CommitteeSubscribeMgmt {
	c := &CommitteeSubscribeMgmt{
		indiciesDB:          indiciesDB,
		beaconConfig:        beaconConfig,
		netConfig:           netConfig,
		ethClock:            ethClock,
		sentinel:            sentinel,
		state:               state,
		aggregationPool:     aggregationPool,
		syncedData:          syncedData,
		subnetSubscriptions: make(map[uint64]*subscription),
	}
	go c.sweepByStaleSlots(ctx)
	return c
}

type subscription struct {
	aggregate        bool
	latestTargetSlot uint64
}

func (c *CommitteeSubscribeMgmt) AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error {
	var (
		slot   = p.Slot
		cIndex = p.CommitteeIndex
	)
	headState := c.syncedData.HeadState()
	if headState == nil {
		return errors.New("head state not available")
	}

	commiteePerSlot := headState.CommitteeCount(slot / c.beaconConfig.SlotsPerEpoch)
	subnetId := subnets.ComputeSubnetForAttestation(commiteePerSlot, slot, cIndex, c.beaconConfig.SlotsPerEpoch, c.netConfig.AttestationSubnetCount)
	log.Debug("Add attestation subscription", "slot", slot, "committeeIndex", cIndex, "isAggregator", p.IsAggregator, "validatorIndex", p.ValidatorIndex, "subnetId", subnetId)
	// add validator to subscription
	c.validatorSubsMutex.Lock()

	if _, ok := c.subnetSubscriptions[subnetId]; !ok {
		c.subnetSubscriptions[subnetId] = &subscription{
			//aggregate:        p.IsAggregator,
			aggregate:        false,
			latestTargetSlot: slot,
		}
	} else {
		// set aggregator to true if any validator in the committee is an aggregator
		//c.subnetSubscriptions[subnetId].aggregate = (c.subnetSubscriptions[subnetId].aggregate || p.IsAggregator)
		// update latest target slot
		if c.subnetSubscriptions[subnetId].latestTargetSlot < slot {
			c.subnetSubscriptions[subnetId].latestTargetSlot = slot
		}
	}

	c.validatorSubsMutex.Unlock()

	// set sentinel gossip expiration by subnet id
	request := sentinel.RequestSubscribeExpiry{
		Topic:          gossip.TopicNameBeaconAttestation(subnetId),
		ExpiryUnixSecs: uint64(time.Now().Add(30 * time.Minute).Unix()), // temporarily set to 30 minutes
	}
	if _, err := c.sentinel.SetSubscribeExpiry(ctx, &request); err != nil {
		return err
	}
	return nil
}

func (c *CommitteeSubscribeMgmt) CheckAggregateAttestation(att *solid.Attestation) error {
	headState := c.syncedData.HeadState()
	if headState == nil {
		return errors.New("head state not available")
	}
	cIndex := att.AttestantionData().CommitteeIndex()
	slot := att.AttestantionData().Slot()
	commiteePerSlot := headState.CommitteeCount(slot / c.beaconConfig.SlotsPerEpoch)
	subnetId := subnets.ComputeSubnetForAttestation(commiteePerSlot, slot, cIndex, c.beaconConfig.SlotsPerEpoch, c.netConfig.AttestationSubnetCount)

	// check if need to aggregate attestation in this subnet
	c.validatorSubsMutex.RLock()
	defer c.validatorSubsMutex.RUnlock()
	if sub, ok := c.subnetSubscriptions[subnetId]; ok && sub.aggregate {
		// aggregate attestation
		if err := c.aggregationPool.AddAttestation(att); err != nil {
			return err
		}
	}
	return nil
}

func (c *CommitteeSubscribeMgmt) sweepByStaleSlots(ctx context.Context) {
	slotIsStale := func(curSlot, targetSlot uint64) bool {
		if curSlot <= targetSlot {
			// Avoid subtracting unsigned integers
			return false
		}
		return curSlot-targetSlot > c.netConfig.AttestationPropagationSlotRange
	}
	// sweep every minute
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var (
				curSlot  = c.ethClock.GetCurrentSlot()
				toRemove = make([]uint64, 0)
			)
			c.validatorSubsMutex.Lock()
			for subnetId, sub := range c.subnetSubscriptions {
				if slotIsStale(curSlot, sub.latestTargetSlot) {
					toRemove = append(toRemove, subnetId)
				}
			}
			for _, subnetId := range toRemove {
				delete(c.subnetSubscriptions, subnetId)
			}
			c.validatorSubsMutex.Unlock()
		}
	}
}
