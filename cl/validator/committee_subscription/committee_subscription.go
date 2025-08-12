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
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/db/kv"
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
	aggregationPool    aggregation.AggregationPool
	validatorSubsMutex sync.RWMutex
	validatorSubs      map[uint64]*validatorSub // committeeIndex -> validatorSub
}

func NewCommitteeSubscribeManagement(
	ctx context.Context,
	indiciesDB kv.RoDB,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
	sentinel sentinel.SentinelClient,
	aggregationPool aggregation.AggregationPool,
	syncedData *synced_data.SyncedDataManager,
) *CommitteeSubscribeMgmt {
	c := &CommitteeSubscribeMgmt{
		indiciesDB:      indiciesDB,
		beaconConfig:    beaconConfig,
		netConfig:       netConfig,
		ethClock:        ethClock,
		sentinel:        sentinel,
		aggregationPool: aggregationPool,
		syncedData:      syncedData,
		validatorSubs:   make(map[uint64]*validatorSub),
	}
	go c.sweepByStaleSlots(ctx)
	return c
}

type validatorSub struct {
	aggregate         bool
	largestTargetSlot uint64
}

func (c *CommitteeSubscribeMgmt) AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error {
	var (
		slot   = p.Slot
		cIndex = p.CommitteeIndex
	)

	if c.syncedData.Syncing() {
		return errors.New("head state not available")
	}

	log.Trace("Add attestation subscription", "slot", slot, "committeeIndex", cIndex, "isAggregator", p.IsAggregator, "validatorIndex", p.ValidatorIndex)
	commiteePerSlot := c.syncedData.CommitteeCount(p.Slot / c.beaconConfig.SlotsPerEpoch)
	subnetId := subnets.ComputeSubnetForAttestation(commiteePerSlot, slot, cIndex, c.beaconConfig.SlotsPerEpoch, c.netConfig.AttestationSubnetCount)
	// add validator to subscription
	c.validatorSubsMutex.Lock()

	if _, ok := c.validatorSubs[cIndex]; !ok {
		c.validatorSubs[cIndex] = &validatorSub{
			aggregate:         p.IsAggregator,
			largestTargetSlot: slot,
		}
	} else {
		// set aggregator to true if any validator in the committee is an aggregator
		c.validatorSubs[cIndex].aggregate = (c.validatorSubs[cIndex].aggregate || p.IsAggregator)
		// update latest target slot
		if c.validatorSubs[cIndex].largestTargetSlot < slot {
			c.validatorSubs[cIndex].largestTargetSlot = slot
		}
	}
	c.validatorSubsMutex.Unlock()

	epochDuration := time.Duration(c.beaconConfig.SlotsPerEpoch) * time.Duration(c.beaconConfig.SecondsPerSlot) * time.Second
	// set sentinel gossip expiration by subnet id
	request := sentinel.RequestSubscribeExpiry{
		Topic:          gossip.TopicNameBeaconAttestation(subnetId),
		ExpiryUnixSecs: uint64(time.Now().Add(epochDuration).Unix()), // expire after epoch
	}
	if _, err := c.sentinel.SetSubscribeExpiry(ctx, &request); err != nil {
		return err
	}
	return nil
}

func (c *CommitteeSubscribeMgmt) AggregateAttestation(att *solid.Attestation) error {
	var (
		committeeIndex = att.Data.CommitteeIndex
		slot           = att.Data.Slot
		clVersion      = c.beaconConfig.GetCurrentStateVersion(slot / c.beaconConfig.SlotsPerEpoch)
	)
	if clVersion.AfterOrEqual(clparams.ElectraVersion) {
		index, err := att.GetCommitteeIndexFromBits()
		if err != nil {
			return err
		}
		committeeIndex = index
	}

	c.validatorSubsMutex.RLock()
	defer c.validatorSubsMutex.RUnlock()
	if sub, ok := c.validatorSubs[committeeIndex]; ok && sub.aggregate {
		// aggregate attestation
		if err := c.aggregationPool.AddAttestation(att); err != nil {
			return err
		}
	}
	return nil
}

func (c *CommitteeSubscribeMgmt) NeedToAggregate(att *solid.Attestation) bool {
	var (
		committeeIndex = att.Data.CommitteeIndex
		slot           = att.Data.Slot
	)
	clVersion := c.beaconConfig.GetCurrentStateVersion(slot / c.beaconConfig.SlotsPerEpoch)
	if clVersion.AfterOrEqual(clparams.ElectraVersion) {
		index, err := att.GetCommitteeIndexFromBits()
		if err != nil {
			return false
		}
		committeeIndex = index
	}

	c.validatorSubsMutex.RLock()
	defer c.validatorSubsMutex.RUnlock()
	if sub, ok := c.validatorSubs[committeeIndex]; ok && sub.aggregate {
		root, err := att.Data.HashSSZ()
		if err != nil {
			log.Warn("failed to hash attestation data", "err", err)
			return false
		}
		aggregation := c.aggregationPool.GetAggregatationByRoot(root)
		if aggregation == nil ||
			!utils.IsNonStrictSupersetBitlist(aggregation.AggregationBits.Bytes(), att.AggregationBits.Bytes()) {
			// the on bit is not set. need to aggregate
			return true
		}
	}
	return false
}

func (c *CommitteeSubscribeMgmt) sweepByStaleSlots(ctx context.Context) {
	slotIsStale := func(curSlot, targetSlot uint64) bool {
		if curSlot <= targetSlot {
			// Avoid subtracting unsigned integers
			return false
		}
		return curSlot-targetSlot > c.netConfig.AttestationPropagationSlotRange
	}
	// sweep every 3 seconds
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curSlot := c.ethClock.GetCurrentSlot()
			toRemoves := make([]uint64, 0)
			c.validatorSubsMutex.Lock()
			for committeeIdx, sub := range c.validatorSubs {
				if slotIsStale(curSlot, sub.largestTargetSlot) {
					toRemoves = append(toRemoves, committeeIdx)
				}
				// try remove aggregator flag to avoid unnecessary aggregation
				if curSlot > sub.largestTargetSlot {
					sub.aggregate = false
				}
			}
			for _, idx := range toRemoves {
				delete(c.validatorSubs, idx)
			}
			c.validatorSubsMutex.Unlock()
		}
	}
}
