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
	"time"

	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	gossipMgr "github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/common/log/v3"
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
	beaconConfig    *clparams.BeaconChainConfig
	netConfig       *clparams.NetworkConfig
	syncedData      *synced_data.SyncedDataManager
	gossipManager   *gossipMgr.GossipManager
	aggregationPool aggregation.AggregationPool
}

func NewCommitteeSubscribeManagement(
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	syncedData *synced_data.SyncedDataManager,
	gossipManager *gossipMgr.GossipManager,
	aggregationPool aggregation.AggregationPool,
) *CommitteeSubscribeMgmt {
	c := &CommitteeSubscribeMgmt{
		beaconConfig:    beaconConfig,
		netConfig:       netConfig,
		syncedData:      syncedData,
		gossipManager:   gossipManager,
		aggregationPool: aggregationPool,
	}
	return c
}

func (c *CommitteeSubscribeMgmt) AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error {
	var (
		slot   = p.Slot
		cIndex = p.CommitteeIndex
	)
	if p.IsAggregator {
		c.aggregationPool.AddAggregatorDuty(slot, cIndex)
	}

	if c.syncedData.Syncing() {
		return synced_data.ErrNotSynced
	}

	log.Trace("Add attestation subscription", "slot", slot, "committeeIndex", cIndex, "isAggregator", p.IsAggregator, "validatorIndex", p.ValidatorIndex)
	commiteePerSlot := c.syncedData.CommitteeCount(p.Slot / c.beaconConfig.SlotsPerEpoch)
	subnetId := subnets.ComputeSubnetForAttestation(commiteePerSlot, slot, cIndex, c.beaconConfig.SlotsPerEpoch, c.netConfig.AttestationSubnetCount)

	epochDuration := 2 * time.Duration(c.beaconConfig.SlotsPerEpoch) * time.Duration(c.beaconConfig.SecondsPerSlot) * time.Second
	// set sentinel gossip expiration by subnet id
	/*request := sentinelproto.RequestSubscribeExpiry{
		Topic:          gossip.TopicNameBeaconAttestation(subnetId),
		ExpiryUnixSecs: uint64(time.Now().Add(epochDuration).Unix()), // expire after epoch
	}
	if _, err := c.sentinel.SetSubscribeExpiry(ctx, &request); err != nil {
		return err
	}*/
	expiry := time.Now().Add(epochDuration)
	if err := c.gossipManager.SubscribeWithExpiry(gossip.TopicNameBeaconAttestation(subnetId), expiry); err != nil {
		return err
	}
	return nil
}
