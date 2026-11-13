// Copyright 2026 The Erigon Authors
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
	"time"

	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common/log/v3"
)

// proposalWindowSlots is how many slots before a local validator's proposal the node subscribes to every attestation
// subnet. The node receives single votes only on subnets it subscribes to, and a newly joined subnet needs several
// slots to form its mesh.
const proposalWindowSlots = 16

// RunProposalWindows subscribes to every attestation subnet from proposalWindowSlots before each proposal of a local
// validator until the end of the proposal slot. The ENR keeps advertising the persistent subnets only.
func (c *CommitteeSubscribeMgmt) RunProposalWindows(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(c.beaconConfig.SecondsPerSlot) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.subscribeForUpcomingProposal()
		}
	}
}

func (c *CommitteeSubscribeMgmt) subscribeForUpcomingProposal() {
	currentSlot := c.ethClock.GetCurrentSlot()
	var (
		proposalSlot uint64
		found        bool
	)
	if err := c.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		proposalSlot, found = lastLocalProposal(s.ProposerIndexFromLookahead, c.validatorParams.HasAttachedValidator, currentSlot)
		return nil
	}); err != nil || !found {
		return
	}
	// Topic expiries only grow, so the duty and persistent subnets keep theirs, and windows of close proposals merge.
	expiry := c.ethClock.GetSlotTime(proposalSlot + 1)
	for subnet := range c.netConfig.AttestationSubnetCount {
		if err := c.gossipManager.SubscribeWithExpiry(gossip.TopicNameBeaconAttestation(subnet), expiry); err != nil {
			log.Debug("[CommitteeSubscribeMgmt] failed to subscribe attestation subnet for a local proposal", "subnet", subnet, "proposalSlot", proposalSlot, "err", err)
		}
	}
}

// lastLocalProposal returns the last slot after currentSlot, up to proposalWindowSlots ahead, whose proposer is a
// local validator. Slots beyond the proposer lookahead are skipped.
func lastLocalProposal(proposerAt func(slot uint64) (uint64, bool), isLocal func(validatorIndex uint64) bool, currentSlot uint64) (uint64, bool) {
	for slot := currentSlot + proposalWindowSlots; slot > currentSlot; slot-- {
		if proposer, ok := proposerAt(slot); ok && isLocal(proposer) {
			return slot, true
		}
	}
	return 0, false
}
