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

package subnets

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

// def compute_subnets_for_sync_committee(state: BeaconState, validator_index: ValidatorIndex) -> Set[uint64]:
//     next_slot_epoch = compute_epoch_at_slot(Slot(state.slot + 1))
//     if compute_sync_committee_period(get_current_epoch(state)) == compute_sync_committee_period(next_slot_epoch):
//         sync_committee = state.current_sync_committee
//     else:
//         sync_committee = state.next_sync_committee

// target_pubkey = state.validators[validator_index].pubkey
// sync_committee_indices = [index for index, pubkey in enumerate(sync_committee.pubkeys) if pubkey == target_pubkey]
// return set([
//
//	uint64(index // (SYNC_COMMITTEE_SIZE // SYNC_COMMITTEE_SUBNET_COUNT))
//	for index in sync_committee_indices
//
// ])

// ComputeSubnetsForSyncCommittee is used by the ValidatorClient to determine which subnets a validator should be subscribed to for sync committees.
// the function takes an extra syncCommitteeIndicies parameter to adapt to the Beacon API specs.
func ComputeSubnetsForSyncCommittee(s *state.CachingBeaconState, validatorIndex uint64) (subnets []uint64, err error) {
	cfg := s.BeaconConfig()
	var syncCommittee *solid.SyncCommittee
	if cfg.SyncCommitteePeriod(s.Slot()) == cfg.SyncCommitteePeriod(s.Slot()+1) {
		syncCommittee = s.CurrentSyncCommittee()
	} else {
		syncCommittee = s.NextSyncCommittee()
	}

	targetPublicKey, err := s.ValidatorPublicKey(int(validatorIndex))
	if err != nil {
		return nil, err
	}

	// make sure we return each subnet id, exactly once.
	alreadySeenSubnetIndex := make(map[uint64]struct{})

	committee := syncCommittee.GetCommittee()
	for index := uint64(0); index < uint64(len(committee)); index++ {
		subnetIdx := index / (cfg.SyncCommitteeSize / cfg.SyncCommitteeSubnetCount)
		if _, ok := alreadySeenSubnetIndex[subnetIdx]; ok {
			continue
		}
		if targetPublicKey == committee[index] {
			subnets = append(subnets, subnetIdx)
			alreadySeenSubnetIndex[subnetIdx] = struct{}{}
		}
	}
	return subnets, nil
}

func ComputeSubnetForAttestation(committeePerSlot, slot, committeeIndex, slotsPerEpoch, attSubnetCount uint64) uint64 {
	// ref: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/validator.md#broadcast-attestation
	// slots_since_epoch_start = uint64(slot % SLOTS_PER_EPOCH)
	// committees_since_epoch_start = committees_per_slot * slots_since_epoch_start
	// return SubnetID((committees_since_epoch_start + committee_index) % ATTESTATION_SUBNET_COUNT)
	slotsSinceEpochStart := slot % slotsPerEpoch
	committeesSinceEpochStart := committeePerSlot * slotsSinceEpochStart
	return (committeesSinceEpochStart + committeeIndex) % attSubnetCount
}

func ComputeCommitteeCountPerSlot(s abstract.BeaconStateReader, slot uint64, slotsPerEpoch uint64) uint64 {
	epoch := slot / slotsPerEpoch
	return s.CommitteeCount(epoch)
}
