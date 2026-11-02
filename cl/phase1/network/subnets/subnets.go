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
	"crypto/sha256"
	"encoding/binary"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	commoncrypto "github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
)

const epochsPerSubnetSubscription = 256

// ComputeSubscribedSubnets implements compute_subscribed_subnets from the phase0 p2p spec.
func ComputeSubscribedSubnets(beaconConfig *clparams.BeaconChainConfig, networkConfig *clparams.NetworkConfig, nodeID enode.ID, epoch uint64) ([]uint64, error) {
	nodeIDInt := uint256.NewInt(0).SetBytes(nodeID.Bytes())
	prefix := new(uint256.Int).Rsh(nodeIDInt, uint(256-networkConfig.AttestationSubnetPrefixBits)).Uint64()
	offset := subnetSubscriptionOffset(nodeID)
	period := (epoch + offset) / epochsPerSubnetSubscription
	var periodBytes [8]byte
	binary.LittleEndian.PutUint64(periodBytes[:], period)
	seed := sha256.Sum256(periodBytes[:])
	permutated, err := shuffling.ComputeShuffledIndex(
		beaconConfig,
		prefix,
		uint64(1)<<networkConfig.AttestationSubnetPrefixBits,
		seed,
		nil,
		commoncrypto.Sha256,
	)
	if err != nil {
		return nil, err
	}

	subnets := make([]uint64, beaconConfig.SubnetsPerNode)
	for i := range beaconConfig.SubnetsPerNode {
		subnets[i] = (permutated + i) % networkConfig.AttestationSubnetCount
	}
	return subnets, nil
}

// SubscribedSubnetsRotationEpoch returns the first epoch after epoch at which ComputeSubscribedSubnets uses a new shuffling seed.
func SubscribedSubnetsRotationEpoch(nodeID enode.ID, epoch uint64) uint64 {
	offset := subnetSubscriptionOffset(nodeID)
	period := (epoch + offset) / epochsPerSubnetSubscription
	return (period+1)*epochsPerSubnetSubscription - offset
}

func subnetSubscriptionOffset(nodeID enode.ID) uint64 {
	return binary.BigEndian.Uint64(nodeID[24:]) % epochsPerSubnetSubscription
}

// ComputeSubnetsForSyncCommittee returns the subnets of a validator in the sync committee that signs for slot
// (compute_subnets_for_sync_committee, with the committee chosen by state.SyncCommitteeForMessageSlot).
func ComputeSubnetsForSyncCommittee(s *state.CachingBeaconState, slot, validatorIndex uint64) (subnets []uint64, err error) {
	cfg := s.BeaconConfig()
	// An unknown validator index is invalid whatever the slot, so check it before the committee.
	targetPublicKey, err := s.ValidatorPublicKey(int(validatorIndex))
	if err != nil {
		return nil, err
	}
	syncCommittee, err := state.SyncCommitteeForMessageSlot(s, slot)
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
