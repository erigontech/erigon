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

package forkchoice

import (
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/monitor/shuffling_metrics"
	corestate "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
)

const randaoMixesLength = 65536

// We only keep in memory a fraction of the beacon state when it comes to checkpoint.
type checkpointState struct {
	beaconConfig *clparams.BeaconChainConfig
	randaoMixes  solid.HashVectorSSZ
	shuffledSet  []uint64 // shuffled set of active validators
	// validator data
	balances []uint64
	// bitlists of active indexes and slashed indexes
	actives  []byte
	slasheds []byte

	publicKeysRegistry public_keys_registry.PublicKeyRegistry

	validatorSetSize int
	// fork data
	genesisValidatorsRoot  common.Hash
	fork                   *cltypes.Fork
	unslashedActiveBalance uint64
	epoch                  uint64
	checkpoint             solid.Checkpoint
}

func writeToBitset(bitset []byte, i int, value bool) {
	bitIndex := i % 8
	sliceIndex := i / 8
	if value {
		bitset[sliceIndex] = ((1 << bitIndex) | bitset[sliceIndex])
	} else {
		bitset[sliceIndex] &= ^(1 << uint(bitIndex))
	}
}

func readFromBitset(bitset []byte, i int) bool {
	bitIndex := i % 8
	sliceIndex := i / 8
	return (bitset[sliceIndex] & (1 << uint(bitIndex))) > 0
}

func newCheckpointState(beaconConfig *clparams.BeaconChainConfig, publicKeysRegistry public_keys_registry.PublicKeyRegistry, validatorSet []solid.Validator, randaoMixes solid.HashVectorSSZ,
	genesisValidatorsRoot common.Hash, fork *cltypes.Fork, unslashedActiveBalance, epoch uint64, checkpoint solid.Checkpoint,
) *checkpointState {
	balances := make([]uint64, len(validatorSet))

	bitsetSize := (len(validatorSet) + 7) / 8
	actives := make([]byte, bitsetSize)
	slasheds := make([]byte, bitsetSize)
	for i := range validatorSet {
		balances[i] = validatorSet[i].EffectiveBalance()
		writeToBitset(actives, i, validatorSet[i].Active(epoch))
		writeToBitset(slasheds, i, validatorSet[i].Slashed())
	}

	mixes := solid.NewHashVector(randaoMixesLength)
	randaoMixes.CopyTo(mixes)

	// bitsets size
	c := &checkpointState{
		beaconConfig:           beaconConfig,
		randaoMixes:            mixes,
		balances:               balances,
		genesisValidatorsRoot:  genesisValidatorsRoot,
		fork:                   fork,
		unslashedActiveBalance: unslashedActiveBalance,
		slasheds:               slasheds,
		actives:                actives,
		validatorSetSize:       len(validatorSet),
		checkpoint:             checkpoint,
		epoch:                  epoch,
		publicKeysRegistry:     publicKeysRegistry,
	}
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	activeIndicies := c.getActiveIndicies()
	monitor.ObserveActiveValidatorsCount(len(activeIndicies))
	c.shuffledSet = make([]uint64, len(activeIndicies))
	start := time.Now()
	c.shuffledSet = shuffling.ComputeShuffledIndicies(c.beaconConfig, c.randaoMixes.Get(int(mixPosition)), c.shuffledSet, activeIndicies, epoch*beaconConfig.SlotsPerEpoch)
	shuffling_metrics.ObserveComputeShuffledIndiciesTime(start)
	return c
}

// getAttestingIndicies retrieves the beacon committee.
func (c *checkpointState) getAttestingIndicies(attestation *solid.Attestation, aggregationBits []byte) ([]uint64, error) {
	slot := attestation.Data.Slot
	epoch := c.epochAtSlot(slot)
	lenIndicies := uint64(len(c.shuffledSet))
	committeesPerSlot := c.committeeCount(lenIndicies)
	count := committeesPerSlot * c.beaconConfig.SlotsPerEpoch
	committeeIndices := []uint64{attestation.Data.CommitteeIndex}
	if c.beaconConfig.GetCurrentStateVersion(epoch).AfterOrEqual(clparams.ElectraVersion) {
		electraCommitteeIndices := attestation.CommitteeBits.GetOnIndices()
		if len(electraCommitteeIndices) == 0 {
			return nil, errors.New("getAttestingIndicies: no committee bits set")
		}
		committeeIndices = committeeIndices[:0]
		for _, committeeIndex := range electraCommitteeIndices {
			committeeIndices = append(committeeIndices, uint64(committeeIndex))
		}
	}

	aggregationBitsLen := utils.GetBitlistLength(aggregationBits)
	committeeOffset := 0
	attestingIndices := []uint64{}
	for _, committeeIndex := range committeeIndices {
		if committeeIndex >= committeesPerSlot {
			return nil, fmt.Errorf("getAttestingIndicies: committee index %d is out of bounds for %d committees", committeeIndex, committeesPerSlot)
		}
		index := (slot%c.beaconConfig.SlotsPerEpoch)*committeesPerSlot + committeeIndex
		start := (lenIndicies * index) / count
		end := (lenIndicies * (index + 1)) / count
		committee := c.shuffledSet[start:end]
		if committeeOffset+len(committee) > aggregationBitsLen {
			return nil, fmt.Errorf("getAttestingIndicies: aggregation bits are too short, committeeOffset: %d, aggrBitsLen: %d, committeeSize: %d",
				committeeOffset, aggregationBitsLen, len(committee))
		}
		for i, member := range committee {
			bitPosition := committeeOffset + i
			if aggregationBits[bitPosition/8]&(1<<uint(bitPosition%8)) > 0 {
				attestingIndices = append(attestingIndices, member)
			}
		}
		committeeOffset += len(committee)
	}
	return attestingIndices, nil
}

func (c *checkpointState) getActiveIndicies() (activeIndicies []uint64) {
	for i := 0; i < c.validatorSetSize; i++ {
		if !readFromBitset(c.actives, i) {
			continue
		}
		activeIndicies = append(activeIndicies, uint64(i))
	}
	return activeIndicies
}

// committeeCount retrieves size of sync committee
func (c *checkpointState) committeeCount(lenIndicies uint64) uint64 {
	committeCount := max(min(c.beaconConfig.MaxCommitteesPerSlot, lenIndicies/c.beaconConfig.SlotsPerEpoch/c.beaconConfig.TargetCommitteeSize), 1)
	return committeCount
}

func (c *checkpointState) getDomain(domainType [4]byte, epoch uint64) ([]byte, error) {
	if epoch < c.fork.Epoch {
		return fork.ComputeDomain(domainType[:], c.fork.PreviousVersion, c.genesisValidatorsRoot)
	}
	return fork.ComputeDomain(domainType[:], c.fork.CurrentVersion, c.genesisValidatorsRoot)
}

// isValidIndexedAttestation verifies indexed attestation
func (c *checkpointState) isValidIndexedAttestation(att *cltypes.IndexedAttestation) (bool, error) {
	inds := att.AttestingIndices
	version := c.beaconConfig.GetCurrentStateVersion(att.Data.Target.Epoch)
	if err := corestate.ValidateIndexedAttestationIndices(c.beaconConfig, version, inds); err != nil {
		return false, err
	}

	domain, err := c.getDomain(c.beaconConfig.DomainBeaconAttester, att.Data.Target.Epoch)
	if err != nil {
		return false, fmt.Errorf("unable to get the domain: %w", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(att.Data, domain)
	if err != nil {
		return false, fmt.Errorf("unable to get signing root: %w", err)
	}

	valid, err := c.publicKeysRegistry.VerifyAggregateSignature(c.checkpoint, inds, signingRoot[:], att.Signature)
	if err != nil {
		return false, fmt.Errorf("error while validating signature: %w", err)
	}
	if !valid {
		return false, errors.New("invalid aggregate signature")
	}
	return true, nil
}

func (c *checkpointState) epochAtSlot(slot uint64) uint64 {
	return slot / c.beaconConfig.SlotsPerEpoch
}
