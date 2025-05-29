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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/monitor/shuffling_metrics"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"

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
	genesisValidatorsRoot common.Hash
	fork                  *cltypes.Fork
	activeBalance, epoch  uint64 // current active balance and epoch
	checkpoint            solid.Checkpoint
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
	genesisValidatorsRoot common.Hash, fork *cltypes.Fork, activeBalance, epoch uint64, checkpoint solid.Checkpoint) *checkpointState {
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
		beaconConfig:          beaconConfig,
		randaoMixes:           mixes,
		balances:              balances,
		genesisValidatorsRoot: genesisValidatorsRoot,
		fork:                  fork,
		activeBalance:         activeBalance,
		slasheds:              slasheds,
		actives:               actives,
		validatorSetSize:      len(validatorSet),
		checkpoint:            checkpoint,
		epoch:                 epoch,
		publicKeysRegistry:    publicKeysRegistry,
	}
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	activeIndicies := c.getActiveIndicies(epoch)
	monitor.ObserveActiveValidatorsCount(len(activeIndicies))
	c.shuffledSet = make([]uint64, len(activeIndicies))
	start := time.Now()
	c.shuffledSet = shuffling.ComputeShuffledIndicies(c.beaconConfig, c.randaoMixes.Get(int(mixPosition)), c.shuffledSet, activeIndicies, epoch*beaconConfig.SlotsPerEpoch)
	shuffling_metrics.ObserveComputeShuffledIndiciesTime(start)
	return c
}

// getAttestingIndicies retrieves the beacon committee.
func (c *checkpointState) getAttestingIndicies(attestation *solid.Attestation, aggregationBits []byte) ([]uint64, error) {
	// First get beacon committee
	slot := attestation.Data.Slot
	epoch := c.epochAtSlot(slot)
	cIndex := attestation.Data.CommitteeIndex
	clversion := c.beaconConfig.GetCurrentStateVersion(epoch)
	if clversion.AfterOrEqual(clparams.ElectraVersion) {
		index, err := attestation.GetCommitteeIndexFromBits()
		if err != nil {
			return nil, err
		}
		cIndex = index
	}

	// Compute shuffled indicies
	lenIndicies := uint64(len(c.shuffledSet))
	committeesPerSlot := c.committeeCount(epoch, lenIndicies)
	count := committeesPerSlot * c.beaconConfig.SlotsPerEpoch
	index := (slot%c.beaconConfig.SlotsPerEpoch)*committeesPerSlot + cIndex
	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	committee := c.shuffledSet[start:end]

	attestingIndices := []uint64{}
	for i, member := range committee {
		bitIndex := i % 8
		sliceIndex := i / 8
		if sliceIndex >= len(aggregationBits) {
			return nil, fmt.Errorf("getAttestingIndicies: committee is too big, committeeOffset: %d, aggrBitsLen: %d, committeeSize: %d",
				sliceIndex, len(aggregationBits), len(committee))
		}
		if (aggregationBits[sliceIndex] & (1 << bitIndex)) > 0 {
			attestingIndices = append(attestingIndices, member)
		}
	}
	return attestingIndices, nil
}

func (c *checkpointState) getActiveIndicies(epoch uint64) (activeIndicies []uint64) {
	for i := 0; i < c.validatorSetSize; i++ {
		if !readFromBitset(c.actives, i) {
			continue
		}
		activeIndicies = append(activeIndicies, uint64(i))
	}
	return activeIndicies
}

// committeeCount retrieves size of sync committee
func (c *checkpointState) committeeCount(epoch, lenIndicies uint64) uint64 {
	committeCount := lenIndicies / c.beaconConfig.SlotsPerEpoch / c.beaconConfig.TargetCommitteeSize
	if c.beaconConfig.MaxCommitteesPerSlot < committeCount {
		committeCount = c.beaconConfig.MaxCommitteesPerSlot
	}
	if committeCount < 1 {
		committeCount = 1
	}
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
	if inds.Length() == 0 || !solid.IsUint64SortedSet(inds) {
		return false, errors.New("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	domain, err := c.getDomain(c.beaconConfig.DomainBeaconAttester, att.Data.Target.Epoch)
	if err != nil {
		return false, fmt.Errorf("unable to get the domain: %v", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(att.Data, domain)
	if err != nil {
		return false, fmt.Errorf("unable to get signing root: %v", err)
	}

	valid, err := c.publicKeysRegistry.VerifyAggregateSignature(c.checkpoint, inds, signingRoot[:], att.Signature)
	if err != nil {
		return false, fmt.Errorf("error while validating signature: %v", err)
	}
	if !valid {
		return false, errors.New("invalid aggregate signature")
	}
	return true, nil
}
func (c *checkpointState) epochAtSlot(slot uint64) uint64 {
	return slot / c.beaconConfig.SlotsPerEpoch
}
