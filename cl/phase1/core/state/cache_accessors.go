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

package state

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor/shuffling_metrics"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/utils/threading"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
)

// these are view functions for the beacon state cache

// GetActiveValidatorsIndices returns the list of validator indices active for the given epoch.
func (b *CachingBeaconState) GetActiveValidatorsIndices(epoch uint64) []uint64 {
	var indicies []uint64
	if cachedIndicies, ok := b.activeValidatorsCache.Get(epoch); ok && len(cachedIndicies) > 0 {
		return cachedIndicies
	}

	numWorkers := runtime.NumCPU()
	wp := threading.NewParallelExecutor()
	indiciesShards := make([][]uint64, numWorkers)
	shardsJobSize := b.ValidatorLength() / numWorkers

	for i := 0; i < numWorkers; i++ {
		start := i * shardsJobSize
		end := (i + 1) * shardsJobSize
		if i == numWorkers-1 || end > b.ValidatorLength() {
			end = b.ValidatorLength()
		}
		indiciesShards[i] = make([]uint64, 0, end-start)
		workerID := i
		wp.AddWork(func() error {
			for j := start; j < end; j++ {
				if b.ValidatorSet().Get(j).Active(epoch) {
					indiciesShards[workerID] = append(indiciesShards[workerID], uint64(j))
				}
			}
			return nil
		})
	}

	wp.Execute()
	for i := 0; i < numWorkers; i++ {
		indicies = append(indicies, indiciesShards[i]...)
	}
	b.activeValidatorsCache.Add(epoch, indicies)
	return indicies
}

// GetTotalActiveBalance return the sum of all balances within active validators.
func (b *CachingBeaconState) GetTotalActiveBalance() uint64 {
	b._refreshActiveBalancesIfNeeded()

	return *b.totalActiveBalanceCache
}

// ComputeCommittee uses cache to compute compittee
func (b *CachingBeaconState) ComputeCommittee(
	indicies []uint64,
	slot uint64,
	index, count uint64,
) ([]uint64, error) {
	lenIndicies := uint64(len(indicies))
	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	var shuffledIndicies []uint64
	epoch := GetEpochAtSlot(b.BeaconConfig(), slot)
	beaconConfig := b.BeaconConfig()

	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	// Input for the seed hash.
	mix := b.GetRandaoMix(int(mixPosition))
	seed := shuffling.GetSeed(b.BeaconConfig(), mix, epoch, b.BeaconConfig().DomainBeaconAttester)

	if shuffledIndicesInterface, ok := b.shuffledSetsCache.Get(seed); ok {
		shuffledIndicies = shuffledIndicesInterface
	} else {
		blockRootAtBegginingPrevEpoch, err := b.GetBlockRootAtSlot(((epoch - 1) * b.BeaconConfig().SlotsPerEpoch) - 1)
		if cachedIndicies, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, blockRootAtBegginingPrevEpoch); ok && err == nil {
			shuffledIndicies = cachedIndicies
		} else {
			// print stack trace
			shuffledIndicies = make([]uint64, lenIndicies)
			start := time.Now()
			shuffledIndicies = shuffling.ComputeShuffledIndicies(b.BeaconConfig(), mix, shuffledIndicies, indicies, slot)
			shuffling_metrics.ObserveComputeShuffledIndiciesTime(start)
		}

		b.shuffledSetsCache.Add(seed, shuffledIndicies)
	}

	return shuffledIndicies[start:end], nil
}

// GetBeaconProposerIndex updates cache and gets the beacon proposer index
func (b *CachingBeaconState) GetBeaconProposerIndex() (uint64, error) {
	if b.Version() >= clparams.FuluVersion {
		p := b.GetProposerLookahead()
		index := int(b.Slot() % b.BeaconConfig().SlotsPerEpoch)
		return p.Get(index), nil
	}

	if b.proposerIndex == nil {
		if err := b._updateProposerIndex(); err != nil {
			return 0, err
		}
	}
	return *b.proposerIndex, nil
}

// GetBeaconProposerIndices returns the proposer indices for the given epoch
func (b *CachingBeaconState) GetBeaconProposerIndices(epoch uint64) ([]uint64, error) {
	indices := b.GetActiveValidatorsIndices(epoch)
	beaconConfig := b.BeaconConfig()
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	// Input for the seed hash.
	mix := b.GetRandaoMix(int(mixPosition))
	seed := shuffling.GetSeed(b.BeaconConfig(), mix, epoch, b.BeaconConfig().DomainBeaconProposer)

	// Write the seed to an array.
	seedArray := [32]byte{}
	copy(seedArray[:], seed[:])

	return shuffling.ComputeProposerIndices(b.BeaconState, epoch, seedArray, indices)
}

// GetBeaconProposerIndexForSlot compute the proposer index for a specific slot
func (b *CachingBeaconState) GetBeaconProposerIndexForSlot(slot uint64) (uint64, error) {
	epoch := slot / b.BeaconConfig().SlotsPerEpoch

	hash := sha256.New()
	beaconConfig := b.BeaconConfig()
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	// Input for the seed hash.
	mix := b.GetRandaoMix(int(mixPosition))
	input := shuffling.GetSeed(b.BeaconConfig(), mix, epoch, b.BeaconConfig().DomainBeaconProposer)
	slotByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(slotByteArray, slot)

	// Add slot to the end of the input.
	inputWithSlot := append(input[:], slotByteArray...)

	// Calculate the hash.
	hash.Write(inputWithSlot)
	seed := hash.Sum(nil)

	indices := b.GetActiveValidatorsIndices(epoch)
	// Write the seed to an array.
	seedArray := [32]byte{}
	copy(seedArray[:], seed)
	return shuffling.ComputeProposerIndex(b.BeaconState, indices, seedArray)
}

// BaseRewardPerIncrement return base rewards for processing sync committee and duties.
func (b *CachingBeaconState) BaseRewardPerIncrement() uint64 {
	b._refreshActiveBalancesIfNeeded()

	return b.BeaconConfig().EffectiveBalanceIncrement *
		b.BeaconConfig().BaseRewardFactor / b.totalActiveBalanceRootCache
}

// BaseReward return base rewards for processing sync committee and duties.
func (b *CachingBeaconState) BaseReward(index uint64) (uint64, error) {
	b._refreshActiveBalancesIfNeeded()

	effectiveBalance, err := b.ValidatorEffectiveBalance(int(index))
	if err != nil {
		return 0, err
	}
	if b.Version() != clparams.Phase0Version {
		return (effectiveBalance / b.BeaconConfig().EffectiveBalanceIncrement) * b.BaseRewardPerIncrement(), nil
	}
	return effectiveBalance * b.BeaconConfig().BaseRewardFactor / b.totalActiveBalanceRootCache / b.BeaconConfig().BaseRewardsPerEpoch, nil
}

// SyncRewards returns the proposer reward and the sync participant reward given the total active balance in state.
// It grabs values from cache as needed
func (b *CachingBeaconState) SyncRewards() (proposerReward, participantReward uint64, err error) {
	activeBalance := b.GetTotalActiveBalance()
	totalActiveIncrements := activeBalance / b.BeaconConfig().EffectiveBalanceIncrement
	baseRewardPerInc := b.BaseRewardPerIncrement()
	totalBaseRewards := baseRewardPerInc * totalActiveIncrements
	maxParticipantRewards := totalBaseRewards * b.BeaconConfig().SyncRewardWeight / b.BeaconConfig().WeightDenominator / b.BeaconConfig().SlotsPerEpoch
	participantReward = maxParticipantRewards / b.BeaconConfig().SyncCommitteeSize
	proposerReward = participantReward * b.BeaconConfig().ProposerWeight / (b.BeaconConfig().WeightDenominator - b.BeaconConfig().ProposerWeight)
	return
}

// CommitteeCount returns current number of committee for epoch.
func (b *CachingBeaconState) CommitteeCount(epoch uint64) uint64 {
	committeCount := uint64(
		len(b.GetActiveValidatorsIndices(epoch)),
	) / b.BeaconConfig().SlotsPerEpoch / b.BeaconConfig().TargetCommitteeSize
	if b.BeaconConfig().MaxCommitteesPerSlot < committeCount {
		committeCount = b.BeaconConfig().MaxCommitteesPerSlot
	}
	if committeCount < 1 {
		committeCount = 1
	}
	return committeCount
}

func (b *CachingBeaconState) GetAttestationParticipationFlagIndicies(
	data *solid.AttestationData,
	inclusionDelay uint64,
	skipAssert bool,
) ([]uint8, error) {

	var justifiedCheckpoint solid.Checkpoint
	// get checkpoint from epoch
	if data.Target.Epoch == Epoch(b) {
		justifiedCheckpoint = b.CurrentJustifiedCheckpoint()
	} else {
		justifiedCheckpoint = b.PreviousJustifiedCheckpoint()
	}
	// Matching roots
	if !data.Source.Equal(justifiedCheckpoint) && !skipAssert {
		return nil, errors.New("GetAttestationParticipationFlagIndicies: source does not match")
	}
	targetRoot, err := GetBlockRoot(b, data.Target.Epoch)
	if err != nil {
		return nil, err
	}
	headRoot, err := b.GetBlockRootAtSlot(data.Slot)
	if err != nil {
		return nil, err
	}
	matchingTarget := data.Target.Root == targetRoot
	matchingHead := matchingTarget && data.BeaconBlockRoot == headRoot
	participationFlagIndicies := []uint8{}
	if inclusionDelay <= utils.IntegerSquareRoot(b.BeaconConfig().SlotsPerEpoch) {
		participationFlagIndicies = append(
			participationFlagIndicies,
			b.BeaconConfig().TimelySourceFlagIndex,
		)
	}
	if b.Version() < clparams.DenebVersion && matchingTarget &&
		inclusionDelay <= b.BeaconConfig().SlotsPerEpoch {
		participationFlagIndicies = append(
			participationFlagIndicies,
			b.BeaconConfig().TimelyTargetFlagIndex,
		)
	}
	if b.Version() >= clparams.DenebVersion && matchingTarget {
		participationFlagIndicies = append(
			participationFlagIndicies,
			b.BeaconConfig().TimelyTargetFlagIndex,
		)
	}
	if matchingHead && inclusionDelay == b.BeaconConfig().MinAttestationInclusionDelay {
		participationFlagIndicies = append(
			participationFlagIndicies,
			b.BeaconConfig().TimelyHeadFlagIndex,
		)
	}
	return participationFlagIndicies, nil
}

// GetBeaconCommitee grabs beacon committee using cache first
func (b *CachingBeaconState) GetBeaconCommitee(slot, committeeIndex uint64) ([]uint64, error) {
	epoch := GetEpochAtSlot(b.BeaconConfig(), slot)
	committeesPerSlot := b.CommitteeCount(epoch)
	indicies := b.GetActiveValidatorsIndices(epoch)
	committee, err := b.ComputeCommittee(
		indicies,
		slot,
		(slot%b.BeaconConfig().SlotsPerEpoch)*committeesPerSlot+committeeIndex,
		committeesPerSlot*b.BeaconConfig().SlotsPerEpoch,
	)
	if err != nil {
		return nil, err
	}
	return committee, nil
}

func (b *CachingBeaconState) ComputeNextSyncCommittee() (*solid.SyncCommittee, error) {
	beaconConfig := b.BeaconConfig()
	optimizedHashFunc := utils.OptimizedSha256NotThreadSafe()
	epoch := Epoch(b) + 1
	//math.MaxUint8
	activeValidatorIndicies := b.GetActiveValidatorsIndices(epoch)
	activeValidatorCount := uint64(len(activeValidatorIndicies))
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	// Input for the seed hash.
	mix := b.GetRandaoMix(int(mixPosition))
	seed := shuffling.GetSeed(b.BeaconConfig(), mix, epoch, beaconConfig.DomainSyncCommittee)
	i := uint64(0)
	syncCommitteePubKeys := make([]common.Bytes48, 0, cltypes.SyncCommitteeSize)
	preInputs := shuffling.ComputeShuffledIndexPreInputs(b.BeaconConfig(), seed)
	for len(syncCommitteePubKeys) < cltypes.SyncCommitteeSize {
		shuffledIndex, err := shuffling.ComputeShuffledIndex(
			b.BeaconConfig(),
			i%activeValidatorCount,
			activeValidatorCount,
			seed,
			preInputs,
			optimizedHashFunc,
		)
		if err != nil {
			return nil, err
		}
		candidateIndex := activeValidatorIndicies[shuffledIndex]
		// retrieve validator.
		validator, err := b.ValidatorForValidatorIndex(int(candidateIndex))
		if err != nil {
			return nil, err
		}
		if b.Version() >= clparams.ElectraVersion {
			// electra and after
			// random_bytes = hash(seed + uint_to_bytes(i // 16))
			// offset = i % 16 * 2
			// random_value = bytes_to_uint64(random_bytes[offset:offset + 2])
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, i/16)
			input := append(seed[:], buf...)
			randomBytes := utils.Sha256(input)
			offset := (i % 16) * 2
			randomValue := binary.LittleEndian.Uint16(randomBytes[offset : offset+2])
			if validator.EffectiveBalance()*math.MaxUint16 >= beaconConfig.MaxEffectiveBalanceForVersion(b.Version())*uint64(randomValue) {
				syncCommitteePubKeys = append(syncCommitteePubKeys, validator.PublicKey())
			}
		} else {
			// Compute random byte.
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, i/32)
			input := append(seed[:], buf...)
			randomByte := uint64(utils.Sha256(input)[i%32])
			if validator.EffectiveBalance()*math.MaxUint8 >= beaconConfig.MaxEffectiveBalanceForVersion(b.Version())*randomByte {
				syncCommitteePubKeys = append(syncCommitteePubKeys, validator.PublicKey())
			}
		}
		i++
	}
	// Format public keys.
	formattedKeys := make([][]byte, cltypes.SyncCommitteeSize)
	for i := range formattedKeys {
		formattedKeys[i] = make([]byte, 48)
		copy(formattedKeys[i], syncCommitteePubKeys[i][:])
	}
	aggregatePublicKeyBytes, err := bls.AggregatePublickKeys(formattedKeys)
	if err != nil {
		return nil, err
	}
	var aggregate common.Bytes48
	copy(aggregate[:], aggregatePublicKeyBytes)

	return solid.NewSyncCommitteeFromParameters(syncCommitteePubKeys, aggregate), nil
}

// GetAttestingIndicies retrieves attesting indicies for a specific attestation. however some tests will not expect the aggregation bits check.
// thus, it is a flag now.
func (b *CachingBeaconState) GetAttestingIndicies(
	attestation *solid.Attestation,
	checkBitsLength bool,
) ([]uint64, error) {
	// check version
	slot := attestation.Data.Slot
	epoch := GetEpochAtSlot(b.BeaconConfig(), slot)
	clversion := b.BeaconConfig().GetCurrentStateVersion(epoch)

	if clversion.BeforeOrEqual(clparams.DenebVersion) {
		// deneb and before version
		aggregationBits := attestation.AggregationBits.Bytes()
		data := attestation.Data
		committee, err := b.GetBeaconCommitee(data.Slot, data.CommitteeIndex)
		if err != nil {
			return nil, err
		}
		aggregationBitsLen := utils.GetBitlistLength(aggregationBits)
		if checkBitsLength && utils.GetBitlistLength(aggregationBits) != len(committee) {
			return nil, fmt.Errorf(
				"GetAttestingIndicies: invalid aggregation bits. agg bits size: %d, expect: %d",
				aggregationBitsLen,
				len(committee),
			)
		}

		attestingIndices := []uint64{}
		for i, member := range committee {
			bitIndex := i % 8
			sliceIndex := i / 8
			if sliceIndex >= len(aggregationBits) {
				return nil, errors.New("GetAttestingIndicies: committee is too big")
			}
			if (aggregationBits[sliceIndex] & (1 << bitIndex)) > 0 {
				attestingIndices = append(attestingIndices, member)
			}
		}
		return attestingIndices, nil
	}

	// electra and after version
	var (
		committeeBits   = attestation.CommitteeBits
		aggregationBits = attestation.AggregationBits
		aggrBitsLen     = aggregationBits.Bits()
		attesters       = []uint64{}
	)
	committeeOffset := 0
	for _, committeeIndex := range committeeBits.GetOnIndices() {
		committee, err := b.GetBeaconCommitee(slot, uint64(committeeIndex))
		if err != nil {
			return nil, err
		}
		for i, member := range committee {
			if i >= aggrBitsLen {
				return nil, fmt.Errorf("GetAttestingIndicies: aggregation bits is too small, slot: %d, committeeIndex: %d, aggrBitsLen: %d, committeeSize: %d",
					slot, committeeIndex, aggrBitsLen, len(committee))
			}
			if aggregationBits.GetBitAt(committeeOffset + i) {
				attesters = append(attesters, member)
			}
		}
		committeeOffset += len(committee)
	}
	return attesters, nil
}

// See: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#get_validator_churn_limit
func (b *CachingBeaconState) GetValidatorChurnLimit() uint64 {
	activeIndsCount := uint64(len(b.GetActiveValidatorsIndices(Epoch(b))))
	return max(
		activeIndsCount/b.BeaconConfig().ChurnLimitQuotient,
		b.BeaconConfig().MinPerEpochChurnLimit,
	)
}

// https://github.com/ethereum/consensus-specs/blob/dev/specs/deneb/beacon-chain.md#new-get_validator_activation_churn_limit
func (b *CachingBeaconState) GetValidatorActivationChurnLimit() uint64 {
	if b.Version() >= clparams.DenebVersion {
		return min(
			b.BeaconConfig().MaxPerEpochActivationChurnLimit,
			b.GetValidatorChurnLimit(),
		)
	}
	return b.GetValidatorChurnLimit()
}
