package state

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/cache"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// these are view functions for the beacon state cache

// GetActiveValidatorsIndices returns the list of validator indices active for the given epoch.
func (b *BeaconState) GetActiveValidatorsIndices(epoch uint64) (indicies []uint64) {
	if cachedIndicies, ok := b.activeValidatorsCache.Get(epoch); ok && len(cachedIndicies) > 0 {
		return cachedIndicies
	}
	b.ForEachValidator(func(v *cltypes.Validator, i, total int) bool {
		if !v.Active(epoch) {
			return true
		}
		indicies = append(indicies, uint64(i))
		return true
	})
	b.activeValidatorsCache.Add(epoch, indicies)
	return
}

// GetTotalActiveBalance return the sum of all balances within active validators.
func (b *BeaconState) GetTotalActiveBalance() uint64 {
	if b.totalActiveBalanceCache == nil {
		b._refreshActiveBalances()
	}
	return *b.totalActiveBalanceCache
}

// ComputeCommittee uses cache to compute compittee
func (b *BeaconState) ComputeCommittee(indicies []uint64, slot uint64, index, count uint64) ([]uint64, error) {
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
		shuffledIndicies = shuffling.ComputeShuffledIndicies(b.BeaconConfig(), mix, indicies, slot)
		b.shuffledSetsCache.Add(seed, shuffledIndicies)
	}
	return shuffledIndicies[start:end], nil
}

// GetBeaconProposerIndex updates cache and gets the beacon proposer index
func (b *BeaconState) GetBeaconProposerIndex() (uint64, error) {
	if b.proposerIndex == nil {
		if err := b._updateProposerIndex(); err != nil {
			return 0, err
		}
	}
	return *b.proposerIndex, nil
}

// BaseRewardPerIncrement return base rewards for processing sync committee and duties.
func (b *BeaconState) BaseRewardPerIncrement() uint64 {
	if b.totalActiveBalanceCache == nil {
		b._refreshActiveBalances()
	}
	return b.BeaconConfig().EffectiveBalanceIncrement *
		b.BeaconConfig().BaseRewardFactor / b.totalActiveBalanceRootCache
}

// BaseReward return base rewards for processing sync committee and duties.
func (b *BeaconState) BaseReward(index uint64) (uint64, error) {
	if b.totalActiveBalanceCache == nil {
		b._refreshActiveBalances()
	}

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
func (b *BeaconState) SyncRewards() (proposerReward, participantReward uint64, err error) {
	activeBalance := b.GetTotalActiveBalance()
	if err != nil {
		return 0, 0, err
	}
	totalActiveIncrements := activeBalance / b.BeaconConfig().EffectiveBalanceIncrement
	baseRewardPerInc := b.BaseRewardPerIncrement()
	totalBaseRewards := baseRewardPerInc * totalActiveIncrements
	maxParticipantRewards := totalBaseRewards * b.BeaconConfig().SyncRewardWeight / b.BeaconConfig().WeightDenominator / b.BeaconConfig().SlotsPerEpoch
	participantReward = maxParticipantRewards / b.BeaconConfig().SyncCommitteeSize
	proposerReward = participantReward * b.BeaconConfig().ProposerWeight / (b.BeaconConfig().WeightDenominator - b.BeaconConfig().ProposerWeight)
	return
}

// CommitteeCount returns current number of committee for epoch.
func (b *BeaconState) CommitteeCount(epoch uint64) uint64 {
	committeCount := uint64(len(b.GetActiveValidatorsIndices(epoch))) / b.BeaconConfig().SlotsPerEpoch / b.BeaconConfig().TargetCommitteeSize
	if b.BeaconConfig().MaxCommitteesPerSlot < committeCount {
		committeCount = b.BeaconConfig().MaxCommitteesPerSlot
	}
	if committeCount < 1 {
		committeCount = 1
	}
	return committeCount
}

func (b *BeaconState) GetAttestationParticipationFlagIndicies(data solid.AttestationData, inclusionDelay uint64) ([]uint8, error) {
	var justifiedCheckpoint solid.Checkpoint
	// get checkpoint from epoch
	if data.Target().Epoch() == Epoch(b.BeaconState) {
		justifiedCheckpoint = b.CurrentJustifiedCheckpoint()
	} else {
		justifiedCheckpoint = b.PreviousJustifiedCheckpoint()
	}
	// Matching roots
	if !data.Source().Equal(justifiedCheckpoint) {
		return nil, fmt.Errorf("GetAttestationParticipationFlagIndicies: source does not match")
	}
	targetRoot, err := GetBlockRoot(b.BeaconState, data.Target().Epoch())
	if err != nil {
		return nil, err
	}
	headRoot, err := b.GetBlockRootAtSlot(data.Slot())
	if err != nil {
		return nil, err
	}
	matchingTarget := data.Target().BlockRoot() == targetRoot
	matchingHead := matchingTarget && data.BeaconBlockRoot() == headRoot
	participationFlagIndicies := []uint8{}
	if inclusionDelay <= utils.IntegerSquareRoot(b.BeaconConfig().SlotsPerEpoch) {
		participationFlagIndicies = append(participationFlagIndicies, b.BeaconConfig().TimelySourceFlagIndex)
	}
	if matchingTarget && inclusionDelay <= b.BeaconConfig().SlotsPerEpoch {
		participationFlagIndicies = append(participationFlagIndicies, b.BeaconConfig().TimelyTargetFlagIndex)
	}
	if matchingHead && inclusionDelay == b.BeaconConfig().MinAttestationInclusionDelay {
		participationFlagIndicies = append(participationFlagIndicies, b.BeaconConfig().TimelyHeadFlagIndex)
	}
	return participationFlagIndicies, nil
}

// GetBeaconCommitee grabs beacon committee using cache first
func (b *BeaconState) GetBeaconCommitee(slot, committeeIndex uint64) ([]uint64, error) {
	var cacheKey [16]byte
	binary.BigEndian.PutUint64(cacheKey[:], slot)
	binary.BigEndian.PutUint64(cacheKey[8:], committeeIndex)

	epoch := GetEpochAtSlot(b.BeaconConfig(), slot)
	committeesPerSlot := b.CommitteeCount(epoch)
	committee, err := b.ComputeCommittee(
		b.GetActiveValidatorsIndices(epoch),
		slot,
		(slot%b.BeaconConfig().SlotsPerEpoch)*committeesPerSlot+committeeIndex,
		committeesPerSlot*b.BeaconConfig().SlotsPerEpoch,
	)
	if err != nil {
		return nil, err
	}
	return committee, nil
}

func (b *BeaconState) ComputeNextSyncCommittee() (*solid.SyncCommittee, error) {
	beaconConfig := b.BeaconConfig()
	optimizedHashFunc := utils.OptimizedKeccak256NotThreadSafe()
	epoch := Epoch(b.BeaconState) + 1
	//math.MaxUint8
	activeValidatorIndicies := b.GetActiveValidatorsIndices(epoch)
	activeValidatorCount := uint64(len(activeValidatorIndicies))
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	// Input for the seed hash.
	mix := b.GetRandaoMix(int(mixPosition))
	seed := shuffling.GetSeed(b.BeaconConfig(), mix, epoch, beaconConfig.DomainSyncCommittee)
	i := uint64(0)
	syncCommitteePubKeys := make([][48]byte, 0, cltypes.SyncCommitteeSize)
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
		// Compute random byte.
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, i/32)
		input := append(seed[:], buf...)
		randomByte := uint64(utils.Keccak256(input)[i%32])
		// retrieve validator.
		validator, err := b.ValidatorForValidatorIndex(int(candidateIndex))
		if err != nil {
			return nil, err
		}
		if validator.EffectiveBalance()*math.MaxUint8 >= beaconConfig.MaxEffectiveBalance*randomByte {
			syncCommitteePubKeys = append(syncCommitteePubKeys, validator.PublicKey())
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
	var aggregate [48]byte
	copy(aggregate[:], aggregatePublicKeyBytes)

	return solid.NewSyncCommitteeFromParameters(syncCommitteePubKeys, aggregate), nil
}

// GetAttestingIndicies retrieves attesting indicies for a specific attestation. however some tests will not expect the aggregation bits check.
// thus, it is a flag now.
func (b *BeaconState) GetAttestingIndicies(attestation solid.AttestationData, aggregationBits []byte, checkBitsLength bool) ([]uint64, error) {
	if cached, ok := cache.LoadAttestatingIndicies(&attestation); ok {
		return cached, nil
	}
	committee, err := b.GetBeaconCommitee(attestation.Slot(), attestation.ValidatorIndex())
	if err != nil {
		return nil, err
	}
	aggregationBitsLen := utils.GetBitlistLength(aggregationBits)
	if checkBitsLength && utils.GetBitlistLength(aggregationBits) != len(committee) {
		return nil, fmt.Errorf("GetAttestingIndicies: invalid aggregation bits. agg bits size: %d, expect: %d", aggregationBitsLen, len(committee))
	}

	attestingIndices := []uint64{}
	for i, member := range committee {
		bitIndex := i % 8
		sliceIndex := i / 8
		if sliceIndex >= len(aggregationBits) {
			return nil, fmt.Errorf("GetAttestingIndicies: committee is too big")
		}
		if (aggregationBits[sliceIndex] & (1 << bitIndex)) > 0 {
			attestingIndices = append(attestingIndices, member)
		}
	}
	cache.StoreAttestation(&attestation, attestingIndices)
	return attestingIndices, nil
}

// Get the maximum number of validators that can be churned in a single epoch.
// See: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#get_validator_churn_limit
func (b *BeaconState) ValidatorChurnLimit() uint64 {
	activeValidatorsCount := uint64(len(b.GetActiveValidatorsIndices(Epoch(b.BeaconState))))
	churnLimit := activeValidatorsCount / b.BeaconConfig().ChurnLimitQuotient
	return utils.Max64(b.BeaconConfig().MinPerEpochChurnLimit, churnLimit)
}

// TODO: why are these the same...
func (b *BeaconState) GetValidatorChurnLimit() uint64 {
	activeIndsCount := uint64(len(b.GetActiveValidatorsIndices(Epoch(b.BeaconState))))
	return utils.Max64(activeIndsCount/b.BeaconConfig().ChurnLimitQuotient, b.BeaconConfig().MinPerEpochChurnLimit)
}
