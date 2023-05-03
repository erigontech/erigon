package state

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/Giulio2002/bls"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	eth2_shuffle "github.com/protolambda/eth2-shuffle"
)

const PreAllocatedRewardsAndPenalties = 8192

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

// PreviousEpoch returns previous epoch.
func (b *BeaconState) PreviousEpoch() uint64 {
	epoch := b.Epoch()
	if epoch == 0 {
		return epoch
	}
	return epoch - 1
}

// getUnslashedParticipatingIndices returns set of currently unslashed participating indexes
func (b *BeaconState) GetUnslashedParticipatingIndices(flagIndex int, epoch uint64) (validatorSet []uint64, err error) {
	var participation cltypes.ParticipationFlagsList
	// Must be either previous or current epoch
	switch epoch {
	case b.Epoch():
		participation = b.EpochParticipation(true)
	case b.PreviousEpoch():
		participation = b.EpochParticipation(false)
	default:
		return nil, fmt.Errorf("getUnslashedParticipatingIndices: only epoch and previous epoch can be used")
	}
	// Iterate over all validators and include the active ones that have flag_index enabled and are not slashed.
	b.ForEachValidator(func(validator *cltypes.Validator, i, total int) bool {
		if !validator.Active(epoch) ||
			!participation[i].HasFlag(flagIndex) ||
			validator.Slashed {
			return true
		}
		validatorSet = append(validatorSet, uint64(i))
		return true
	})
	return
}

// GetTotalActiveBalance return the sum of all balances within active validators.
func (b *BeaconState) GetTotalActiveBalance() uint64 {
	if b.totalActiveBalanceCache == nil {
		b._refreshActiveBalances()
	}
	return *b.totalActiveBalanceCache
}

// GetTotalSlashingAmount return the sum of all slashings.
func (b *BeaconState) GetTotalSlashingAmount() (t uint64) {
	b.ForEachSlashingSegment(func(v uint64, idx, total int) bool {
		t += v
		return true
	})
	return
}
func (b *BeaconState) ComputeShuffledIndex(ind, ind_count uint64, seed [32]byte, preInputs [][32]byte, hashFunc utils.HashFunc) (uint64, error) {
	if ind >= ind_count {
		return 0, fmt.Errorf("index=%d must be less than the index count=%d", ind, ind_count)
	}
	if len(preInputs) == 0 {
		preInputs = b.ComputeShuffledIndexPreInputs(seed)
	}
	for i := uint64(0); i < b.BeaconConfig().ShuffleRoundCount; i++ {
		// Read hash value.
		hashValue := binary.LittleEndian.Uint64(preInputs[i][:8])

		// Caclulate pivot and flip.
		pivot := hashValue % ind_count
		flip := (pivot + ind_count - ind) % ind_count

		// No uint64 max function in go standard library.
		position := ind
		if flip > ind {
			position = flip
		}

		// Construct the second hash input.
		positionByteArray := make([]byte, 4)
		binary.LittleEndian.PutUint32(positionByteArray, uint32(position>>8))
		input2 := append(seed[:], byte(i))
		input2 = append(input2, positionByteArray...)
		hashedInput2 := hashFunc(input2)
		// Read hash value.
		byteVal := hashedInput2[(position%256)/8]
		bitVal := (byteVal >> (position % 8)) % 2
		if bitVal == 1 {
			ind = flip
		}
	}
	return ind, nil
}

func ComputeShuffledIndicies(beaconConfig *clparams.BeaconChainConfig, mixes []libcommon.Hash, indicies []uint64, slot uint64) []uint64 {
	shuffledIndicies := make([]uint64, len(indicies))
	copy(shuffledIndicies, indicies)
	hashFunc := utils.OptimizedKeccak256()
	epoch := slot / beaconConfig.SlotsPerEpoch
	seed := GetSeed(beaconConfig, mixes, epoch, beaconConfig.DomainBeaconAttester)
	eth2ShuffleHashFunc := func(data []byte) []byte {
		hashed := hashFunc(data)
		return hashed[:]
	}
	eth2_shuffle.UnshuffleList(eth2ShuffleHashFunc, shuffledIndicies, uint8(beaconConfig.ShuffleRoundCount), seed)
	return shuffledIndicies
}

func (b *BeaconState) ComputeProposerIndex(indices []uint64, seed [32]byte) (uint64, error) {
	if len(indices) == 0 {
		return 0, nil
	}
	maxRandomByte := uint64(1<<8 - 1)
	i := uint64(0)
	total := uint64(len(indices))
	buf := make([]byte, 8)
	preInputs := b.ComputeShuffledIndexPreInputs(seed)
	for {
		shuffled, err := b.ComputeShuffledIndex(i%total, total, seed, preInputs, utils.Keccak256)
		if err != nil {
			return 0, err
		}
		candidateIndex := indices[shuffled]
		if candidateIndex >= uint64(b.ValidatorLength()) {
			return 0, fmt.Errorf("candidate index out of range: %d for validator set of length: %d", candidateIndex, b.ValidatorLength())
		}
		binary.LittleEndian.PutUint64(buf, i/32)
		input := append(seed[:], buf...)
		randomByte := uint64(utils.Keccak256(input)[i%32])

		validator, err := b.ValidatorForValidatorIndex(int(candidateIndex))
		if err != nil {
			return 0, err
		}
		if validator.EffectiveBalance*maxRandomByte >= b.BeaconConfig().MaxEffectiveBalance*randomByte {
			return candidateIndex, nil
		}
		i += 1
	}
}

func (b *BeaconState) ComputeCommittee(indicies []uint64, slot uint64, index, count uint64) ([]uint64, error) {
	lenIndicies := uint64(len(indicies))
	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	var shuffledIndicies []uint64
	epoch := b.GetEpochAtSlot(slot)
	randaoMixes := b.RandaoMixes()
	seed := GetSeed(b.BeaconConfig(), randaoMixes[:], epoch, b.BeaconConfig().DomainBeaconAttester)
	if shuffledIndicesInterface, ok := b.shuffledSetsCache.Get(seed); ok {
		shuffledIndicies = shuffledIndicesInterface
	} else {
		shuffledIndicies = ComputeShuffledIndicies(b.BeaconConfig(), randaoMixes[:], indicies, slot)
		b.shuffledSetsCache.Add(seed, shuffledIndicies)
	}
	return shuffledIndicies[start:end], nil
}

func (b *BeaconState) GetBeaconProposerIndex() (uint64, error) {
	if b.proposerIndex == nil {
		if err := b._updateProposerIndex(); err != nil {
			return 0, err
		}
	}
	return *b.proposerIndex, nil
}

func GetSeed(beaconConfig *clparams.BeaconChainConfig, mixes []libcommon.Hash, epoch uint64, domain [4]byte) libcommon.Hash {
	mix := mixes[(epoch+beaconConfig.EpochsPerHistoricalVector-beaconConfig.MinSeedLookahead-1)%beaconConfig.EpochsPerHistoricalVector]
	epochByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(epochByteArray, epoch)
	input := append(domain[:], epochByteArray...)
	input = append(input, mix[:]...)
	return utils.Keccak256(input)
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

func (b *BeaconState) ValidatorFromDeposit(deposit *cltypes.Deposit) *cltypes.Validator {
	amount := deposit.Data.Amount
	effectiveBalance := utils.Min64(amount-amount%b.BeaconConfig().EffectiveBalanceIncrement, b.BeaconConfig().MaxEffectiveBalance)

	return &cltypes.Validator{
		PublicKey:                  deposit.Data.PubKey,
		WithdrawalCredentials:      deposit.Data.WithdrawalCredentials,
		ActivationEligibilityEpoch: b.BeaconConfig().FarFutureEpoch,
		ActivationEpoch:            b.BeaconConfig().FarFutureEpoch,
		ExitEpoch:                  b.BeaconConfig().FarFutureEpoch,
		WithdrawableEpoch:          b.BeaconConfig().FarFutureEpoch,
		EffectiveBalance:           effectiveBalance,
	}
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

func (b *BeaconState) GetAttestationParticipationFlagIndicies(data *cltypes.AttestationData, inclusionDelay uint64) ([]uint8, error) {
	var justifiedCheckpoint *cltypes.Checkpoint
	// get checkpoint from epoch
	if data.Target.Epoch == b.Epoch() {
		justifiedCheckpoint = b.CurrentJustifiedCheckpoint()
	} else {
		justifiedCheckpoint = b.PreviousJustifiedCheckpoint()
	}
	// Matching roots
	if !data.Source.Equal(justifiedCheckpoint) {
		return nil, fmt.Errorf("GetAttestationParticipationFlagIndicies: source does not match")
	}
	targetRoot, err := b.GetBlockRoot(data.Target.Epoch)
	if err != nil {
		return nil, err
	}
	headRoot, err := b.GetBlockRootAtSlot(data.Slot)
	if err != nil {
		return nil, err
	}
	matchingTarget := data.Target.Root == targetRoot
	matchingHead := matchingTarget && data.BeaconBlockHash == headRoot
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

func (b *BeaconState) GetBeaconCommitee(slot, committeeIndex uint64) ([]uint64, error) {
	var cacheKey [16]byte
	binary.BigEndian.PutUint64(cacheKey[:], slot)
	binary.BigEndian.PutUint64(cacheKey[8:], committeeIndex)
	if cachedCommittee, ok := b.committeeCache.Get(cacheKey); ok {
		return cachedCommittee, nil
	}
	epoch := b.GetEpochAtSlot(slot)
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
	b.committeeCache.Add(cacheKey, committee)
	return committee, nil
}

func GetIndexedAttestation(attestation *cltypes.Attestation, attestingIndicies []uint64) *cltypes.IndexedAttestation {
	// Sort the the attestation indicies.
	sort.Slice(attestingIndicies, func(i, j int) bool {
		return attestingIndicies[i] < attestingIndicies[j]
	})
	return &cltypes.IndexedAttestation{
		AttestingIndices: attestingIndicies,
		Data:             attestation.Data,
		Signature:        attestation.Signature,
	}
}

// Implementation of get_eligible_validator_indices as defined in the eth 2.0 specs.
func (b *BeaconState) EligibleValidatorsIndicies() (eligibleValidators []uint64) {
	eligibleValidators = make([]uint64, 0, b.ValidatorLength())
	previousEpoch := b.PreviousEpoch()

	b.ForEachValidator(func(validator *cltypes.Validator, i, total int) bool {
		if validator.Active(previousEpoch) || (validator.Slashed && previousEpoch+1 < validator.WithdrawableEpoch) {
			eligibleValidators = append(eligibleValidators, uint64(i))
		}
		return true
	})
	return
}

// FinalityDelay determines by how many epochs we are late on finality.
func (b *BeaconState) FinalityDelay() uint64 {
	return b.PreviousEpoch() - b.FinalizedCheckpoint().Epoch
}

// Implementation of is_in_inactivity_leak. tells us if network is in danger pretty much. defined in ETH 2.0 specs.
func (b *BeaconState) InactivityLeaking() bool {
	return b.FinalityDelay() > b.BeaconConfig().MinEpochsToInactivityPenalty
}

func (b *BeaconState) IsUnslashedParticipatingIndex(epoch, index uint64, flagIdx int) bool {

	validator, err := b.ValidatorForValidatorIndex(int(index))
	if err != nil {
		return false
	}

	return validator.Active(epoch) && b.EpochParticipation(false)[index].HasFlag(flagIdx) && !validator.Slashed
}

// Implementation of is_eligible_for_activation_queue. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#is_eligible_for_activation_queue
func (b *BeaconState) IsValidatorEligibleForActivationQueue(validator *cltypes.Validator) bool {
	return validator.ActivationEligibilityEpoch == b.BeaconConfig().FarFutureEpoch &&
		validator.EffectiveBalance == b.BeaconConfig().MaxEffectiveBalance
}

// Implementation of is_eligible_for_activation. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#is_eligible_for_activation
func (b *BeaconState) IsValidatorEligibleForActivation(validator *cltypes.Validator) bool {
	return validator.ActivationEligibilityEpoch <= b.FinalizedCheckpoint().Epoch &&
		validator.ActivationEpoch == b.BeaconConfig().FarFutureEpoch
}

// Get the maximum number of validators that can be churned in a single epoch.
// See: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#get_validator_churn_limit
func (b *BeaconState) ValidatorChurnLimit() uint64 {
	activeValidatorsCount := uint64(len(b.GetActiveValidatorsIndices(b.Epoch())))
	churnLimit := activeValidatorsCount / b.BeaconConfig().ChurnLimitQuotient
	return utils.Max64(b.BeaconConfig().MinPerEpochChurnLimit, churnLimit)
}

// Check whether a merge transition is complete by verifying the presence of a valid execution payload header.
func (b *BeaconState) IsMergeTransitionComplete() bool {
	return !b.LatestExecutionPayloadHeader().IsZero()
}

// Compute the Unix timestamp at the specified slot number.
func (b *BeaconState) ComputeTimestampAtSlot(slot uint64) uint64 {
	return b.GenesisTime() + (slot-b.BeaconConfig().GenesisSlot)*b.BeaconConfig().SecondsPerSlot
}

// Check whether a validator is fully withdrawable at the given epoch.
func (b *BeaconState) isFullyWithdrawableValidator(validator *cltypes.Validator, balance uint64, epoch uint64) bool {
	return validator.WithdrawalCredentials[0] == b.BeaconConfig().ETH1AddressWithdrawalPrefixByte &&
		validator.WithdrawableEpoch <= epoch && balance > 0
}

// Check whether a validator is partially withdrawable.
func (b *BeaconState) isPartiallyWithdrawableValidator(validator *cltypes.Validator, balance uint64) bool {
	return validator.WithdrawalCredentials[0] == b.BeaconConfig().ETH1AddressWithdrawalPrefixByte &&
		validator.EffectiveBalance == b.BeaconConfig().MaxEffectiveBalance && balance > b.BeaconConfig().MaxEffectiveBalance
}

// ExpectedWithdrawals calculates the expected withdrawals that can be made by validators in the current epoch
func (b *BeaconState) ExpectedWithdrawals() []*types.Withdrawal {
	// Get the current epoch, the next withdrawal index, and the next withdrawal validator index
	currentEpoch := b.Epoch()
	nextWithdrawalIndex := b.NextWithdrawalIndex()
	nextWithdrawalValidatorIndex := b.NextWithdrawalValidatorIndex()

	// Determine the upper bound for the loop and initialize the withdrawals slice with a capacity of bound
	maxValidators := uint64(b.ValidatorLength())
	maxValidatorsPerWithdrawalsSweep := b.BeaconConfig().MaxValidatorsPerWithdrawalsSweep
	bound := utils.Min64(maxValidators, maxValidatorsPerWithdrawalsSweep)
	withdrawals := make([]*types.Withdrawal, 0, bound)

	// Loop through the validators to calculate expected withdrawals
	for validatorCount := uint64(0); validatorCount < bound && len(withdrawals) != int(b.BeaconConfig().MaxWithdrawalsPerPayload); validatorCount++ {
		// Get the validator and balance for the current validator index
		// supposedly this operation is safe because we checked the validator length about
		currentValidator, _ := b.ValidatorForValidatorIndex(int(nextWithdrawalValidatorIndex))
		currentBalance, _ := b.ValidatorBalance(int(nextWithdrawalValidatorIndex))

		// Check if the validator is fully withdrawable
		if b.isFullyWithdrawableValidator(currentValidator, currentBalance, currentEpoch) {
			// Add a new withdrawal with the validator's withdrawal credentials and balance
			newWithdrawal := &types.Withdrawal{
				Index:     nextWithdrawalIndex,
				Validator: nextWithdrawalValidatorIndex,
				Address:   libcommon.BytesToAddress(currentValidator.WithdrawalCredentials[12:]),
				Amount:    currentBalance,
			}
			withdrawals = append(withdrawals, newWithdrawal)
			nextWithdrawalIndex++
		} else if b.isPartiallyWithdrawableValidator(currentValidator, currentBalance) { // Check if the validator is partially withdrawable
			// Add a new withdrawal with the validator's withdrawal credentials and balance minus the maximum effective balance
			newWithdrawal := &types.Withdrawal{
				Index:     nextWithdrawalIndex,
				Validator: nextWithdrawalValidatorIndex,
				Address:   libcommon.BytesToAddress(currentValidator.WithdrawalCredentials[12:]),
				Amount:    currentBalance - b.BeaconConfig().MaxEffectiveBalance,
			}
			withdrawals = append(withdrawals, newWithdrawal)
			nextWithdrawalIndex++
		}

		// Increment the validator index, looping back to 0 if necessary
		nextWithdrawalValidatorIndex = (nextWithdrawalValidatorIndex + 1) % maxValidators
	}

	// Return the withdrawals slice
	return withdrawals
}
func (b *BeaconState) ComputeNextSyncCommittee() (*cltypes.SyncCommittee, error) {
	beaconConfig := b.BeaconConfig()
	optimizedHashFunc := utils.OptimizedKeccak256()
	epoch := b.Epoch() + 1
	//math.MaxUint8
	activeValidatorIndicies := b.GetActiveValidatorsIndices(epoch)
	activeValidatorCount := uint64(len(activeValidatorIndicies))
	mixes := b.RandaoMixes()
	seed := GetSeed(b.BeaconConfig(), mixes[:], epoch, beaconConfig.DomainSyncCommittee)
	i := uint64(0)
	syncCommitteePubKeys := make([][48]byte, 0, cltypes.SyncCommitteeSize)
	preInputs := b.ComputeShuffledIndexPreInputs(seed)
	for len(syncCommitteePubKeys) < cltypes.SyncCommitteeSize {
		shuffledIndex, err := b.ComputeShuffledIndex(i%activeValidatorCount, activeValidatorCount, seed, preInputs, optimizedHashFunc)
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
		if validator.EffectiveBalance*math.MaxUint8 >= beaconConfig.MaxEffectiveBalance*randomByte {
			syncCommitteePubKeys = append(syncCommitteePubKeys, validator.PublicKey)
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
	return &cltypes.SyncCommittee{
		PubKeys:            syncCommitteePubKeys,
		AggregatePublicKey: aggregate,
	}, nil
}

func (b *BeaconState) IsValidIndexedAttestation(att *cltypes.IndexedAttestation) (bool, error) {
	inds := att.AttestingIndices
	if len(inds) == 0 || !utils.IsSliceSortedSet(inds) {
		return false, fmt.Errorf("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := [][]byte{}
	for _, v := range inds {
		val, err := b.ValidatorForValidatorIndex(int(v))
		if err != nil {
			return false, err
		}
		pks = append(pks, val.PublicKey[:])
	}

	domain, err := b.GetDomain(b.BeaconConfig().DomainBeaconAttester, att.Data.Target.Epoch)
	if err != nil {
		return false, fmt.Errorf("unable to get the domain: %v", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(att.Data, domain)
	if err != nil {
		return false, fmt.Errorf("unable to get signing root: %v", err)
	}

	valid, err := bls.VerifyAggregate(att.Signature[:], signingRoot[:], pks)
	if err != nil {
		return false, fmt.Errorf("error while validating signature: %v", err)
	}
	if !valid {
		return false, fmt.Errorf("invalid aggregate signature")
	}
	return true, nil
}

// GetAttestingIndicies retrieves attesting indicies for a specific attestation. however some tests will not expect the aggregation bits check.
// thus, it is a flag now.
func (b *BeaconState) GetAttestingIndicies(attestation *cltypes.AttestationData, aggregationBits []byte, checkBitsLength bool) ([]uint64, error) {
	committee, err := b.GetBeaconCommitee(attestation.Slot, attestation.Index)
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
	return attestingIndices, nil
}
