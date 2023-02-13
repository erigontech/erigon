package state

import (
	"encoding/binary"
	"fmt"
	"sort"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	eth2_shuffle "github.com/protolambda/eth2-shuffle"
)

const PreAllocatedRewardsAndPenalties = 8192

// GetActiveValidatorsIndices returns the list of validator indices active for the given epoch.
func (b *BeaconState) GetActiveValidatorsIndices(epoch uint64) (indicies []uint64) {
	if cachedIndicies, ok := b.activeValidatorsCache.Get(epoch); ok {
		return cachedIndicies.([]uint64)
	}
	for i, validator := range b.validators {
		if !validator.Active(epoch) {
			continue
		}
		indicies = append(indicies, uint64(i))
	}
	b.activeValidatorsCache.Add(epoch, indicies)
	return
}

// GetEpochAtSlot gives the epoch for a certain slot
func (b *BeaconState) GetEpochAtSlot(slot uint64) uint64 {
	return slot / b.beaconConfig.SlotsPerEpoch
}

// Epoch returns current epoch.
func (b *BeaconState) Epoch() uint64 {
	return b.GetEpochAtSlot(b.slot)
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
		participation = b.currentEpochParticipation
	case b.PreviousEpoch():
		participation = b.previousEpochParticipation
	default:
		return nil, fmt.Errorf("getUnslashedParticipatingIndices: only epoch and previous epoch can be used")
	}
	// Iterate over all validators and include the active ones that have flag_index enabled and are not slashed.
	for i, validator := range b.Validators() {
		if !validator.Active(epoch) ||
			!participation[i].HasFlag(flagIndex) ||
			validator.Slashed {
			continue
		}
		validatorSet = append(validatorSet, uint64(i))
	}
	return
}

// GetTotalBalance return the sum of all balances within the given validator set.
func (b *BeaconState) GetTotalBalance(validatorSet []uint64) (uint64, error) {
	var (
		total          uint64
		validatorsSize = uint64(len(b.validators))
	)
	for _, validatorIndex := range validatorSet {
		// Should be in bounds.
		if validatorIndex >= validatorsSize {
			return 0, fmt.Errorf("GetTotalBalance: out of bounds validator index")
		}
		total += b.validators[validatorIndex].EffectiveBalance
	}
	// Always minimum set to EffectiveBalanceIncrement
	if total < b.beaconConfig.EffectiveBalanceIncrement {
		total = b.beaconConfig.EffectiveBalanceIncrement
	}
	return total, nil
}

// GetTotalActiveBalance return the sum of all balances within active validators.
func (b *BeaconState) GetTotalActiveBalance() uint64 {
	if b.totalActiveBalanceCache == nil {
		b._refreshActiveBalances()
	}
	if *b.totalActiveBalanceCache < b.beaconConfig.EffectiveBalanceIncrement {
		return b.beaconConfig.EffectiveBalanceIncrement
	}
	return *b.totalActiveBalanceCache
}

// GetTotalSlashingAmount return the sum of all slashings.
func (b *BeaconState) GetTotalSlashingAmount() (t uint64) {
	for _, slash := range &b.slashings {
		t += slash
	}
	return
}

// GetBlockRoot returns blook root at start of a given epoch
func (b *BeaconState) GetBlockRoot(epoch uint64) (libcommon.Hash, error) {
	return b.GetBlockRootAtSlot(epoch * b.beaconConfig.SlotsPerEpoch)
}

// GetBlockRootAtSlot returns the block root at a given slot
func (b *BeaconState) GetBlockRootAtSlot(slot uint64) (libcommon.Hash, error) {
	if slot >= b.slot {
		return libcommon.Hash{}, fmt.Errorf("GetBlockRootAtSlot: slot in the future")
	}
	if b.slot > slot+b.beaconConfig.SlotsPerHistoricalRoot {
		return libcommon.Hash{}, fmt.Errorf("GetBlockRootAtSlot: slot too much far behind")
	}
	return b.blockRoots[slot%b.beaconConfig.SlotsPerHistoricalRoot], nil
}

func (b *BeaconState) GetDomain(domainType [4]byte, epoch uint64) ([]byte, error) {
	if epoch == 0 {
		epoch = b.Epoch()
	}
	var forkVersion [4]byte
	if epoch < b.fork.Epoch {
		forkVersion = b.fork.PreviousVersion
	} else {
		forkVersion = b.fork.CurrentVersion
	}
	return fork.ComputeDomain(domainType[:], forkVersion, b.genesisValidatorsRoot)
}

func (b *BeaconState) ComputeShuffledIndexPreInputs(seed [32]byte) [][32]byte {
	ret := make([][32]byte, b.beaconConfig.ShuffleRoundCount)
	for i := range ret {
		ret[i] = utils.Keccak256(append(seed[:], byte(i)))
	}
	return ret
}

func (b *BeaconState) ComputeShuffledIndex(ind, ind_count uint64, seed [32]byte, preInputs [][32]byte, hashFunc utils.HashFunc) (uint64, error) {
	if ind >= ind_count {
		return 0, fmt.Errorf("index=%d must be less than the index count=%d", ind, ind_count)
	}
	if len(preInputs) == 0 {
		preInputs = b.ComputeShuffledIndexPreInputs(seed)
	}
	for i := uint64(0); i < b.beaconConfig.ShuffleRoundCount; i++ {
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

func (b *BeaconState) ComputeCommittee(indicies []uint64, seed libcommon.Hash, index, count uint64, hashFunc utils.HashFunc) ([]uint64, error) {
	lenIndicies := uint64(len(indicies))
	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	var shuffledIndicies []uint64
	if shuffledIndicesInterface, ok := b.shuffledSetsCache.Get(seed); ok {
		shuffledIndicies = shuffledIndicesInterface.([]uint64)
	} else {
		shuffledIndicies = make([]uint64, lenIndicies)
		copy(shuffledIndicies, indicies)
		eth2ShuffleHashFunc := func(data []byte) []byte {
			hashed := hashFunc(data)
			return hashed[:]
		}
		eth2_shuffle.UnshuffleList(eth2ShuffleHashFunc, shuffledIndicies, uint8(b.beaconConfig.ShuffleRoundCount), seed)
		b.shuffledSetsCache.Add(seed, shuffledIndicies)
	}
	return shuffledIndicies[start:end], nil
}

func (b *BeaconState) ComputeProposerIndex(indices []uint64, seed [32]byte) (uint64, error) {
	if len(indices) == 0 {
		return 0, fmt.Errorf("must have >0 indices")
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
		if candidateIndex >= uint64(len(b.validators)) {
			return 0, fmt.Errorf("candidate index out of range: %d for validator set of length: %d", candidateIndex, len(b.validators))
		}
		binary.LittleEndian.PutUint64(buf, i/32)
		input := append(seed[:], buf...)
		randomByte := uint64(utils.Keccak256(input)[i%32])

		validator, err := b.ValidatorAt(int(candidateIndex))
		if err != nil {
			return 0, err
		}
		if validator.EffectiveBalance*maxRandomByte >= clparams.MainnetBeaconConfig.MaxEffectiveBalance*randomByte {
			return candidateIndex, nil
		}
		i += 1
	}
}

func (b *BeaconState) GetRandaoMixes(epoch uint64) [32]byte {
	return b.randaoMixes[epoch%b.beaconConfig.EpochsPerHistoricalVector]
}

func (b *BeaconState) GetBeaconProposerIndex() (uint64, error) {
	if b.proposerIndex == nil {
		if err := b._updateProposerIndex(); err != nil {
			return 0, err
		}
	}
	return *b.proposerIndex, nil
}

func (b *BeaconState) GetSeed(epoch uint64, domain [4]byte) libcommon.Hash {
	mix := b.GetRandaoMixes(epoch + b.beaconConfig.EpochsPerHistoricalVector - b.beaconConfig.MinSeedLookahead - 1)
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
	return b.beaconConfig.EffectiveBalanceIncrement * b.beaconConfig.BaseRewardFactor / b.totalActiveBalanceRootCache
}

// BaseReward return base rewards for processing sync committee and duties.
func (b *BeaconState) BaseReward(index uint64) (uint64, error) {
	if index >= uint64(len(b.validators)) {
		return 0, InvalidValidatorIndex
	}
	return (b.validators[index].EffectiveBalance / b.beaconConfig.EffectiveBalanceIncrement) * b.BaseRewardPerIncrement(), nil
}

// SyncRewards returns the proposer reward and the sync participant reward given the total active balance in state.
func (b *BeaconState) SyncRewards() (proposerReward, participantReward uint64, err error) {
	activeBalance := b.GetTotalActiveBalance()
	if err != nil {
		return 0, 0, err
	}
	totalActiveIncrements := activeBalance / b.beaconConfig.EffectiveBalanceIncrement
	baseRewardPerInc := b.BaseRewardPerIncrement()
	totalBaseRewards := baseRewardPerInc * totalActiveIncrements
	maxParticipantRewards := totalBaseRewards * b.beaconConfig.SyncRewardWeight / b.beaconConfig.WeightDenominator / b.beaconConfig.SlotsPerEpoch
	participantReward = maxParticipantRewards / b.beaconConfig.SyncCommitteeSize
	proposerReward = participantReward * b.beaconConfig.ProposerWeight / (b.beaconConfig.WeightDenominator - b.beaconConfig.ProposerWeight)
	return
}

func (b *BeaconState) ValidatorFromDeposit(deposit *cltypes.Deposit) *cltypes.Validator {
	amount := deposit.Data.Amount
	effectiveBalance := amount - amount%b.beaconConfig.EffectiveBalanceIncrement
	if effectiveBalance > b.beaconConfig.EffectiveBalanceIncrement {
		effectiveBalance = b.beaconConfig.EffectiveBalanceIncrement
	}

	return &cltypes.Validator{
		PublicKey:                  deposit.Data.PubKey,
		WithdrawalCredentials:      deposit.Data.WithdrawalCredentials,
		ActivationEligibilityEpoch: b.beaconConfig.FarFutureEpoch,
		ActivationEpoch:            b.beaconConfig.FarFutureEpoch,
		ExitEpoch:                  b.beaconConfig.FarFutureEpoch,
		WithdrawableEpoch:          b.beaconConfig.FarFutureEpoch,
		EffectiveBalance:           effectiveBalance,
	}
}

// CommitteeCount returns current number of committee for epoch.
func (b *BeaconState) CommitteeCount(epoch uint64) uint64 {
	committeCount := uint64(len(b.GetActiveValidatorsIndices(epoch))) / b.beaconConfig.SlotsPerEpoch / b.beaconConfig.TargetCommitteeSize
	if b.beaconConfig.MaxCommitteesPerSlot < committeCount {
		committeCount = b.beaconConfig.MaxCommitteesPerSlot
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
		justifiedCheckpoint = b.currentJustifiedCheckpoint
	} else {
		justifiedCheckpoint = b.previousJustifiedCheckpoint
	}
	// Matching roots
	if *data.Source != *justifiedCheckpoint {
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
	if inclusionDelay <= utils.IntegerSquareRoot(b.beaconConfig.SlotsPerEpoch) {
		participationFlagIndicies = append(participationFlagIndicies, b.beaconConfig.TimelySourceFlagIndex)
	}
	if matchingTarget && inclusionDelay <= b.beaconConfig.SlotsPerEpoch {
		participationFlagIndicies = append(participationFlagIndicies, b.beaconConfig.TimelyTargetFlagIndex)
	}
	if matchingHead && inclusionDelay == b.beaconConfig.MinAttestationInclusionDelay {
		participationFlagIndicies = append(participationFlagIndicies, b.beaconConfig.TimelyHeadFlagIndex)
	}
	return participationFlagIndicies, nil
}

func (b *BeaconState) GetBeaconCommitee(slot, committeeIndex uint64) ([]uint64, error) {
	var cacheKey [16]byte
	binary.BigEndian.PutUint64(cacheKey[:], slot)
	binary.BigEndian.PutUint64(cacheKey[8:], committeeIndex)
	if cachedCommittee, ok := b.committeeCache.Get(cacheKey); ok {
		return cachedCommittee.([]uint64), nil
	}
	epoch := b.GetEpochAtSlot(slot)
	committeesPerSlot := b.CommitteeCount(epoch)
	seed := b.GetSeed(epoch, b.beaconConfig.DomainBeaconAttester)
	hashFunc := utils.OptimizedKeccak256()
	committee, err := b.ComputeCommittee(
		b.GetActiveValidatorsIndices(epoch),
		seed,
		(slot%b.beaconConfig.SlotsPerEpoch)*committeesPerSlot+committeeIndex,
		committeesPerSlot*b.beaconConfig.SlotsPerEpoch,
		hashFunc,
	)
	if err != nil {
		return nil, err
	}
	b.committeeCache.Add(cacheKey, committee)
	return committee, nil
}

func (b *BeaconState) GetIndexedAttestation(attestation *cltypes.Attestation, attestingIndicies []uint64) (*cltypes.IndexedAttestation, error) {
	// Sort the the attestation indicies.
	sort.Slice(attestingIndicies, func(i, j int) bool {
		return attestingIndicies[i] < attestingIndicies[j]
	})
	return &cltypes.IndexedAttestation{
		AttestingIndices: attestingIndicies,
		Data:             attestation.Data,
		Signature:        attestation.Signature,
	}, nil
}

func (b *BeaconState) GetAttestingIndicies(attestation *cltypes.AttestationData, aggregationBits []byte) ([]uint64, error) {
	committee, err := b.GetBeaconCommitee(attestation.Slot, attestation.Index)
	if err != nil {
		return nil, err
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

// Implementation of get_eligible_validator_indices as defined in the eth 2.0 specs.
func (b *BeaconState) EligibleValidatorsIndicies() (eligibleValidators []uint64) {
	eligibleValidators = make([]uint64, 0, len(b.validators))
	previousEpoch := b.PreviousEpoch()
	// TODO(Giulio2002): Proper caching
	for i, validator := range b.validators {
		if validator.Active(previousEpoch) || (validator.Slashed && previousEpoch+1 < validator.WithdrawableEpoch) {
			eligibleValidators = append(eligibleValidators, uint64(i))
		}
	}
	return
}

// Implementation of is_in_inactivity_leak. tells us if network is in danger pretty much. defined in ETH 2.0 specs.
func (b *BeaconState) InactivityLeaking() bool {
	return (b.PreviousEpoch() - b.finalizedCheckpoint.Epoch) > b.beaconConfig.MinEpochsToInactivityPenalty
}

func (b *BeaconState) IsUnslashedParticipatingIndex(epoch, index uint64, flagIdx int) bool {
	return b.validators[index].Active(epoch) &&
		b.previousEpochParticipation[index].HasFlag(flagIdx) &&
		!b.validators[index].Slashed
}

// Implementation of is_eligible_for_activation_queue. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#is_eligible_for_activation_queue
func (b *BeaconState) IsValidatorEligibleForActivationQueue(validator *cltypes.Validator) bool {
	return validator.ActivationEligibilityEpoch == b.beaconConfig.FarFutureEpoch &&
		validator.EffectiveBalance == b.beaconConfig.MaxEffectiveBalance
}

// Implementation of is_eligible_for_activation. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#is_eligible_for_activation
func (b *BeaconState) IsValidatorEligibleForActivation(validator *cltypes.Validator) bool {
	return validator.ActivationEligibilityEpoch <= b.finalizedCheckpoint.Epoch &&
		validator.ActivationEpoch == b.beaconConfig.FarFutureEpoch
}

// Implementation of get_validator_churn_limit. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#get_validator_churn_limit
func (b *BeaconState) ValidatorChurnLimit() (limit uint64) {
	activeValidatorsCount := uint64(len(b.GetActiveValidatorsIndices(b.Epoch())))
	limit = activeValidatorsCount / b.beaconConfig.ChurnLimitQuotient
	if limit < b.beaconConfig.MinPerEpochChurnLimit {
		limit = b.beaconConfig.MinPerEpochChurnLimit
	}
	return

}
