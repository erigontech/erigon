package raw

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func (b *BeaconState) SetVersion(version clparams.StateVersion) {
	b.version = version
}

func (b *BeaconState) SetSlot(slot uint64) {
	b.slot = slot
	b.markLeaf(SlotLeafIndex)
}

func (b *BeaconState) SetFork(fork *cltypes.Fork) {
	b.fork = fork
	b.markLeaf(ForkLeafIndex)
}

func (b *BeaconState) SetLatestBlockHeader(header *cltypes.BeaconBlockHeader) {
	b.latestBlockHeader = header
	b.markLeaf(LatestBlockHeaderLeafIndex)
}

func (b *BeaconState) SetBlockRootAt(index int, root libcommon.Hash) {
	b.markLeaf(BlockRootsLeafIndex)
	b.blockRoots[index] = root
}

func (b *BeaconState) SetStateRootAt(index int, root libcommon.Hash) {
	b.markLeaf(StateRootsLeafIndex)
	b.stateRoots[index] = root
}

func (b *BeaconState) SetHistoricalRootAt(index int, root [32]byte) {
	b.markLeaf(HistoricalRootsLeafIndex)
	b.historicalRoots[index] = root
}

func (b *BeaconState) SetWithdrawalCredentialForValidatorAtIndex(index int, creds libcommon.Hash) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].WithdrawalCredentials = creds
}

func (b *BeaconState) SetExitEpochForValidatorAtIndex(index int, epoch uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].ExitEpoch = epoch
}

func (b *BeaconState) SetWithdrawableEpochForValidatorAtIndex(index int, epoch uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].WithdrawableEpoch = epoch
}

func (b *BeaconState) SetEffectiveBalanceForValidatorAtIndex(index int, balance uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].EffectiveBalance = balance
}

func (b *BeaconState) SetActivationEpochForValidatorAtIndex(index int, epoch uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].ActivationEpoch = epoch
}

func (b *BeaconState) SetActivationEligibilityEpochForValidatorAtIndex(index int, epoch uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].ActivationEligibilityEpoch = epoch
}

func (b *BeaconState) SetEth1Data(eth1Data *cltypes.Eth1Data) {
	b.markLeaf(Eth1DataLeafIndex)
	b.eth1Data = eth1Data
}

func (b *BeaconState) AddEth1DataVote(vote *cltypes.Eth1Data) {
	b.markLeaf(Eth1DataVotesLeafIndex)
	b.eth1DataVotes = append(b.eth1DataVotes, vote)
}

func (b *BeaconState) ResetEth1DataVotes() {
	b.markLeaf(Eth1DataVotesLeafIndex)
	b.eth1DataVotes = nil
}

func (b *BeaconState) SetEth1DepositIndex(eth1DepositIndex uint64) {
	b.markLeaf(Eth1DepositIndexLeafIndex)
	b.eth1DepositIndex = eth1DepositIndex
}

func (b *BeaconState) SetValidatorSlashed(index int, slashed bool) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].Slashed = slashed
	return nil
}

func (b *BeaconState) SetValidatorWithdrawableEpoch(index int, epoch uint64) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].WithdrawableEpoch = epoch
	return nil
}
func (b *BeaconState) SetValidatorMinCurrentInclusionDelayAttestation(index int, value *cltypes.PendingAttestation) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].MinCurrentInclusionDelayAttestation = value
	return nil
}
func (b *BeaconState) SetValidatorIsCurrentMatchingSourceAttester(index int, value bool) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].IsCurrentMatchingSourceAttester = value
	return nil
}
func (b *BeaconState) SetValidatorIsCurrentMatchingTargetAttester(index int, value bool) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].IsCurrentMatchingTargetAttester = value
	return nil
}
func (b *BeaconState) SetValidatorIsCurrentMatchingHeadAttester(index int, value bool) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].IsCurrentMatchingHeadAttester = value
	return nil
}
func (b *BeaconState) SetValidatorMinPreviousInclusionDelayAttestation(index int, value *cltypes.PendingAttestation) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].MinPreviousInclusionDelayAttestation = value
	return nil
}
func (b *BeaconState) SetValidatorIsPreviousMatchingSourceAttester(index int, value bool) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].IsPreviousMatchingSourceAttester = value
	return nil
}
func (b *BeaconState) SetValidatorIsPreviousMatchingTargetAttester(index int, value bool) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].IsPreviousMatchingTargetAttester = value
	return nil
}
func (b *BeaconState) SetValidatorIsPreviousMatchingHeadAttester(index int, value bool) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators[index].IsPreviousMatchingHeadAttester = value
	return nil
}

func (b *BeaconState) SetValidatorBalance(index int, balance uint64) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(BalancesLeafIndex)
	b.balances[index] = balance
	return nil
}

// Should not be called if not for testing
func (b *BeaconState) SetValidators(validators []*cltypes.Validator) error {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators = validators
	return nil
}

func (b *BeaconState) AddValidator(validator *cltypes.Validator, balance uint64) {
	b.validators = append(b.validators, validator)
	b.balances = append(b.balances, balance)

	b.markLeaf(ValidatorsLeafIndex)
	b.markLeaf(BalancesLeafIndex)
}

func (b *BeaconState) SetBalances(balances []uint64) {
	b.markLeaf(BalancesLeafIndex)
	b.balances = balances
}

func (b *BeaconState) SetRandaoMixAt(index int, mix libcommon.Hash) {
	b.markLeaf(RandaoMixesLeafIndex)
	b.randaoMixes[index] = mix
}

func (b *BeaconState) SetSlashingSegmentAt(index int, segment uint64) {
	b.markLeaf(SlashingsLeafIndex)
	b.slashings[index] = segment
}
func (b *BeaconState) IncrementSlashingSegmentAt(index int, delta uint64) {
	b.markLeaf(SlashingsLeafIndex)
	b.slashings[index] += delta
}

func (b *BeaconState) SetEpochParticipationForValidatorIndex(isCurrentEpoch bool, index int, flags cltypes.ParticipationFlags) {
	if isCurrentEpoch {
		b.markLeaf(CurrentEpochParticipationLeafIndex)
		b.currentEpochParticipation[index] = flags
		return
	}
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation[index] = flags
}

func (b *BeaconState) SetValidatorAtIndex(index int, validator *cltypes.Validator) {
	b.validators[index] = validator
	b.markLeaf(ValidatorsLeafIndex)
}

func (b *BeaconState) ResetEpochParticipation() {
	b.previousEpochParticipation = b.currentEpochParticipation
	b.currentEpochParticipation = make(cltypes.ParticipationFlagsList, len(b.validators))
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.markLeaf(PreviousEpochParticipationLeafIndex)
}

func (b *BeaconState) SetJustificationBits(justificationBits cltypes.JustificationBits) {
	b.justificationBits = justificationBits
	b.markLeaf(JustificationBitsLeafIndex)
}

func (b *BeaconState) SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint *cltypes.Checkpoint) {
	b.previousJustifiedCheckpoint = previousJustifiedCheckpoint
	b.markLeaf(PreviousJustifiedCheckpointLeafIndex)
}

func (b *BeaconState) SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint *cltypes.Checkpoint) {
	b.currentJustifiedCheckpoint = currentJustifiedCheckpoint
	b.markLeaf(CurrentJustifiedCheckpointLeafIndex)
}

func (b *BeaconState) SetFinalizedCheckpoint(finalizedCheckpoint *cltypes.Checkpoint) {
	b.finalizedCheckpoint = finalizedCheckpoint
	b.markLeaf(FinalizedCheckpointLeafIndex)
}

func (b *BeaconState) SetCurrentSyncCommittee(currentSyncCommittee *cltypes.SyncCommittee) {
	b.currentSyncCommittee = currentSyncCommittee
	b.markLeaf(CurrentSyncCommitteeLeafIndex)
}

func (b *BeaconState) SetNextSyncCommittee(nextSyncCommittee *cltypes.SyncCommittee) {
	b.nextSyncCommittee = nextSyncCommittee
	b.markLeaf(NextSyncCommitteeLeafIndex)
}

func (b *BeaconState) SetLatestExecutionPayloadHeader(header *cltypes.Eth1Header) {
	b.latestExecutionPayloadHeader = header
	b.markLeaf(LatestExecutionPayloadHeaderLeafIndex)
}

func (b *BeaconState) SetNextWithdrawalIndex(index uint64) {
	b.nextWithdrawalIndex = index
	b.markLeaf(NextWithdrawalIndexLeafIndex)
}

func (b *BeaconState) SetNextWithdrawalValidatorIndex(index uint64) {
	b.nextWithdrawalValidatorIndex = index
	b.markLeaf(NextWithdrawalValidatorIndexLeafIndex)
}

func (b *BeaconState) ResetHistoricalSummaries() {
	b.historicalSummaries = nil
	b.markLeaf(HistoricalSummariesLeafIndex)
}
func (b *BeaconState) AddHistoricalSummary(summary *cltypes.HistoricalSummary) {
	b.historicalSummaries = append(b.historicalSummaries, summary)
	b.markLeaf(HistoricalSummariesLeafIndex)
}

func (b *BeaconState) AddHistoricalRoot(root libcommon.Hash) {
	b.historicalRoots = append(b.historicalRoots, root)
	b.markLeaf(HistoricalRootsLeafIndex)
}

func (b *BeaconState) SetInactivityScores(scores []uint64) {
	b.inactivityScores = scores
	b.markLeaf(InactivityScoresLeafIndex)
}

func (b *BeaconState) AddInactivityScore(score uint64) {
	b.inactivityScores = append(b.inactivityScores, score)
	b.markLeaf(InactivityScoresLeafIndex)
}

func (b *BeaconState) SetValidatorInactivityScore(index int, score uint64) error {
	if index >= len(b.inactivityScores) {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(InactivityScoresLeafIndex)
	b.inactivityScores[index] = score
	return nil
}

func (b *BeaconState) SetCurrentEpochParticipationFlags(flags []cltypes.ParticipationFlags) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochParticipation = flags
}

func (b *BeaconState) SetPreviousEpochParticipationFlags(flags []cltypes.ParticipationFlags) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation = flags
}

func (b *BeaconState) AddCurrentEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochParticipation = append(b.currentEpochParticipation, flags)
}

func (b *BeaconState) AddPreviousEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation = append(b.previousEpochParticipation, flags)
}
func (b *BeaconState) AddPreviousEpochParticipationAt(index int, delta byte) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation[index] = b.previousEpochParticipation[index].Add(int(delta))
}

// phase0 fields
func (b *BeaconState) AddCurrentEpochAtteastation(attestation *cltypes.PendingAttestation) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochAttestations = append(b.currentEpochAttestations, attestation)
}

func (b *BeaconState) AddPreviousEpochAttestation(attestation *cltypes.PendingAttestation) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations = append(b.previousEpochAttestations, attestation)
}

func (b *BeaconState) ResetCurrentEpochAttestations() {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochAttestations = nil
}

func (b *BeaconState) SetPreviousEpochAttestations(attestations []*cltypes.PendingAttestation) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations = attestations
}

func (b *BeaconState) ResetPreviousEpochAttestations() {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations = nil
}
