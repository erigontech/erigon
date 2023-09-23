package raw

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
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
	b.blockRoots.Set(index, root)
}

func (b *BeaconState) SetStateRootAt(index int, root libcommon.Hash) {
	b.markLeaf(StateRootsLeafIndex)
	b.stateRoots.Set(index, root)
}

func (b *BeaconState) SetWithdrawalCredentialForValidatorAtIndex(index int, creds libcommon.Hash) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetWithdrawalCredentialForValidatorAtIndex(index, creds)
}

func (b *BeaconState) SetExitEpochForValidatorAtIndex(index int, epoch uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetExitEpochForValidatorAtIndex(index, epoch)
}

func (b *BeaconState) SetWithdrawableEpochForValidatorAtIndex(index int, epoch uint64) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetWithdrawableEpochForValidatorAtIndex(index, epoch)
	return nil
}

func (b *BeaconState) SetEffectiveBalanceForValidatorAtIndex(index int, balance uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetEffectiveBalanceForValidatorAtIndex(index, balance)
}

func (b *BeaconState) SetActivationEpochForValidatorAtIndex(index int, epoch uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetActivationEpochForValidatorAtIndex(index, epoch)
}

func (b *BeaconState) SetActivationEligibilityEpochForValidatorAtIndex(index int, epoch uint64) {
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetActivationEligibilityEpochForValidatorAtIndex(index, epoch)
}

func (b *BeaconState) SetEth1Data(eth1Data *cltypes.Eth1Data) {
	b.markLeaf(Eth1DataLeafIndex)
	b.eth1Data = eth1Data
}

func (b *BeaconState) AddEth1DataVote(vote *cltypes.Eth1Data) {
	b.markLeaf(Eth1DataVotesLeafIndex)
	b.eth1DataVotes.Append(vote)
}

func (b *BeaconState) ResetEth1DataVotes() {
	b.markLeaf(Eth1DataVotesLeafIndex)
	b.eth1DataVotes.Clear()
}

func (b *BeaconState) SetEth1DepositIndex(eth1DepositIndex uint64) {
	b.markLeaf(Eth1DepositIndexLeafIndex)
	b.eth1DepositIndex = eth1DepositIndex
}

func (b *BeaconState) SetValidatorSlashed(index int, slashed bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetValidatorSlashed(index, slashed)
	return nil
}

func (b *BeaconState) SetValidatorMinCurrentInclusionDelayAttestation(index int, value *solid.PendingAttestation) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetMinCurrentInclusionDelayAttestation(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsCurrentMatchingSourceAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetIsCurrentMatchingSourceAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsCurrentMatchingTargetAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetIsCurrentMatchingTargetAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsCurrentMatchingHeadAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetIsCurrentMatchingHeadAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorMinPreviousInclusionDelayAttestation(index int, value *solid.PendingAttestation) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetMinPreviousInclusionDelayAttestation(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsPreviousMatchingSourceAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.validators.SetIsPreviousMatchingSourceAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsPreviousMatchingTargetAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)
	b.validators.SetIsPreviousMatchingTargetAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorIsPreviousMatchingHeadAttester(index int, value bool) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(ValidatorsLeafIndex)

	b.validators.SetIsPreviousMatchingHeadAttester(index, value)
	return nil
}

func (b *BeaconState) SetValidatorBalance(index int, balance uint64) error {
	if index >= b.balances.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(BalancesLeafIndex)
	b.balances.Set(index, balance)
	return nil
}

func (b *BeaconState) AddValidator(validator solid.Validator, balance uint64) {
	b.validators.Append(validator)
	b.balances.Append(balance)

	b.markLeaf(ValidatorsLeafIndex)
	b.markLeaf(BalancesLeafIndex)
}

func (b *BeaconState) SetRandaoMixAt(index int, mix libcommon.Hash) {
	b.markLeaf(RandaoMixesLeafIndex)
	b.randaoMixes.Set(index, mix)
}

func (b *BeaconState) SetSlashingSegmentAt(index int, segment uint64) {
	b.markLeaf(SlashingsLeafIndex)
	b.slashings.Set(index, segment)
}
func (b *BeaconState) IncrementSlashingSegmentAt(index int, delta uint64) {
	b.markLeaf(SlashingsLeafIndex)
	b.slashings.Set(index, b.SlashingSegmentAt(index)+delta)
}

func (b *BeaconState) SetEpochParticipationForValidatorIndex(isCurrentEpoch bool, index int, flags cltypes.ParticipationFlags) {
	if isCurrentEpoch {
		b.markLeaf(CurrentEpochParticipationLeafIndex)
		b.currentEpochParticipation.Set(index, byte(flags))
		return
	}
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation.Set(index, byte(flags))
}

func (b *BeaconState) SetValidatorAtIndex(index int, validator solid.Validator) {
	b.validators.Set(index, validator)
	b.markLeaf(ValidatorsLeafIndex)
}

func (b *BeaconState) ResetEpochParticipation() {
	b.previousEpochParticipation = b.currentEpochParticipation
	b.currentEpochParticipation = solid.NewBitList(b.validators.Length(), int(b.beaconConfig.ValidatorRegistryLimit))
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.markLeaf(PreviousEpochParticipationLeafIndex)
}

func (b *BeaconState) SetJustificationBits(justificationBits cltypes.JustificationBits) {
	b.justificationBits = justificationBits
	b.markLeaf(JustificationBitsLeafIndex)
}

func (b *BeaconState) SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint solid.Checkpoint) {
	b.previousJustifiedCheckpoint = previousJustifiedCheckpoint
	b.markLeaf(PreviousJustifiedCheckpointLeafIndex)
}

func (b *BeaconState) SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint solid.Checkpoint) {
	b.currentJustifiedCheckpoint = currentJustifiedCheckpoint
	b.markLeaf(CurrentJustifiedCheckpointLeafIndex)
}

func (b *BeaconState) SetFinalizedCheckpoint(finalizedCheckpoint solid.Checkpoint) {
	b.finalizedCheckpoint = finalizedCheckpoint
	b.markLeaf(FinalizedCheckpointLeafIndex)
}

func (b *BeaconState) SetCurrentSyncCommittee(currentSyncCommittee *solid.SyncCommittee) {
	b.currentSyncCommittee = currentSyncCommittee
	b.markLeaf(CurrentSyncCommitteeLeafIndex)
}

func (b *BeaconState) SetNextSyncCommittee(nextSyncCommittee *solid.SyncCommittee) {
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
	b.historicalSummaries.Clear()
	b.markLeaf(HistoricalSummariesLeafIndex)
}

func (b *BeaconState) AddHistoricalSummary(summary *cltypes.HistoricalSummary) {
	b.historicalSummaries.Append(summary)
	b.markLeaf(HistoricalSummariesLeafIndex)
}

func (b *BeaconState) AddHistoricalRoot(root libcommon.Hash) {
	b.historicalRoots.Append(root)
	b.markLeaf(HistoricalRootsLeafIndex)
}

func (b *BeaconState) SetInactivityScores(scores []uint64) {
	b.inactivityScores.Clear()
	for _, v := range scores {
		b.inactivityScores.Append(v)
	}
	b.markLeaf(InactivityScoresLeafIndex)
}

func (b *BeaconState) AddInactivityScore(score uint64) {
	b.inactivityScores.Append(score)
	b.markLeaf(InactivityScoresLeafIndex)
}

func (b *BeaconState) SetValidatorInactivityScore(index int, score uint64) error {
	if index >= b.inactivityScores.Length() {
		return ErrInvalidValidatorIndex
	}
	b.markLeaf(InactivityScoresLeafIndex)
	b.inactivityScores.Set(index, score)
	return nil
}

func (b *BeaconState) SetCurrentEpochParticipationFlags(flags []cltypes.ParticipationFlags) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochParticipation.Clear()
	for _, v := range flags {
		b.currentEpochParticipation.Append(byte(v))
	}
}

func (b *BeaconState) SetPreviousEpochParticipationFlags(flags []cltypes.ParticipationFlags) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation.Clear()
	for _, v := range flags {
		b.previousEpochParticipation.Append(byte(v))
	}
}

func (b *BeaconState) AddCurrentEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochParticipation.Append(byte(flags))
}

func (b *BeaconState) AddPreviousEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochParticipation.Append(byte(flags))
}

func (b *BeaconState) AddPreviousEpochParticipationAt(index int, delta byte) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	tmp := cltypes.ParticipationFlags(b.previousEpochParticipation.Get(index)).Add(int(delta))
	b.previousEpochParticipation.Set(index, byte(tmp))
}

// phase0 fields
func (b *BeaconState) AddCurrentEpochAtteastation(attestation *solid.PendingAttestation) {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochAttestations.Append(attestation)
}

func (b *BeaconState) AddPreviousEpochAttestation(attestation *solid.PendingAttestation) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations.Append(attestation)
}

func (b *BeaconState) ResetCurrentEpochAttestations() {
	b.markLeaf(CurrentEpochParticipationLeafIndex)
	b.currentEpochAttestations = solid.NewDynamicListSSZ[*solid.PendingAttestation](int(b.beaconConfig.PreviousEpochAttestationsLength()))
}

func (b *BeaconState) SetPreviousEpochAttestations(attestations *solid.ListSSZ[*solid.PendingAttestation]) {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations = attestations
}

func (b *BeaconState) ResetPreviousEpochAttestations() {
	b.markLeaf(PreviousEpochParticipationLeafIndex)
	b.previousEpochAttestations = solid.NewDynamicListSSZ[*solid.PendingAttestation](int(b.beaconConfig.PreviousEpochAttestationsLength()))
}
