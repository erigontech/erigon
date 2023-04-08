package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/cltypes"
)

const maxEth1Votes = 2048

// Below are setters. Note that they also dirty the state.

func (b *BeaconState) SetSlot(slot uint64) {
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnSlotChange(b.slot)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnSlotChange(slot)
	}
	b.touchedLeaves[SlotLeafIndex] = true
	b.slot = slot
	b.proposerIndex = nil
	if b.slot%b.beaconConfig.SlotsPerEpoch == 0 {
		b.totalActiveBalanceCache = nil
	}
}

func (b *BeaconState) SetFork(fork *cltypes.Fork) {
	b.touchedLeaves[ForkLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnForkChange(b.fork)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnForkChange(fork)
	}
	b.fork = fork
}

func (b *BeaconState) SetLatestBlockHeader(header *cltypes.BeaconBlockHeader) {
	b.touchedLeaves[LatestBlockHeaderLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnLatestHeaderChange(b.latestBlockHeader)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnLatestHeaderChange(header)
	}
	b.latestBlockHeader = header
}

func (b *BeaconState) SetBlockRootAt(index int, root libcommon.Hash) {
	b.touchedLeaves[BlockRootsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.BlockRootsChanges.AddChange(index, b.blockRoots[index])
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.BlockRootsChanges.AddChange(index, root)
	}
	b.blockRoots[index] = root
}

func (b *BeaconState) SetStateRootAt(index int, root libcommon.Hash) {
	b.touchedLeaves[StateRootsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.StateRootsChanges.AddChange(index, b.stateRoots[index])
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.StateRootsChanges.AddChange(index, root)
	}
	b.stateRoots[index] = root
}

func (b *BeaconState) SetHistoricalRootAt(index int, root [32]byte) {
	b.touchedLeaves[HistoricalRootsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.HistoricalRootsChanges.AddChange(index, b.historicalRoots[index])
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.HistoricalRootsChanges.AddChange(index, root)
	}
	b.historicalRoots[index] = root
}

func (b *BeaconState) SetWithdrawalCredentialForValidatorAtIndex(index int, creds libcommon.Hash) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.WithdrawalCredentialsChange.AddChange(index, b.validators[index].WithdrawalCredentials)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.WithdrawalCredentialsChange.AddChange(index, creds)
	}
	b.validators[index].WithdrawalCredentials = creds
}

func (b *BeaconState) SetExitEpochForValidatorAtIndex(index int, epoch uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.ExitEpochChange.AddChange(index, b.validators[index].ExitEpoch)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.ExitEpochChange.AddChange(index, epoch)
	}
	b.validators[index].ExitEpoch = epoch
}

func (b *BeaconState) SetWithdrawableEpochForValidatorAtIndex(index int, epoch uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.WithdrawalEpochChange.AddChange(index, b.validators[index].WithdrawableEpoch)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.WithdrawalEpochChange.AddChange(index, epoch)
	}
	b.validators[index].WithdrawableEpoch = epoch
}

func (b *BeaconState) SetEffectiveBalanceForValidatorAtIndex(index int, balance uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.EffectiveBalanceChange.AddChange(index, b.validators[index].EffectiveBalance)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.EffectiveBalanceChange.AddChange(index, balance)
	}
	b.validators[index].EffectiveBalance = balance
}

func (b *BeaconState) SetActivationEpochForValidatorAtIndex(index int, epoch uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.ActivationEpochChange.AddChange(index, b.validators[index].ActivationEpoch)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.ActivationEpochChange.AddChange(index, epoch)
	}
	b.validators[index].ActivationEpoch = epoch
}

func (b *BeaconState) SetActivationEligibilityEpochForValidatorAtIndex(index int, epoch uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.ActivationEligibilityEpochChange.AddChange(index, b.validators[index].ActivationEligibilityEpoch)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.ActivationEligibilityEpochChange.AddChange(index, epoch)
	}
	b.validators[index].ActivationEligibilityEpoch = epoch
}

func (b *BeaconState) SetEth1Data(eth1Data *cltypes.Eth1Data) {
	b.touchedLeaves[Eth1DataLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnEth1DataChange(eth1Data)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnEth1DataChange(eth1Data)
	}
	b.eth1Data = eth1Data
}

func (b *BeaconState) AddEth1DataVote(vote *cltypes.Eth1Data) {
	b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	if b.forwardChangeset != nil {
		b.forwardChangeset.AddVote(*vote)
	}
	b.eth1DataVotes = append(b.eth1DataVotes, vote)
}

func (b *BeaconState) ResetEth1DataVotes() {
	if b.reverseChangeset != nil {
		b.reverseChangeset.ReportVotesReset(b.eth1DataVotes)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.ReportVotesReset(nil)
	}
	b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	b.eth1DataVotes = nil
}

func (b *BeaconState) SetEth1DepositIndex(eth1DepositIndex uint64) {
	b.touchedLeaves[Eth1DepositIndexLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnEth1DepositIndexChange(b.eth1DepositIndex)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnEth1DepositIndexChange(eth1DepositIndex)
	}
	b.eth1DepositIndex = eth1DepositIndex
}

// Should not be called if not for testing
func (b *BeaconState) SetValidators(validators []*cltypes.Validator) error {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	b.validators = validators
	return b.initBeaconState()
}

func (b *BeaconState) AddValidator(validator *cltypes.Validator, balance uint64) {
	b.validators = append(b.validators, validator)
	b.balances = append(b.balances, balance)
	b.touchedLeaves[ValidatorsLeafIndex] = true
	b.touchedLeaves[BalancesLeafIndex] = true
	b.publicKeyIndicies[validator.PublicKey] = uint64(len(b.validators)) - 1
	if b.forwardChangeset != nil {
		b.forwardChangeset.BalancesChanges.OnAddNewElement(balance)
		b.forwardChangeset.OnNewValidator(validator)
	}
	// change in validator set means cache purging
	b.totalActiveBalanceCache = nil
}

func (b *BeaconState) SetBalances(balances []uint64) {
	b.touchedLeaves[BalancesLeafIndex] = true
	b.balances = balances
	b._refreshActiveBalances()
}

func (b *BeaconState) SetValidatorBalance(index int, balance uint64) error {
	if index >= len(b.balances) {
		return ErrInvalidValidatorIndex
	}

	b.touchedLeaves[BalancesLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.BalancesChanges.AddChange(index, b.balances[index])
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.BalancesChanges.AddChange(index, balance)
	}
	b.balances[index] = balance
	return nil
}

func (b *BeaconState) SetRandaoMixAt(index int, mix libcommon.Hash) {
	b.touchedLeaves[RandaoMixesLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.RandaoMixesChanges.AddChange(index, b.randaoMixes[index])
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.RandaoMixesChanges.AddChange(index, mix)
	}
	b.randaoMixes[index] = mix
}

func (b *BeaconState) SetSlashingSegmentAt(index int, segment uint64) {
	b.touchedLeaves[SlashingsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.SlashingsChanges.AddChange(index, b.slashings[index])
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.SlashingsChanges.AddChange(index, segment)
	}
	b.slashings[index] = segment
}

func (b *BeaconState) SetEpochParticipationForValidatorIndex(isCurrentEpoch bool, index int, flags cltypes.ParticipationFlags) {
	if isCurrentEpoch {
		if b.reverseChangeset != nil {
			b.reverseChangeset.CurrentEpochParticipationChanges.AddChange(index, b.currentEpochParticipation[index])
		}
		if b.forwardChangeset != nil {
			b.forwardChangeset.CurrentEpochParticipationChanges.AddChange(index, flags)
		}
		b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
		b.currentEpochParticipation[index] = flags
		return
	}
	if b.reverseChangeset != nil {
		b.reverseChangeset.PreviousEpochParticipationChanges.AddChange(index, b.previousEpochParticipation[index])
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.PreviousEpochParticipationChanges.AddChange(index, flags)
	}
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochParticipation[index] = flags
}

func (b *BeaconState) SetValidatorAtIndex(index int, validator *cltypes.Validator) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	b.validators[index] = validator
}

func (b *BeaconState) ResetEpochParticipation() {
	if b.reverseChangeset != nil {
		b.reverseChangeset.ReportEpochParticipationReset(b.previousEpochParticipation, b.currentEpochParticipation)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.ReportEpochParticipationReset(b.currentEpochParticipation, make(cltypes.ParticipationFlagsList, len(b.validators)))
	}
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.previousEpochParticipation = b.currentEpochParticipation
	b.currentEpochParticipation = make(cltypes.ParticipationFlagsList, len(b.validators))
}

func (b *BeaconState) SetJustificationBits(justificationBits cltypes.JustificationBits) {
	b.touchedLeaves[JustificationBitsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnJustificationBitsChange(b.justificationBits)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnJustificationBitsChange(justificationBits)
	}
	b.justificationBits = justificationBits
}

func (b *BeaconState) SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[PreviousJustifiedCheckpointLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnPreviousJustifiedCheckpointChange(b.previousJustifiedCheckpoint)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnPreviousJustifiedCheckpointChange(previousJustifiedCheckpoint)
	}
	b.previousJustifiedCheckpoint = previousJustifiedCheckpoint
}

func (b *BeaconState) SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[CurrentJustifiedCheckpointLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnCurrentJustifiedCheckpointChange(b.currentJustifiedCheckpoint)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnCurrentJustifiedCheckpointChange(currentJustifiedCheckpoint)
	}
	b.currentJustifiedCheckpoint = currentJustifiedCheckpoint
}

func (b *BeaconState) SetFinalizedCheckpoint(finalizedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[FinalizedCheckpointLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnFinalizedCheckpointChange(b.finalizedCheckpoint)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnFinalizedCheckpointChange(finalizedCheckpoint)
	}
	b.finalizedCheckpoint = finalizedCheckpoint
}

func (b *BeaconState) SetCurrentSyncCommittee(currentSyncCommittee *cltypes.SyncCommittee) {
	b.touchedLeaves[CurrentSyncCommitteeLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnCurrentSyncCommitteeChange(b.currentSyncCommittee)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnCurrentSyncCommitteeChange(currentSyncCommittee)
	}
	b.currentSyncCommittee = currentSyncCommittee
}

func (b *BeaconState) SetNextSyncCommittee(nextSyncCommittee *cltypes.SyncCommittee) {
	b.touchedLeaves[NextSyncCommitteeLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnNextSyncCommitteeChange(b.nextSyncCommittee)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnNextSyncCommitteeChange(nextSyncCommittee)
	}
	b.nextSyncCommittee = nextSyncCommittee
}

func (b *BeaconState) SetLatestExecutionPayloadHeader(header *cltypes.Eth1Header) {
	b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnEth1Header(b.latestExecutionPayloadHeader)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnEth1Header(header)
	}
	b.latestExecutionPayloadHeader = header
}

func (b *BeaconState) SetNextWithdrawalIndex(index uint64) {
	b.touchedLeaves[NextWithdrawalIndexLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnNextWithdrawalIndexChange(b.nextWithdrawalIndex)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnNextWithdrawalIndexChange(index)
	}
	b.nextWithdrawalIndex = index
}

func (b *BeaconState) SetNextWithdrawalValidatorIndex(index uint64) {
	b.touchedLeaves[NextWithdrawalValidatorIndexLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnNextWithdrawalValidatorIndexChange(b.nextWithdrawalValidatorIndex)
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.OnNextWithdrawalValidatorIndexChange(index)
	}
	b.nextWithdrawalValidatorIndex = index
}

func (b *BeaconState) AddHistoricalSummary(summary *cltypes.HistoricalSummary) {
	b.touchedLeaves[HistoricalSummariesLeafIndex] = true
	if b.forwardChangeset != nil {
		b.forwardChangeset.AddHistoricalSummary(*summary)
	}
	b.historicalSummaries = append(b.historicalSummaries, summary)
}

func (b *BeaconState) AddHistoricalRoot(root libcommon.Hash) {
	b.touchedLeaves[HistoricalRootsLeafIndex] = true
	if b.forwardChangeset != nil {
		b.forwardChangeset.HistoricalRootsChanges.OnAddNewElement(root)
	}
	b.historicalRoots = append(b.historicalRoots, root)
}

func (b *BeaconState) AddInactivityScore(score uint64) {
	b.touchedLeaves[InactivityScoresLeafIndex] = true
	if b.forwardChangeset != nil {
		b.forwardChangeset.InactivityScoresChanges.OnAddNewElement(score)
	}
	b.inactivityScores = append(b.inactivityScores, score)
}

func (b *BeaconState) SetValidatorInactivityScore(index int, score uint64) error {
	if index >= len(b.inactivityScores) {
		return ErrInvalidValidatorIndex
	}
	if b.reverseChangeset != nil {
		b.reverseChangeset.InactivityScoresChanges.AddChange(index, b.inactivityScores[index])
	}
	if b.forwardChangeset != nil {
		b.forwardChangeset.InactivityScoresChanges.AddChange(index, score)
	}
	b.touchedLeaves[InactivityScoresLeafIndex] = true
	b.inactivityScores[index] = score
	return nil
}

func (b *BeaconState) AddCurrentEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	if b.forwardChangeset != nil {
		b.forwardChangeset.CurrentEpochParticipationChanges.OnAddNewElement(flags)
	}
	b.currentEpochParticipation = append(b.currentEpochParticipation, flags)
}

func (b *BeaconState) AddPreviousEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	if b.forwardChangeset != nil {
		b.forwardChangeset.PreviousEpochParticipationChanges.OnAddNewElement(flags)
	}
	b.previousEpochParticipation = append(b.previousEpochParticipation, flags)
}

func (b *BeaconState) AddCurrentEpochAtteastation(attestation *cltypes.PendingAttestation) {
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.currentEpochAttestations = append(b.currentEpochAttestations, attestation)
}

func (b *BeaconState) AddPreviousEpochAttestation(attestation *cltypes.PendingAttestation) {
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochAttestations = append(b.previousEpochAttestations, attestation)
}

func (b *BeaconState) ResetCurrentEpochAttestations() {
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.currentEpochAttestations = nil
}

func (b *BeaconState) SetPreviousEpochAttestations(attestations []*cltypes.PendingAttestation) {
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochAttestations = attestations
}
