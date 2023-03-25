package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

const maxEth1Votes = 2048

// Below are setters. Note that they also dirty the state.

func (b *BeaconState) SetSlot(slot uint64) {
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnSlotChange(b.slot)
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
	b.fork = fork
}

func (b *BeaconState) SetLatestBlockHeader(header *cltypes.BeaconBlockHeader) {
	b.touchedLeaves[LatestBlockHeaderLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnLatestHeaderChange(b.latestBlockHeader)
	}
	b.latestBlockHeader = header
}

func (b *BeaconState) SetBlockRootAt(index int, root libcommon.Hash) {
	b.touchedLeaves[BlockRootsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.BlockRootsChanges.AddChange(index, root)
	}
	b.blockRoots[index] = root
}

func (b *BeaconState) SetStateRootAt(index int, root libcommon.Hash) {
	b.touchedLeaves[StateRootsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.StateRootsChanges.AddChange(index, root)
	}
	b.stateRoots[index] = root
}

func (b *BeaconState) SetHistoricalRootAt(index int, root [32]byte) {
	b.touchedLeaves[HistoricalRootsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.HistoricalRootsChanges.AddChange(index, root)
	}
	b.historicalRoots[index] = root
}

func (b *BeaconState) SetWithdrawalCredentialForValidatorAtIndex(index int, creds libcommon.Hash) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.WithdrawalCredentialsChange.AddChange(index, b.validators[index].WithdrawalCredentials)
	}
	b.validators[index].WithdrawalCredentials = creds
}

func (b *BeaconState) SetEffectiveBalanceForValidatorAtIndex(index int, balance uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.EffectiveBalanceChange.AddChange(index, b.validators[index].EffectiveBalance)
	}
	b.validators[index].EffectiveBalance = balance
}

func (b *BeaconState) SetActivationEpochForValidatorAtIndex(index int, epoch uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.ActivationEpochChange.AddChange(index, b.validators[index].ActivationEpoch)
	}
	b.validators[index].ActivationEpoch = epoch
}

func (b *BeaconState) SetActivationEligibilityEpochForValidatorAtIndex(index int, epoch uint64) {
	b.touchedLeaves[ValidatorsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.ActivationEligibilityEpochChange.AddChange(index, b.validators[index].ActivationEligibilityEpoch)
	}
	b.validators[index].ActivationEligibilityEpoch = epoch
}

func (b *BeaconState) SetEth1Data(eth1Data *cltypes.Eth1Data) {
	b.touchedLeaves[Eth1DataLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnEth1DataChange(eth1Data)
	}
	b.eth1Data = eth1Data
}

func (b *BeaconState) AddEth1DataVote(vote *cltypes.Eth1Data) {
	b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	b.eth1DataVotes = append(b.eth1DataVotes, vote)
}

func (b *BeaconState) ResetEth1DataVotes() {
	b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	b.eth1DataVotes = b.eth1DataVotes[:0]
}

func (b *BeaconState) SetEth1DepositIndex(eth1DepositIndex uint64) {
	b.touchedLeaves[Eth1DepositIndexLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnEth1DepositIndexChange(eth1DepositIndex)
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
	b.balances[index] = balance
	return nil
}

func (b *BeaconState) SetRandaoMixAt(index int, mix libcommon.Hash) {
	b.touchedLeaves[RandaoMixesLeafIndex] = true
	b.randaoMixes[index] = mix
}

func (b *BeaconState) SetSlashingSegmentAt(index int, segment uint64) {
	b.touchedLeaves[SlashingsLeafIndex] = true
	b.slashings[index] = segment
}

func (b *BeaconState) SetPreviousEpochParticipation(previousEpochParticipation []cltypes.ParticipationFlags) {
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochParticipation = previousEpochParticipation
}

func (b *BeaconState) SetCurrentEpochParticipation(currentEpochParticipation []cltypes.ParticipationFlags) {
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.currentEpochParticipation = currentEpochParticipation
}

func (b *BeaconState) SetJustificationBits(justificationBits cltypes.JustificationBits) {
	b.touchedLeaves[JustificationBitsLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnJustificationBitsChange(b.justificationBits)
	}
	b.justificationBits = justificationBits
}

func (b *BeaconState) SetPreviousJustifiedCheckpoint(previousJustifiedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[PreviousJustifiedCheckpointLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnPreviousJustifiedCheckpointChange(b.previousJustifiedCheckpoint)
	}
	b.previousJustifiedCheckpoint = previousJustifiedCheckpoint
}

func (b *BeaconState) SetCurrentJustifiedCheckpoint(currentJustifiedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[CurrentJustifiedCheckpointLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnCurrentJustifiedCheckpointChange(b.currentJustifiedCheckpoint)
	}
	b.currentJustifiedCheckpoint = currentJustifiedCheckpoint
}

func (b *BeaconState) SetFinalizedCheckpoint(finalizedCheckpoint *cltypes.Checkpoint) {
	b.touchedLeaves[FinalizedCheckpointLeafIndex] = true
	if b.reverseChangeset != nil {
		b.reverseChangeset.OnFinalizedCheckpointChange(b.finalizedCheckpoint)
	}
	b.finalizedCheckpoint = finalizedCheckpoint
}

func (b *BeaconState) SetCurrentSyncCommittee(currentSyncCommittee *cltypes.SyncCommittee) {
	b.touchedLeaves[CurrentSyncCommitteeLeafIndex] = true
	b.currentSyncCommittee = currentSyncCommittee
}

func (b *BeaconState) SetNextSyncCommittee(nextSyncCommittee *cltypes.SyncCommittee) {
	b.touchedLeaves[NextSyncCommitteeLeafIndex] = true
	b.nextSyncCommittee = nextSyncCommittee
}

func (b *BeaconState) SetLatestExecutionPayloadHeader(header *cltypes.Eth1Header) {
	b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
	b.latestExecutionPayloadHeader = header
}

func (b *BeaconState) SetNextWithdrawalIndex(index uint64) {
	b.touchedLeaves[NextWithdrawalIndexLeafIndex] = true
	b.nextWithdrawalIndex = index
}

func (b *BeaconState) SetNextWithdrawalValidatorIndex(index uint64) {
	b.touchedLeaves[NextWithdrawalValidatorIndexLeafIndex] = true
	b.nextWithdrawalValidatorIndex = index
}

func (b *BeaconState) AddHistoricalSummary(summary *cltypes.HistoricalSummary) {
	b.touchedLeaves[HistoricalSummariesLeafIndex] = true
	b.historicalSummaries = append(b.historicalSummaries, summary)
}

func (b *BeaconState) AddHistoricalRoot(root libcommon.Hash) {
	b.touchedLeaves[HistoricalRootsLeafIndex] = true
	b.historicalRoots = append(b.historicalRoots, root)
}

func (b *BeaconState) AddInactivityScore(score uint64) {
	b.touchedLeaves[InactivityScoresLeafIndex] = true
	b.inactivityScores = append(b.inactivityScores, score)
}

func (b *BeaconState) SetValidatorInactivityScore(index int, score uint64) error {
	if index >= len(b.inactivityScores) {
		return ErrInvalidValidatorIndex
	}
	b.touchedLeaves[InactivityScoresLeafIndex] = true
	b.inactivityScores[index] = score
	return nil
}

func (b *BeaconState) AddCurrentEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	if b.version == clparams.Phase0Version {
		panic("cannot call AddCurrentEpochParticipationFlags on phase0")
	}
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.currentEpochParticipation = append(b.currentEpochParticipation, flags)
}

func (b *BeaconState) AddPreviousEpochParticipationFlags(flags cltypes.ParticipationFlags) {
	if b.version == clparams.Phase0Version {
		panic("cannot call AddPreviousEpochParticipationFlags on phase0")
	}
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochParticipation = append(b.previousEpochParticipation, flags)
}

func (b *BeaconState) AddCurrentEpochAtteastation(attestation *cltypes.PendingAttestation) {
	if b.version != clparams.Phase0Version {
		panic("can call AddCurrentEpochAtteastation only on phase0")
	}
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.currentEpochAttestations = append(b.currentEpochAttestations, attestation)
}

func (b *BeaconState) AddPreviousEpochAtteastation(attestation *cltypes.PendingAttestation) {
	if b.version != clparams.Phase0Version {
		panic("can call AddPreviousEpochAtteastation only on phase0")
	}
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochAttestations = append(b.previousEpochAttestations, attestation)
}

func (b *BeaconState) SetCurrentEpochAtteastations(attestations []*cltypes.PendingAttestation) {
	if b.version != clparams.Phase0Version {
		panic("can call SetCurrentEpochAtteastations only on phase0")
	}
	b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	b.currentEpochAttestations = attestations
}

func (b *BeaconState) SetPreviousEpochAtteastations(attestations []*cltypes.PendingAttestation) {
	if b.version != clparams.Phase0Version {
		panic("can call SetPreviousEpochAtteastations only on phase0")
	}
	b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	b.previousEpochAttestations = attestations
}
