package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/beacon_changeset"
)

// StartCollectingReverseChangeSet starts collection change sets.
func (b *BeaconState) StartCollectingReverseChangeSet() {
	b.reverseChangeset = &beacon_changeset.ReverseBeaconStateChangeSet{
		BlockRootsChanges:                 beacon_changeset.NewListChangeSet[libcommon.Hash](len(b.blockRoots)),
		StateRootsChanges:                 beacon_changeset.NewListChangeSet[libcommon.Hash](len(b.stateRoots)),
		HistoricalRootsChanges:            beacon_changeset.NewListChangeSet[libcommon.Hash](len(b.historicalRoots)),
		Eth1DataVotesChanges:              beacon_changeset.NewListChangeSet[cltypes.Eth1Data](len(b.eth1DataVotes)),
		BalancesChanges:                   beacon_changeset.NewListChangeSet[uint64](len(b.balances)),
		RandaoMixesChanges:                beacon_changeset.NewListChangeSet[libcommon.Hash](len(b.randaoMixes)),
		SlashingsChanges:                  beacon_changeset.NewListChangeSet[uint64](len(b.slashings)),
		PreviousEpochParticipationChanges: beacon_changeset.NewListChangeSet[cltypes.ParticipationFlags](len(b.previousEpochParticipation)),
		CurrentEpochParticipationChanges:  beacon_changeset.NewListChangeSet[cltypes.ParticipationFlags](len(b.currentEpochParticipation)),
		InactivityScoresChanges:           beacon_changeset.NewListChangeSet[uint64](len(b.inactivityScores)),
		HistoricalSummaryChange:           beacon_changeset.NewListChangeSet[cltypes.HistoricalSummary](len(b.historicalSummaries)),
		// Validators section
		WithdrawalCredentialsChange:      beacon_changeset.NewListChangeSet[libcommon.Hash](len(b.validators)),
		EffectiveBalanceChange:           beacon_changeset.NewListChangeSet[uint64](len(b.validators)),
		ActivationEligibilityEpochChange: beacon_changeset.NewListChangeSet[uint64](len(b.validators)),
		ActivationEpochChange:            beacon_changeset.NewListChangeSet[uint64](len(b.validators)),
		ExitEpochChange:                  beacon_changeset.NewListChangeSet[uint64](len(b.validators)),
		WithdrawalEpochChange:            beacon_changeset.NewListChangeSet[uint64](len(b.validators)),
		SlashedChange:                    beacon_changeset.NewListChangeSet[bool](len(b.validators)),
	}
}

// StopCollectingReverseChangeSet stops collection change sets.
func (b *BeaconState) StopCollectingReverseChangeSet() *beacon_changeset.ReverseBeaconStateChangeSet {
	ret := b.reverseChangeset
	b.reverseChangeset = nil
	return ret
}

func (b *BeaconState) RevertWithChangeset(changeset *beacon_changeset.ReverseBeaconStateChangeSet) {
	changeset.CompactChanges()
	// Updates all single types accordingly.
	if changeset.SlotChange != nil {
		b.slot = *changeset.SlotChange
		b.touchedLeaves[SlotLeafIndex] = true
	}
	if changeset.ForkChange != nil {
		b.fork = changeset.ForkChange
		b.touchedLeaves[ForkLeafIndex] = true
	}
	if changeset.LatestBlockHeaderChange != nil {
		b.latestBlockHeader = changeset.LatestBlockHeaderChange
		b.touchedLeaves[LatestBlockHeaderLeafIndex] = true
	}
	if changeset.Eth1DataChange != nil {
		b.eth1Data = changeset.Eth1DataChange
		b.touchedLeaves[Eth1DataLeafIndex] = true
	}
	if changeset.Eth1DepositIndexChange != nil {
		b.eth1DepositIndex = *changeset.Eth1DepositIndexChange
		b.touchedLeaves[Eth1DepositIndexLeafIndex] = true
	}
	if changeset.JustificationBitsChange != nil {
		b.justificationBits = *changeset.JustificationBitsChange
		b.touchedLeaves[JustificationBitsLeafIndex] = true
	}
	if changeset.PreviousJustifiedCheckpointChange != nil {
		b.previousJustifiedCheckpoint = changeset.PreviousJustifiedCheckpointChange
		b.touchedLeaves[PreviousJustifiedCheckpointLeafIndex] = true
	}
	if changeset.CurrentJustifiedCheckpointChange != nil {
		b.currentJustifiedCheckpoint = changeset.CurrentJustifiedCheckpointChange
		b.touchedLeaves[CurrentJustifiedCheckpointLeafIndex] = true
	}
	if changeset.FinalizedCheckpointChange != nil {
		b.finalizedCheckpoint = changeset.FinalizedCheckpointChange
		b.touchedLeaves[FinalizedCheckpointLeafIndex] = true
	}
	if changeset.CurrentSyncCommitteeChange != nil {
		b.currentSyncCommittee = changeset.CurrentSyncCommitteeChange
		b.touchedLeaves[CurrentSyncCommitteeLeafIndex] = true
	}
	if changeset.NextSyncCommitteeChange != nil {
		b.nextSyncCommittee = changeset.NextSyncCommitteeChange
		b.touchedLeaves[NextSyncCommitteeLeafIndex] = true
	}
	if changeset.LatestBlockHeaderChange != nil {
		b.latestBlockHeader = changeset.LatestBlockHeaderChange
		b.touchedLeaves[LatestBlockHeaderLeafIndex] = true
	}
	if changeset.NextWithdrawalIndexChange != nil {
		b.nextWithdrawalIndex = *changeset.NextWithdrawalIndexChange
		b.touchedLeaves[NextWithdrawalIndexLeafIndex] = true
	}
	if changeset.NextWithdrawalValidatorIndexChange != nil {
		b.nextWithdrawalValidatorIndex = *changeset.NextWithdrawalValidatorIndexChange
		b.touchedLeaves[NextWithdrawalValidatorIndexLeafIndex] = true
	}
	// Process all the lists now.
	// Start with arrays first
	changeset.BlockRootsChanges.ChangesWithHandler(func(value libcommon.Hash, index int) {
		b.blockRoots[index] = value
		b.touchedLeaves[BlockRootsLeafIndex] = true
	})
	changeset.StateRootsChanges.ChangesWithHandler(func(value libcommon.Hash, index int) {
		b.stateRoots[index] = value
		b.touchedLeaves[StateRootsLeafIndex] = true
	})
	changeset.RandaoMixesChanges.ChangesWithHandler(func(value libcommon.Hash, index int) {
		b.randaoMixes[index] = value
		b.touchedLeaves[RandaoMixesLeafIndex] = true
	})
	changeset.SlashingsChanges.ChangesWithHandler(func(value uint64, index int) {
		b.slashings[index] = value
		b.touchedLeaves[SlashingsLeafIndex] = true
	})
	// Process the lists now.
	b.historicalRoots, b.touchedLeaves[HistoricalRootsLeafIndex] = changeset.HistoricalRootsChanges.ApplyChanges(b.historicalRoots)
	// This is a special case, as reset will lead to complete change of votes
	if len(changeset.Eth1DataVotesAtReset) == 0 {
		b.eth1DataVotes, b.touchedLeaves[Eth1DataVotesLeafIndex] = changeset.ApplyEth1DataVotesChanges(b.eth1DataVotes)
	} else {
		b.eth1DataVotes = changeset.Eth1DataVotesAtReset
		b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	}
	b.balances, b.touchedLeaves[BalancesLeafIndex] = changeset.BalancesChanges.ApplyChanges(b.balances)
	// This also a special case, as this is another victim of reset, we use rotation with curr and prev to handle it efficiently
	if len(changeset.PreviousEpochParticipationAtReset) == 0 {
		b.previousEpochParticipation, b.touchedLeaves[PreviousEpochParticipationLeafIndex] = changeset.PreviousEpochParticipationChanges.ApplyChanges(b.previousEpochParticipation)
		b.currentEpochParticipation, b.touchedLeaves[CurrentEpochParticipationLeafIndex] = changeset.CurrentEpochParticipationChanges.ApplyChanges(b.currentEpochParticipation)
	} else {
		b.currentEpochParticipation = b.previousEpochParticipation
		b.previousEpochParticipation = changeset.PreviousEpochParticipationAtReset
	}
	b.inactivityScores, b.touchedLeaves[InactivityScoresLeafIndex] = changeset.InactivityScoresChanges.ApplyChanges(b.inactivityScores)
	b.historicalSummaries, b.touchedLeaves[HistoricalSummariesLeafIndex] = changeset.ApplyHistoricalSummaryChanges(b.historicalSummaries)
	// Now start processing validators if there are any.
	if changeset.HasValidatorSetNotChanged() {
		return
	}
	// We do it like this because validators diff can get quite big so we only save individual fields.
	b.touchedLeaves[ValidatorsLeafIndex] = true
	newValidatorsSet := b.validators
	// All changes habe same length
	if changeset.WithdrawalCredentialsChange.ListLength() != len(b.validators) {
		newValidatorsSet := make([]*cltypes.Validator, len(b.validators))
		copy(newValidatorsSet, b.validators)
	}
	b.validators = newValidatorsSet
	// Now start processing all of the validator fields
	changeset.WithdrawalCredentialsChange.ChangesWithHandler(func(value libcommon.Hash, index int) {
		b.validators[index].WithdrawalCredentials = value
	})
	changeset.ExitEpochChange.ChangesWithHandler(func(value uint64, index int) {
		b.validators[index].ExitEpoch = value
	})
	changeset.ActivationEligibilityEpochChange.ChangesWithHandler(func(value uint64, index int) {
		b.validators[index].ActivationEligibilityEpoch = value
	})
	changeset.ActivationEpochChange.ChangesWithHandler(func(value uint64, index int) {
		b.validators[index].ActivationEpoch = value
	})
	changeset.SlashedChange.ChangesWithHandler(func(value bool, index int) {
		b.validators[index].Slashed = value
	})
	changeset.WithdrawalEpochChange.ChangesWithHandler(func(value uint64, index int) {
		b.validators[index].WithdrawableEpoch = value
	})
	changeset.EffectiveBalanceChange.ChangesWithHandler(func(value uint64, index int) {
		b.validators[index].EffectiveBalance = value
	})
}
