package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
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
	beforeSlot := b.slot
	var touched bool
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
	if changeset.LatestExecutionPayloadHeaderChange != nil {
		b.latestExecutionPayloadHeader = changeset.LatestExecutionPayloadHeaderChange
		b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
	}
	if changeset.NextWithdrawalIndexChange != nil {
		b.nextWithdrawalIndex = *changeset.NextWithdrawalIndexChange
		b.touchedLeaves[NextWithdrawalIndexLeafIndex] = true
	}
	if changeset.NextWithdrawalValidatorIndexChange != nil {
		b.nextWithdrawalValidatorIndex = *changeset.NextWithdrawalValidatorIndexChange
		b.touchedLeaves[NextWithdrawalValidatorIndexLeafIndex] = true
	}
	if changeset.VersionChange != nil {
		b.version = *changeset.VersionChange
		if b.version == clparams.AltairVersion {
			b.leaves[LatestExecutionPayloadHeaderLeafIndex] = libcommon.Hash{}
			b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = false
		}
		if b.version == clparams.BellatrixVersion {
			b.leaves[NextWithdrawalIndexLeafIndex] = libcommon.Hash{}
			b.leaves[NextWithdrawalValidatorIndexLeafIndex] = libcommon.Hash{}
			b.leaves[HistoricalSummariesLeafIndex] = libcommon.Hash{}
			b.touchedLeaves[NextWithdrawalIndexLeafIndex] = false
			b.touchedLeaves[NextWithdrawalValidatorIndexLeafIndex] = false
			b.touchedLeaves[HistoricalSummariesLeafIndex] = false
		}
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
	b.historicalRoots, touched = changeset.HistoricalRootsChanges.ApplyChanges(b.historicalRoots)
	if touched {
		b.touchedLeaves[HistoricalRootsLeafIndex] = true
	}
	// Process votes changes
	b.eth1DataVotes, touched = changeset.ApplyEth1DataVotesChanges(b.eth1DataVotes)
	if touched {
		b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	}
	b.balances, touched = changeset.BalancesChanges.ApplyChanges(b.balances)
	if touched {
		b.touchedLeaves[BalancesLeafIndex] = true
	}
	// Process epoch participation changes
	var touchedPreviousEpochParticipation, touchedCurrentEpochParticipation bool
	b.previousEpochParticipation, b.currentEpochParticipation,
		touchedPreviousEpochParticipation,
		touchedCurrentEpochParticipation = changeset.ApplyEpochParticipationChanges(b.previousEpochParticipation, b.currentEpochParticipation)
	if touchedPreviousEpochParticipation {
		b.touchedLeaves[PreviousEpochParticipationLeafIndex] = true
	}
	if touchedCurrentEpochParticipation {
		b.touchedLeaves[CurrentEpochParticipationLeafIndex] = true
	}
	// Process inactivity scores changes.
	b.inactivityScores, touched = changeset.InactivityScoresChanges.ApplyChanges(b.inactivityScores)
	if touched {
		b.touchedLeaves[InactivityScoresLeafIndex] = true
	}
	b.historicalSummaries, touched = changeset.ApplyHistoricalSummaryChanges(b.historicalSummaries)
	if touched {
		b.touchedLeaves[HistoricalSummariesLeafIndex] = true
	}
	// Now start processing validators if there are any.
	if changeset.HasValidatorSetNotChanged(len(b.validators)) {
		b.revertCachesOnBoundary(beforeSlot)
		return
	}
	// We do it like this because validators diff can get quite big so we only save individual fields.
	b.touchedLeaves[ValidatorsLeafIndex] = true
	newValidatorsSet := b.validators
	// All changes habe same length
	previousValidatorSetLength := changeset.WithdrawalCredentialsChange.ListLength()

	if previousValidatorSetLength != len(b.validators) {
		for _, removedValidator := range b.validators[previousValidatorSetLength:] {
			delete(b.publicKeyIndicies, removedValidator.PublicKey)
		}
		newValidatorsSet = make([]*cltypes.Validator, previousValidatorSetLength)
		copy(newValidatorsSet, b.validators)
	}
	b.validators = newValidatorsSet
	// Now start processing all of the validator fields
	changeset.WithdrawalCredentialsChange.ChangesWithHandler(func(value libcommon.Hash, index int) {
		if index >= len(b.validators) {
			return
		}
		b.validators[index].WithdrawalCredentials = value
	})
	changeset.ExitEpochChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		b.validators[index].ExitEpoch = value
	})
	changeset.ActivationEligibilityEpochChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		b.validators[index].ActivationEligibilityEpoch = value
	})
	changeset.ActivationEpochChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		b.validators[index].ActivationEpoch = value
	})
	changeset.SlashedChange.ChangesWithHandler(func(value bool, index int) {
		if index >= len(b.validators) {
			return
		}
		b.validators[index].Slashed = value
	})
	changeset.WithdrawalEpochChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		b.validators[index].WithdrawableEpoch = value
	})
	changeset.EffectiveBalanceChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		b.validators[index].EffectiveBalance = value
	})
	b.revertCachesOnBoundary(beforeSlot)
}

func (b *BeaconState) revertCachesOnBoundary(beforeSlot uint64) {
	beforeEpoch := beforeSlot / b.beaconConfig.SlotsPerEpoch
	epoch := b.Epoch()
	b.previousStateRoot = libcommon.Hash{}
	b.proposerIndex = nil
	b.totalActiveBalanceCache = nil
	if epoch <= beforeEpoch {
		return
	}
	b.totalActiveBalanceCache = nil
	b.committeeCache.Purge()
	for epochToBeRemoved := beforeEpoch; epochToBeRemoved < epoch+1; beforeEpoch++ {
		b.activeValidatorsCache.Remove(epochToBeRemoved)
		b.shuffledSetsCache.Remove(b.GetSeed(epochToBeRemoved, b.beaconConfig.DomainBeaconAttester))
		b.activeValidatorsCache.Remove(epochToBeRemoved)
	}
}
