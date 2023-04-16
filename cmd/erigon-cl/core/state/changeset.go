package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/beacon_changeset"
)

// StartCollectingReverseChangeSet starts collection change sets.
func (b *BeaconState) StartCollectingReverseChangeSet() {
	b.reverseChangeset = beacon_changeset.New(len(b.validators), len(b.blockRoots), len(b.stateRoots), len(b.slashings), len(b.historicalSummaries), len(b.historicalRoots), len(b.eth1DataVotes), len(b.randaoMixes), true)
}

// StopCollectingReverseChangeSet stops collection change sets.
func (b *BeaconState) StopCollectingReverseChangeSet() *beacon_changeset.ChangeSet {
	ret := b.reverseChangeset
	b.reverseChangeset = nil
	return ret
}

// StartCollectingForwardChangeSet starts collection change sets.
func (b *BeaconState) StartCollectingForwardChangeSet() {
	b.forwardChangeset = beacon_changeset.New(len(b.validators), len(b.blockRoots), len(b.stateRoots), len(b.slashings), len(b.historicalSummaries), len(b.historicalRoots), len(b.eth1DataVotes), len(b.randaoMixes), false)
}

// StopCollectingForwardChangeSet stops collection change sets.
func (b *BeaconState) StopCollectingForwardChangeSet() *beacon_changeset.ChangeSet {
	ret := b.forwardChangeset
	b.forwardChangeset = nil
	return ret
}

func (b *BeaconState) RevertWithChangeset(changeset *beacon_changeset.ChangeSet) {
	changeset.CompactChanges()
	beforeSlot := b.slot
	var touched bool
	// Updates all single types accordingly.
	if b.slot, touched = changeset.ApplySlotChange(b.slot); touched {
		b.touchedLeaves[SlotLeafIndex] = true
	}
	if b.fork, touched = changeset.ApplyForkChange(b.fork); touched {
		b.touchedLeaves[ForkLeafIndex] = true
	}
	if b.latestBlockHeader, touched = changeset.ApplyLatestBlockHeader(b.latestBlockHeader); touched {
		b.touchedLeaves[LatestBlockHeaderLeafIndex] = true
	}
	if b.eth1Data, touched = changeset.ApplyEth1DataChange(b.eth1Data); touched {
		b.touchedLeaves[Eth1DataLeafIndex] = true
	}
	if b.eth1DepositIndex, touched = changeset.ApplyEth1DepositIndexChange(b.eth1DepositIndex); touched {
		b.touchedLeaves[Eth1DepositIndexLeafIndex] = true
	}
	if b.justificationBits, touched = changeset.ApplyJustificationBitsChange(b.justificationBits); touched {
		b.touchedLeaves[JustificationBitsLeafIndex] = true
	}
	if b.previousJustifiedCheckpoint, touched = changeset.ApplyPreviousJustifiedCheckpointChange(b.previousJustifiedCheckpoint); touched {
		b.touchedLeaves[PreviousJustifiedCheckpointLeafIndex] = true
	}
	if b.currentJustifiedCheckpoint, touched = changeset.ApplyCurrentJustifiedCheckpointChange(b.currentJustifiedCheckpoint); touched {
		b.touchedLeaves[CurrentJustifiedCheckpointLeafIndex] = true
	}
	if b.finalizedCheckpoint, touched = changeset.ApplyFinalizedCheckpointChange(b.finalizedCheckpoint); touched {
		b.touchedLeaves[FinalizedCheckpointLeafIndex] = true
	}
	if b.currentSyncCommittee, touched = changeset.ApplyCurrentSyncCommitteeChange(b.currentSyncCommittee); touched {
		b.touchedLeaves[CurrentSyncCommitteeLeafIndex] = true
	}
	if b.nextSyncCommittee, touched = changeset.ApplyNextSyncCommitteeChange(b.nextSyncCommittee); touched {
		b.touchedLeaves[NextSyncCommitteeLeafIndex] = true
	}
	if b.version >= clparams.BellatrixVersion {
		if b.latestExecutionPayloadHeader, touched = changeset.ApplyLatestExecutionPayloadHeaderChange(b.latestExecutionPayloadHeader); touched {
			b.touchedLeaves[LatestExecutionPayloadHeaderLeafIndex] = true
		}
	}
	if b.version >= clparams.CapellaVersion {
		if b.nextWithdrawalIndex, touched = changeset.ApplyNextWithdrawalIndexChange(b.nextWithdrawalIndex); touched {
			b.touchedLeaves[NextWithdrawalIndexLeafIndex] = true
		}
		if b.nextWithdrawalValidatorIndex, touched = changeset.ApplyNextValidatorWithdrawalIndexChange(b.nextWithdrawalValidatorIndex); touched {
			b.touchedLeaves[NextWithdrawalValidatorIndexLeafIndex] = true
		}
	}
	if b.version, touched = changeset.ApplyVersionChange(b.version); touched {
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
	if b.historicalRoots, touched = changeset.HistoricalRootsChanges.ApplyChanges(b.historicalRoots); touched {
		b.touchedLeaves[HistoricalRootsLeafIndex] = true
	}

	// Process votes changes
	if b.eth1DataVotes, touched = changeset.ApplyEth1DataVotesChanges(b.eth1DataVotes); touched {
		b.touchedLeaves[Eth1DataVotesLeafIndex] = true
	}
	if b.balances, touched = changeset.BalancesChanges.ApplyChanges(b.balances); touched {
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
	if b.inactivityScores, touched = changeset.InactivityScoresChanges.ApplyChanges(b.inactivityScores); touched {
		b.touchedLeaves[InactivityScoresLeafIndex] = true
	}

	if b.historicalSummaries, touched = changeset.ApplyHistoricalSummaryChanges(b.historicalSummaries); touched {
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
	newValidatorSetLength := changeset.WithdrawalCredentialsChange.ListLength()
	if newValidatorSetLength < len(b.validators) {
		for _, removedValidator := range b.validators[newValidatorSetLength:] {
			delete(b.publicKeyIndicies, removedValidator.PublicKey)
		}
	}
	if newValidatorSetLength != len(b.validators) {
		newValidatorsSet = make([]*cltypes.Validator, newValidatorSetLength)
		copy(newValidatorsSet, b.validators)
	}
	b.validators = newValidatorsSet
	// Now start processing all of the validator fields
	changeset.PublicKeyChanges.ChangesWithHandler(func(key [48]byte, index int) {
		if index >= len(b.validators) {
			return
		}
		if b.validators[index] == nil {
			b.validators[index] = new(cltypes.Validator)
		}
		b.validators[index].PublicKey = key
	})
	changeset.WithdrawalCredentialsChange.ChangesWithHandler(func(value libcommon.Hash, index int) {
		if index >= len(b.validators) {
			return
		}
		if b.validators[index] == nil {
			b.validators[index] = new(cltypes.Validator)
		}
		b.validators[index].WithdrawalCredentials = value
	})
	changeset.ExitEpochChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		if b.validators[index] == nil {
			b.validators[index] = new(cltypes.Validator)
		}
		b.validators[index].ExitEpoch = value
	})
	changeset.ActivationEligibilityEpochChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		if b.validators[index] == nil {
			b.validators[index] = new(cltypes.Validator)
		}
		b.validators[index].ActivationEligibilityEpoch = value
	})
	changeset.ActivationEpochChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		if b.validators[index] == nil {
			b.validators[index] = new(cltypes.Validator)
		}
		b.validators[index].ActivationEpoch = value
	})
	changeset.SlashedChange.ChangesWithHandler(func(value bool, index int) {
		if index >= len(b.validators) {
			return
		}
		if b.validators[index] == nil {
			b.validators[index] = new(cltypes.Validator)
		}
		b.validators[index].Slashed = value
	})
	changeset.WithdrawalEpochChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		if b.validators[index] == nil {
			b.validators[index] = new(cltypes.Validator)
		}
		b.validators[index].WithdrawableEpoch = value
	})
	changeset.EffectiveBalanceChange.ChangesWithHandler(func(value uint64, index int) {
		if index >= len(b.validators) {
			return
		}
		if b.validators[index] == nil {
			b.validators[index] = new(cltypes.Validator)
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
	if epoch < beforeEpoch {
		return
	}
	b.committeeCache.Purge()
	for epochToBeRemoved := beforeEpoch; epochToBeRemoved < epoch+1; epochToBeRemoved++ {
		b.activeValidatorsCache.Remove(epochToBeRemoved)
		b.shuffledSetsCache.Remove(b.GetSeed(epochToBeRemoved, b.beaconConfig.DomainBeaconAttester))
		b.activeValidatorsCache.Remove(epochToBeRemoved)
	}
}
