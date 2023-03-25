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
