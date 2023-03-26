package beacon_changeset

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// This type of changeset is the diff beetwen next state and input state and is used to reverse beacon state.
// It does not work the other way around. So they apply [curr state] + [reverse change set] = [prev state]
type ReverseBeaconStateChangeSet struct {
	// Single types.
	SlotChange                         *uint64
	ForkChange                         *cltypes.Fork
	LatestBlockHeaderChange            *cltypes.BeaconBlockHeader
	Eth1DataChange                     *cltypes.Eth1Data
	Eth1DepositIndexChange             *uint64
	JustificationBitsChange            *cltypes.JustificationBits
	PreviousJustifiedCheckpointChange  *cltypes.Checkpoint
	CurrentJustifiedCheckpointChange   *cltypes.Checkpoint
	FinalizedCheckpointChange          *cltypes.Checkpoint
	CurrentSyncCommitteeChange         *cltypes.SyncCommittee
	NextSyncCommitteeChange            *cltypes.SyncCommittee
	LatestExecutionPayloadHeaderChange *cltypes.Eth1Header
	NextWithdrawalIndexChange          *uint64
	NextWithdrawalValidatorIndexChange *uint64
	VersionChange                      *clparams.StateVersion
	// Lists and arrays changesets
	BlockRootsChanges                 *ListChangeSet[libcommon.Hash]
	StateRootsChanges                 *ListChangeSet[libcommon.Hash]
	HistoricalRootsChanges            *ListChangeSet[libcommon.Hash]
	Eth1DataVotesChanges              *ListChangeSet[cltypes.Eth1Data]
	BalancesChanges                   *ListChangeSet[uint64]
	RandaoMixesChanges                *ListChangeSet[libcommon.Hash]
	SlashingsChanges                  *ListChangeSet[uint64]
	PreviousEpochParticipationChanges *ListChangeSet[cltypes.ParticipationFlags]
	CurrentEpochParticipationChanges  *ListChangeSet[cltypes.ParticipationFlags]
	InactivityScoresChanges           *ListChangeSet[uint64]
	HistoricalSummaryChange           *ListChangeSet[cltypes.HistoricalSummary]
	// Validator fields.
	WithdrawalCredentialsChange      *ListChangeSet[libcommon.Hash]
	EffectiveBalanceChange           *ListChangeSet[uint64]
	SlashedChange                    *ListChangeSet[bool]
	ActivationEligibilityEpochChange *ListChangeSet[uint64]
	ActivationEpochChange            *ListChangeSet[uint64]
	ExitEpochChange                  *ListChangeSet[uint64]
	WithdrawalEpochChange            *ListChangeSet[uint64]
	// Efficient unwinding on reset (only applicable at epoch boundaries)
	PreviousEpochParticipationAtReset cltypes.ParticipationFlagsList
	CurrentEpochParticipationAtReset  cltypes.ParticipationFlagsList
	Eth1DataVotesAtReset              []*cltypes.Eth1Data
}

func (r *ReverseBeaconStateChangeSet) OnSlotChange(prevSlot uint64) {
	if r.SlotChange != nil {
		return
	}
	r.SlotChange = new(uint64)
	*r.SlotChange = prevSlot
}

func (r *ReverseBeaconStateChangeSet) OnForkChange(fork *cltypes.Fork) {
	if r.ForkChange != nil {
		return
	}
	r.ForkChange = new(cltypes.Fork)
	*r.ForkChange = *fork
}

func (r *ReverseBeaconStateChangeSet) OnLatestHeaderChange(h *cltypes.BeaconBlockHeader) {
	if r.LatestBlockHeaderChange != nil {
		return
	}
	r.LatestBlockHeaderChange = new(cltypes.BeaconBlockHeader)
	*r.LatestBlockHeaderChange = *h
}

func (r *ReverseBeaconStateChangeSet) OnEth1DataChange(e *cltypes.Eth1Data) {
	if r.LatestBlockHeaderChange != nil {
		return
	}
	r.Eth1DataChange = new(cltypes.Eth1Data)
	*r.Eth1DataChange = *e
}

func (r *ReverseBeaconStateChangeSet) OnJustificationBitsChange(j cltypes.JustificationBits) {
	if r.JustificationBitsChange != nil {
		return
	}
	r.JustificationBitsChange = new(cltypes.JustificationBits)
	*r.JustificationBitsChange = j.Copy()
}

func (r *ReverseBeaconStateChangeSet) OnEth1DepositIndexChange(e uint64) {
	if r.Eth1DepositIndexChange != nil {
		return
	}
	r.Eth1DepositIndexChange = new(uint64)
	*r.Eth1DepositIndexChange = e
}

func (r *ReverseBeaconStateChangeSet) OnPreviousJustifiedCheckpointChange(c *cltypes.Checkpoint) {
	if r.PreviousJustifiedCheckpointChange != nil {
		return
	}
	r.PreviousJustifiedCheckpointChange = new(cltypes.Checkpoint)
	*r.PreviousJustifiedCheckpointChange = *c
}

func (r *ReverseBeaconStateChangeSet) OnCurrentJustifiedCheckpointChange(c *cltypes.Checkpoint) {
	if r.CurrentJustifiedCheckpointChange != nil {
		return
	}
	r.CurrentJustifiedCheckpointChange = new(cltypes.Checkpoint)
	*r.CurrentJustifiedCheckpointChange = *c
}

func (r *ReverseBeaconStateChangeSet) OnFinalizedCheckpointChange(c *cltypes.Checkpoint) {
	if r.FinalizedCheckpointChange != nil {
		return
	}
	r.FinalizedCheckpointChange = new(cltypes.Checkpoint)
	*r.FinalizedCheckpointChange = *c
}

func (r *ReverseBeaconStateChangeSet) OnCurrentSyncCommitteeChange(c *cltypes.SyncCommittee) {
	if r.CurrentSyncCommitteeChange != nil {
		return
	}
	r.CurrentSyncCommitteeChange = new(cltypes.SyncCommittee)
	*r.CurrentSyncCommitteeChange = *c
}

func (r *ReverseBeaconStateChangeSet) OnNextSyncCommitteeChange(c *cltypes.SyncCommittee) {
	if r.NextSyncCommitteeChange != nil {
		return
	}
	r.NextSyncCommitteeChange = new(cltypes.SyncCommittee)
	*r.NextSyncCommitteeChange = *c
}

func (r *ReverseBeaconStateChangeSet) OnEth1Header(e *cltypes.Eth1Header) {
	if r.LatestExecutionPayloadHeaderChange != nil {
		return
	}
	r.LatestExecutionPayloadHeaderChange = new(cltypes.Eth1Header)
	*r.LatestExecutionPayloadHeaderChange = *e
}

func (r *ReverseBeaconStateChangeSet) OnNextWithdrawalIndexChange(index uint64) {
	if r.NextWithdrawalIndexChange != nil {
		return
	}
	r.NextWithdrawalIndexChange = new(uint64)
	*r.NextWithdrawalIndexChange = index
}

func (r *ReverseBeaconStateChangeSet) OnNextWithdrawalValidatorIndexChange(index uint64) {
	if r.NextWithdrawalValidatorIndexChange != nil {
		return
	}
	r.NextWithdrawalValidatorIndexChange = new(uint64)
	*r.NextWithdrawalValidatorIndexChange = index
}

func (r *ReverseBeaconStateChangeSet) OnVersionChange(v clparams.StateVersion) {
	if r.VersionChange != nil {
		return
	}
	r.VersionChange = new(clparams.StateVersion)
	*r.VersionChange = v
}

func (r *ReverseBeaconStateChangeSet) HasValidatorSetNotChanged(validatorSetLength int) bool {
	return validatorSetLength == r.WithdrawalCredentialsChange.ListLength() && r.WithdrawalCredentialsChange.Empty() && r.ActivationEligibilityEpochChange.Empty() && r.ActivationEpochChange.Empty() &&
		r.EffectiveBalanceChange.Empty() && r.SlashedChange.Empty() && r.ExitEpochChange.Empty() && r.WithdrawalEpochChange.Empty()
}

func (r *ReverseBeaconStateChangeSet) ApplyEth1DataVotesChanges(input []*cltypes.Eth1Data) (output []*cltypes.Eth1Data, changed bool) {
	output = input
	if r.Eth1DataVotesChanges.Empty() && r.Eth1DataVotesChanges.ListLength() == len(output) {
		return
	}
	changed = true
	if r.Eth1DataVotesChanges.ListLength() != len(output) {
		output = make([]*cltypes.Eth1Data, r.Eth1DataVotesChanges.ListLength())
		copy(output, input)
	}
	r.Eth1DataVotesChanges.ChangesWithHandler(func(value cltypes.Eth1Data, index int) {
		*output[index] = value
	})
	return
}

func (r *ReverseBeaconStateChangeSet) ApplyHistoricalSummaryChanges(input []*cltypes.HistoricalSummary) (output []*cltypes.HistoricalSummary, changed bool) {
	output = input
	if r.HistoricalSummaryChange.Empty() && r.Eth1DataVotesChanges.ListLength() == len(output) {
		return
	}
	changed = true
	historicalSummarryLength := r.HistoricalSummaryChange.ListLength()
	if historicalSummarryLength != len(output) {
		output = make([]*cltypes.HistoricalSummary, historicalSummarryLength)
		copy(output, input)
	}
	r.HistoricalSummaryChange.ChangesWithHandler(func(value cltypes.HistoricalSummary, index int) {
		*output[index] = value
	})
	return
}

func (r *ReverseBeaconStateChangeSet) CompactChanges() {

	r.BlockRootsChanges.CompactChangesReverse()
	r.StateRootsChanges.CompactChangesReverse()
	r.HistoricalRootsChanges.CompactChangesReverse()
	r.SlashingsChanges.CompactChangesReverse()
	r.RandaoMixesChanges.CompactChangesReverse()
	r.BalancesChanges.CompactChangesReverse()
	if len(r.Eth1DataVotesAtReset) > 0 {
		r.Eth1DataVotesChanges = nil
	} else {
		r.Eth1DataVotesChanges.CompactChangesReverse()
	}
	if len(r.PreviousEpochParticipationAtReset) > 0 {
		r.PreviousEpochParticipationChanges = nil
		r.CurrentEpochParticipationChanges = nil
	} else {
		r.PreviousEpochParticipationChanges.CompactChangesReverse()
		r.CurrentEpochParticipationChanges.CompactChangesReverse()
	}
	r.InactivityScoresChanges.CompactChangesReverse()
	r.HistoricalRootsChanges.CompactChangesReverse()
	r.WithdrawalCredentialsChange.CompactChangesReverse()
	r.EffectiveBalanceChange.CompactChangesReverse()
	r.ExitEpochChange.CompactChangesReverse()
	r.ActivationEligibilityEpochChange.CompactChangesReverse()
	r.ActivationEpochChange.CompactChangesReverse()
	r.SlashedChange.CompactChangesReverse()
	r.WithdrawalEpochChange.CompactChangesReverse()
}
