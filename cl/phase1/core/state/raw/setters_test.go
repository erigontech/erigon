// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package raw

import (
	"errors"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBeaconState_SetVersion(t *testing.T) {
	state := GetTestState()
	state.SetVersion(clparams.Phase0Version)
	assert.Equal(t, clparams.Phase0Version, state.Version())
}

func TestBeaconState_SetSlot(t *testing.T) {
	state := GetTestState()
	slot := uint64(12345)
	require.NoError(t, state.SetSlot(slot))
	assert.Equal(t, slot, state.slot)
}

func TestBeaconState_SetBlockRootAt(t *testing.T) {
	state := GetTestState()
	index := 0
	root := common.HexToHash("0x1234567890abcdef")
	require.NoError(t, state.SetBlockRootAt(index, root))
	assert.Equal(t, root, state.blockRoots.Get(index))
}

func TestBeaconState_SetStateRootAt(t *testing.T) {
	state := GetTestState()
	index := 0
	root := common.HexToHash("0xabcdef1234567890")
	require.NoError(t, state.SetStateRootAt(index, root))
	assert.Equal(t, root, state.stateRoots.Get(index))
}

func TestBeaconState_SetWithdrawalCredentialForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	creds := common.HexToHash("0xabcdef1234567890")
	require.NoError(t, state.SetWithdrawalCredentialForValidatorAtIndex(index, creds))
	assert.Equal(t, creds, state.validators.Get(index).WithdrawalCredentials())
}

func TestBeaconState_SetExitEpochForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	epoch := uint64(10)
	require.NoError(t, state.SetExitEpochForValidatorAtIndex(index, epoch))
	assert.Equal(t, epoch, state.validators.Get(index).ExitEpoch())
}

func TestBeaconState_SetWithdrawableEpochForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	epoch := uint64(5)
	err := state.SetWithdrawableEpochForValidatorAtIndex(index, epoch)
	require.NoError(t, err)
	assert.Equal(t, epoch, state.validators.Get(index).WithdrawableEpoch())
}

func TestBeaconState_SetWithdrawableEpochForValidatorAtIndex_InvalidIndex(t *testing.T) {
	state := GetTestState()
	index := 10000000000000
	epoch := uint64(5)
	err := state.SetWithdrawableEpochForValidatorAtIndex(index, epoch)
	assert.Error(t, err)
}

func TestBeaconState_SetEffectiveBalanceForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	balance := uint64(1000)
	require.NoError(t, state.SetEffectiveBalanceForValidatorAtIndex(index, balance))
	assert.Equal(t, balance, state.validators.Get(index).EffectiveBalance())
}

func TestBeaconState_SetActivationEpochForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	epoch := uint64(5)
	require.NoError(t, state.SetActivationEpochForValidatorAtIndex(index, epoch))
	assert.Equal(t, epoch, state.validators.Get(index).ActivationEpoch())
}

func TestBeaconState_SetActivationEligibilityEpochForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	epoch := uint64(10)
	require.NoError(t, state.SetActivationEligibilityEpochForValidatorAtIndex(index, epoch))
	assert.Equal(t, epoch, state.validators.Get(index).ActivationEligibilityEpoch())
}

func TestBeaconState_SetEth1Data(t *testing.T) {
	state := GetTestState()
	eth1Data := &cltypes.Eth1Data{
		Root:         common.HexToHash("0xabcdef1234567890"),
		DepositCount: 100,
		BlockHash:    common.HexToHash("0x1234567890abcdef"),
	}
	state.SetEth1Data(eth1Data)
	assert.Equal(t, eth1Data, state.eth1Data)
}

func TestBeaconState_AddEth1DataVote(t *testing.T) {
	state := GetTestState()
	vote1 := &cltypes.Eth1Data{
		Root:         common.HexToHash("0xabcdef1234567890"),
		DepositCount: 100,
		BlockHash:    common.HexToHash("0x1234567890abcdef"),
	}
	vote2 := &cltypes.Eth1Data{
		Root:         common.HexToHash("0x1234567890abcdef"),
		DepositCount: 200,
		BlockHash:    common.HexToHash("0xabcdef1234567890"),
	}
	require.NoError(t, state.AddEth1DataVote(vote1))
	require.NoError(t, state.AddEth1DataVote(vote2))
	assert.Equal(t, 2, state.Eth1DataVotes().Len())
}

func TestBeaconState_ResetEth1DataVotes(t *testing.T) {
	state := GetTestState()
	vote1 := &cltypes.Eth1Data{
		Root:         common.HexToHash("0xabcdef1234567890"),
		DepositCount: 100,
		BlockHash:    common.HexToHash("0x1234567890abcdef"),
	}
	require.NoError(t, state.AddEth1DataVote(vote1))
	state.ResetEth1DataVotes()
	assert.Zero(t, state.eth1DataVotes.Len())
}

func TestBeaconState_SetEth1DepositIndex(t *testing.T) {
	state := GetTestState()
	depositIndex := uint64(1000)
	state.SetEth1DepositIndex(depositIndex)
	assert.Equal(t, depositIndex, state.eth1DepositIndex)
}

func TestBeaconState_SetValidatorSlashed(t *testing.T) {
	state := GetTestState()
	index := 0
	slashed := true
	err := state.SetValidatorSlashed(index, slashed)
	require.NoError(t, err)
	assert.Equal(t, slashed, state.validators.Get(index).Slashed())
}

func TestBeaconState_SetValidatorSlashed_InvalidIndex(t *testing.T) {
	state := GetTestState()
	index := 10
	slashed := true
	err := state.SetValidatorSlashed(index, slashed)
	assert.NoError(t, err)
}

func TestBeaconState_SetValidatorMinCurrentInclusionDelayAttestation(t *testing.T) {
	state := GetTestState()
	index := 0
	//value := solid.NewPendingAttestionFromParameters(nil, solid.NewAttestationData(), 123, 3)
	value := &solid.PendingAttestation{
		AggregationBits: solid.NewBitList(0, 2048),
		InclusionDelay:  123,
		ProposerIndex:   3,
	}

	err := state.SetValidatorMinCurrentInclusionDelayAttestation(index, value)
	require.NoError(t, err)
	assert.Equal(t, value, state.validators.MinCurrentInclusionDelayAttestation(index))
}

func TestBeaconState_SetValidatorIsCurrentMatchingSourceAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsCurrentMatchingSourceAttester(index, value)
	require.NoError(t, err)
	assert.Equal(t, value, state.validators.IsCurrentMatchingSourceAttester(index))
}

func TestBeaconState_SetValidatorIsCurrentMatchingTargetAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsCurrentMatchingTargetAttester(index, value)
	require.NoError(t, err)
	assert.Equal(t, value, state.validators.IsCurrentMatchingTargetAttester(index))
}

func TestBeaconState_SetValidatorIsCurrentMatchingHeadAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsCurrentMatchingHeadAttester(index, value)
	require.NoError(t, err)
	assert.Equal(t, value, state.validators.IsCurrentMatchingHeadAttester(index))
}

func TestBeaconState_SetValidatorMinPreviousInclusionDelayAttestation(t *testing.T) {
	state := GetTestState()
	index := 0
	value := &solid.PendingAttestation{
		AggregationBits: solid.NewBitList(0, 2048),
		InclusionDelay:  123,
		ProposerIndex:   3,
	}
	err := state.SetValidatorMinPreviousInclusionDelayAttestation(index, value)
	require.NoError(t, err)
	assert.Equal(t, value, state.validators.MinPreviousInclusionDelayAttestation(index))
}

func TestBeaconState_SetValidatorIsPreviousMatchingSourceAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsPreviousMatchingSourceAttester(index, value)
	require.NoError(t, err)
	assert.Equal(t, value, state.validators.IsPreviousMatchingSourceAttester(index))
}

func TestBeaconState_SetValidatorIsPreviousMatchingTargetAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsPreviousMatchingTargetAttester(index, value)
	require.NoError(t, err)
	assert.Equal(t, value, state.validators.IsPreviousMatchingTargetAttester(index))
}

func TestBeaconState_SetNextWithdrawalValidatorIndex(t *testing.T) {
	state := GetTestState()
	index := uint64(100)
	state.SetNextWithdrawalValidatorIndex(index)
	assert.Equal(t, index, state.nextWithdrawalValidatorIndex)
}

func TestBeaconState_ResetHistoricalSummaries(t *testing.T) {
	state := GetTestState()
	summary := &cltypes.HistoricalSummary{}
	state.AddHistoricalSummary(summary)
	state.ResetHistoricalSummaries()
	assert.Zero(t, state.historicalSummaries.Len())
}

func TestBeaconState_AddHistoricalSummary(t *testing.T) {
	state := GetTestState()
	summary1 := &cltypes.HistoricalSummary{}
	summary2 := &cltypes.HistoricalSummary{}
	state.AddHistoricalSummary(summary1)
	state.AddHistoricalSummary(summary2)
	assert.Equal(t, 2, state.historicalSummaries.Len())
}

func TestBeaconState_AddHistoricalRoot(t *testing.T) {
	state := GetTestState()
	root1 := common.HexToHash("0xabcdef1234567890")
	root2 := common.HexToHash("0x1234567890abcdef")
	state.AddHistoricalRoot(root1)
	state.AddHistoricalRoot(root2)
	assert.Equal(t, 2, state.historicalRoots.Length())
}

func TestBeaconState_SetInactivityScores(t *testing.T) {
	state := GetTestState()
	scores := []uint64{100, 200, 300}
	state.SetInactivityScores(scores)
	assert.Equal(t, 3, state.inactivityScores.Length())
}

func TestBeaconState_AddInactivityScore(t *testing.T) {
	state := GetTestState()
	score1 := uint64(100)
	score2 := uint64(200)
	state.AddInactivityScore(score1)
	state.AddInactivityScore(score2)
	assert.Equal(t, 514, state.inactivityScores.Length())
}

func TestBeaconState_SetValidatorInactivityScore(t *testing.T) {
	state := GetTestState()
	err := state.SetValidatorInactivityScore(0, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), state.inactivityScores.Get(0))
}

func TestBeaconState_SetValidatorInactivityScore_InvalidIndex(t *testing.T) {
	state := GetTestState()
	index := 100000000
	score := uint64(100)
	err := state.SetValidatorInactivityScore(index, score)
	assert.Error(t, err)
}

func TestBeaconState_SetValidatorIsPreviousMatchingHeadAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsPreviousMatchingHeadAttester(index, value)
	require.NoError(t, err)
	assert.Equal(t, value, state.validators.IsPreviousMatchingHeadAttester(index))
}

func TestBeaconState_SetValidatorBalance(t *testing.T) {
	state := GetTestState()
	index := 0
	balance := uint64(1000)
	err := state.SetValidatorBalance(index, balance)
	require.NoError(t, err)
	assert.Equal(t, balance, state.balances.Get(index))
}

func TestBeaconState_AddValidator(t *testing.T) {
	state := GetTestState()
	validator := solid.NewValidator()
	balance := uint64(1000)
	require.NoError(t, state.AddValidator(validator, balance))
	assert.Equal(t, state.balances.Length(), state.validators.Length())
}

func TestBeaconState_SetRandaoMixAt(t *testing.T) {
	state := GetTestState()
	index := 0
	mix := common.HexToHash("0xabcdef1234567890")
	require.NoError(t, state.SetRandaoMixAt(index, mix))
	assert.Equal(t, mix, state.randaoMixes.Get(index))
}

func TestBeaconState_SetSlashingSegmentAt(t *testing.T) {
	state := GetTestState()
	index := 0
	segment := uint64(100)
	require.NoError(t, state.SetSlashingSegmentAt(index, segment))
	assert.Equal(t, segment, state.slashings.Get(index))
}

func TestBeaconState_SetEpochParticipationForValidatorIndex(t *testing.T) {
	state := GetTestState()
	isCurrentEpoch := true
	index := 0
	state.SetEpochParticipationForValidatorIndex(isCurrentEpoch, index, cltypes.ParticipationFlags(1))
	assert.Equal(t, uint8(1), state.currentEpochParticipation.Get(index))
}

func TestBeaconState_SetValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	validator := solid.NewValidator()
	state.SetValidatorAtIndex(index, validator)
	assert.Equal(t, validator, state.validators.Get(index))
}

// TestValidatorSetterLeavesLeafCleanOnHookError pins that a setter which aborts
// on an event-hook error does not leave ValidatorsLeafIndex dirty: the state
// would then re-hash the whole validators subtree for a write that never landed.
func TestValidatorSetterLeavesLeafCleanOnHookError(t *testing.T) {
	hookErr := errors.New("hook failed")
	cases := []struct {
		name   string
		events Events
		set    func(*BeaconState) error
	}{
		{
			name:   "WithdrawalCredential",
			events: Events{OnNewValidatorWithdrawalCredentials: func(int, []byte) error { return hookErr }},
			set:    func(b *BeaconState) error { return b.SetWithdrawalCredentialForValidatorAtIndex(0, common.Hash{1}) },
		},
		{
			name:   "ExitEpoch",
			events: Events{OnNewValidatorExitEpoch: func(int, uint64) error { return hookErr }},
			set:    func(b *BeaconState) error { return b.SetExitEpochForValidatorAtIndex(0, 1) },
		},
		{
			name:   "WithdrawableEpoch",
			events: Events{OnNewValidatorWithdrawableEpoch: func(int, uint64) error { return hookErr }},
			set:    func(b *BeaconState) error { return b.SetWithdrawableEpochForValidatorAtIndex(0, 1) },
		},
		{
			name:   "EffectiveBalance",
			events: Events{OnNewValidatorEffectiveBalance: func(int, uint64) error { return hookErr }},
			set:    func(b *BeaconState) error { return b.SetEffectiveBalanceForValidatorAtIndex(0, 1) },
		},
		{
			name:   "ActivationEpoch",
			events: Events{OnNewValidatorActivationEpoch: func(int, uint64) error { return hookErr }},
			set:    func(b *BeaconState) error { return b.SetActivationEpochForValidatorAtIndex(0, 1) },
		},
		{
			name:   "ActivationEligibilityEpoch",
			events: Events{OnNewValidatorActivationEligibilityEpoch: func(int, uint64) error { return hookErr }},
			set:    func(b *BeaconState) error { return b.SetActivationEligibilityEpochForValidatorAtIndex(0, 1) },
		},
		{
			name:   "Slashed",
			events: Events{OnNewValidatorSlashed: func(int, bool) error { return hookErr }},
			set:    func(b *BeaconState) error { return b.SetValidatorSlashed(0, true) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := GetTestState()
			_, err := state.HashSSZ()
			require.NoError(t, err)
			require.False(t, state.isLeafDirty(ValidatorsLeafIndex))

			state.SetEvents(tc.events)
			require.ErrorIs(t, tc.set(state), hookErr)
			assert.False(t, state.isLeafDirty(ValidatorsLeafIndex))
		})
	}
}

// TestBeaconState_SetSlot_HookErrorLeavesStateUnchanged pins that an
// OnEpochBoundary failure rolls back the slot write instead of leaving b.slot
// updated with SlotLeafIndex undirtied, which would desync the cached hash
// from the actual slot value.
func TestBeaconState_SetSlot_HookErrorLeavesStateUnchanged(t *testing.T) {
	state := GetTestState()
	_, err := state.HashSSZ()
	require.NoError(t, err)
	require.False(t, state.isLeafDirty(SlotLeafIndex))

	oldSlot := state.slot
	epochBoundarySlot := (oldSlot/state.beaconConfig.SlotsPerEpoch + 1) * state.beaconConfig.SlotsPerEpoch

	hookErr := errors.New("hook failed")
	state.SetEvents(Events{OnEpochBoundary: func(uint64) error { return hookErr }})

	require.ErrorIs(t, state.SetSlot(epochBoundarySlot), hookErr)
	assert.Equal(t, oldSlot, state.slot)
	assert.False(t, state.isLeafDirty(SlotLeafIndex))
}

// TestBeaconState_ResetEpochParticipation_HookErrorLeavesStateUnchanged pins
// that an OnResetParticipation failure leaves both participation lists as
// they were, rather than aliasing previousEpochParticipation to
// currentEpochParticipation without completing the reset.
func TestBeaconState_ResetEpochParticipation_HookErrorLeavesStateUnchanged(t *testing.T) {
	state := GetTestState()
	_, err := state.HashSSZ()
	require.NoError(t, err)
	require.False(t, state.isLeafDirty(CurrentEpochParticipationLeafIndex))
	require.False(t, state.isLeafDirty(PreviousEpochParticipationLeafIndex))

	oldCurrent := state.currentEpochParticipation
	oldPrevious := state.previousEpochParticipation

	hookErr := errors.New("hook failed")
	state.SetEvents(Events{OnResetParticipation: func(*solid.ParticipationBitList) error { return hookErr }})

	require.ErrorIs(t, state.ResetEpochParticipation(), hookErr)
	assert.Same(t, oldCurrent, state.currentEpochParticipation)
	assert.Same(t, oldPrevious, state.previousEpochParticipation)
	assert.False(t, state.isLeafDirty(CurrentEpochParticipationLeafIndex))
	assert.False(t, state.isLeafDirty(PreviousEpochParticipationLeafIndex))
}
