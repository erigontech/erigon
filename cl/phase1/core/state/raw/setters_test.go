package raw

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/assert"
)

func TestBeaconState_SetVersion(t *testing.T) {
	state := GetTestState()
	state.SetVersion(clparams.Phase0Version)
	assert.Equal(t, state.Version(), clparams.Phase0Version)
}

func TestBeaconState_SetSlot(t *testing.T) {
	state := GetTestState()
	slot := uint64(12345)
	state.SetSlot(slot)
	assert.Equal(t, slot, state.slot)
}

func TestBeaconState_SetBlockRootAt(t *testing.T) {
	state := GetTestState()
	index := 0
	root := common.HexToHash("0x1234567890abcdef")
	state.SetBlockRootAt(index, root)
	assert.Equal(t, root, state.blockRoots.Get(index))
}

func TestBeaconState_SetStateRootAt(t *testing.T) {
	state := GetTestState()
	index := 0
	root := common.HexToHash("0xabcdef1234567890")
	state.SetStateRootAt(index, root)
	assert.Equal(t, root, state.stateRoots.Get(index))
}

func TestBeaconState_SetWithdrawalCredentialForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	creds := common.HexToHash("0xabcdef1234567890")
	state.SetWithdrawalCredentialForValidatorAtIndex(index, creds)
	assert.Equal(t, creds, state.validators.Get(index).WithdrawalCredentials())
}

func TestBeaconState_SetExitEpochForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	epoch := uint64(10)
	state.SetExitEpochForValidatorAtIndex(index, epoch)
	assert.Equal(t, epoch, state.validators.Get(index).ExitEpoch())
}

func TestBeaconState_SetWithdrawableEpochForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	epoch := uint64(5)
	err := state.SetWithdrawableEpochForValidatorAtIndex(index, epoch)
	assert.NoError(t, err)
	assert.Equal(t, epoch, state.validators.Get(index).WithdrawableEpoch())
}

func TestBeaconState_SetWithdrawableEpochForValidatorAtIndex_InvalidIndex(t *testing.T) {
	state := GetTestState()
	index := 10000000000000
	epoch := uint64(5)
	err := state.SetWithdrawableEpochForValidatorAtIndex(index, epoch)
	assert.NotNil(t, err)
}

func TestBeaconState_SetEffectiveBalanceForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	balance := uint64(1000)
	state.SetEffectiveBalanceForValidatorAtIndex(index, balance)
	assert.Equal(t, balance, state.validators.Get(index).EffectiveBalance())
}

func TestBeaconState_SetActivationEpochForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	epoch := uint64(5)
	state.SetActivationEpochForValidatorAtIndex(index, epoch)
	assert.Equal(t, epoch, state.validators.Get(index).ActivationEpoch())
}

func TestBeaconState_SetActivationEligibilityEpochForValidatorAtIndex(t *testing.T) {
	state := GetTestState()
	index := 0
	epoch := uint64(10)
	state.SetActivationEligibilityEpochForValidatorAtIndex(index, epoch)
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
	state.AddEth1DataVote(vote1)
	state.AddEth1DataVote(vote2)
	assert.Equal(t, 2, state.Eth1DataVotes().Len())
}

func TestBeaconState_ResetEth1DataVotes(t *testing.T) {
	state := GetTestState()
	vote1 := &cltypes.Eth1Data{
		Root:         common.HexToHash("0xabcdef1234567890"),
		DepositCount: 100,
		BlockHash:    common.HexToHash("0x1234567890abcdef"),
	}
	state.AddEth1DataVote(vote1)
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
	assert.NoError(t, err)
	assert.Equal(t, slashed, state.validators.Get(index).Slashed())
}

func TestBeaconState_SetValidatorSlashed_InvalidIndex(t *testing.T) {
	state := GetTestState()
	index := 10
	slashed := true
	err := state.SetValidatorSlashed(index, slashed)
	assert.Nil(t, err)
}

func TestBeaconState_SetValidatorMinCurrentInclusionDelayAttestation(t *testing.T) {
	state := GetTestState()
	index := 0
	value := solid.NewPendingAttestionFromParameters(nil, solid.NewAttestationData(), 123, 3)

	err := state.SetValidatorMinCurrentInclusionDelayAttestation(index, value)
	assert.NoError(t, err)
	assert.Equal(t, value, state.validators.MinCurrentInclusionDelayAttestation(index))
}

func TestBeaconState_SetValidatorIsCurrentMatchingSourceAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsCurrentMatchingSourceAttester(index, value)
	assert.NoError(t, err)
	assert.Equal(t, value, state.validators.IsCurrentMatchingSourceAttester(index))
}

func TestBeaconState_SetValidatorIsCurrentMatchingTargetAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsCurrentMatchingTargetAttester(index, value)
	assert.NoError(t, err)
	assert.Equal(t, value, state.validators.IsCurrentMatchingTargetAttester(index))
}

func TestBeaconState_SetValidatorIsCurrentMatchingHeadAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsCurrentMatchingHeadAttester(index, value)
	assert.NoError(t, err)
	assert.Equal(t, value, state.validators.IsCurrentMatchingHeadAttester(index))
}

func TestBeaconState_SetValidatorMinPreviousInclusionDelayAttestation(t *testing.T) {
	state := GetTestState()
	index := 0
	value := solid.NewPendingAttestionFromParameters(nil, solid.NewAttestationData(), 123, 3)
	err := state.SetValidatorMinPreviousInclusionDelayAttestation(index, value)
	assert.NoError(t, err)
	assert.Equal(t, value, state.validators.MinPreviousInclusionDelayAttestation(index))
}

func TestBeaconState_SetValidatorIsPreviousMatchingSourceAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsPreviousMatchingSourceAttester(index, value)
	assert.NoError(t, err)
	assert.Equal(t, value, state.validators.IsPreviousMatchingSourceAttester(index))
}

func TestBeaconState_SetValidatorIsPreviousMatchingTargetAttester(t *testing.T) {
	state := GetTestState()
	index := 0
	value := true
	err := state.SetValidatorIsPreviousMatchingTargetAttester(index, value)
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	assert.Equal(t, value, state.validators.IsPreviousMatchingHeadAttester(index))
}

func TestBeaconState_SetValidatorBalance(t *testing.T) {
	state := GetTestState()
	index := 0
	balance := uint64(1000)
	err := state.SetValidatorBalance(index, balance)
	assert.NoError(t, err)
	assert.Equal(t, balance, state.balances.Get(index))
}

func TestBeaconState_AddValidator(t *testing.T) {
	state := GetTestState()
	validator := solid.NewValidator()
	balance := uint64(1000)
	state.AddValidator(validator, balance)
	assert.Equal(t, state.balances.Length(), state.validators.Length())
}

func TestBeaconState_SetRandaoMixAt(t *testing.T) {
	state := GetTestState()
	index := 0
	mix := common.HexToHash("0xabcdef1234567890")
	state.SetRandaoMixAt(index, mix)
	assert.Equal(t, mix, state.randaoMixes.Get(index))
}

func TestBeaconState_SetSlashingSegmentAt(t *testing.T) {
	state := GetTestState()
	index := 0
	segment := uint64(100)
	state.SetSlashingSegmentAt(index, segment)
	assert.Equal(t, segment, state.slashings.Get(index))
}

func TestBeaconState_IncrementSlashingSegmentAt(t *testing.T) {
	state := GetTestState()
	index := 0
	delta := uint64(10)
	state.SetSlashingSegmentAt(index, 100)
	state.IncrementSlashingSegmentAt(index, delta)
	assert.Equal(t, uint64(110), state.slashings.Get(index))
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
