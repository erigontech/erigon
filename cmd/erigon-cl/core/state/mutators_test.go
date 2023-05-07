package state_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/stretchr/testify/require"
)

const (
	testExitEpoch = 53
)

func getTestStateBalances(t *testing.T) *state.BeaconState {
	numVals := uint64(2048)
	b := state.GetEmptyBeaconState()
	for i := uint64(0); i < numVals; i++ {
		v := &cltypes.Validator{}
		v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
		b.AddValidator(v, i)
	}
	return b
}

func getTestStateValidators(t *testing.T, numVals int) *state.BeaconState {
	validators := make([]*cltypes.Validator, numVals)
	for i := 0; i < numVals; i++ {
		v := &cltypes.Validator{}
		v.SetActivationEpoch(0)
		v.SetExitEpoch(testExitEpoch)
		validators[i] = v
	}
	b := state.GetEmptyBeaconState()
	b.SetSlot(testExitEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch)
	b.SetValidators(validators)
	return b
}

func TestIncreaseBalance(t *testing.T) {
	s := getTestStateBalances(t)
	testInd := uint64(42)
	amount := uint64(100)
	beforeBalance, _ := s.ValidatorBalance(int(testInd))
	state.IncreaseBalance(s.BeaconState, testInd, amount)
	afterBalance, _ := s.ValidatorBalance(int(testInd))
	require.Equal(t, afterBalance, beforeBalance+amount)
}

func TestDecreaseBalance(t *testing.T) {
	sampleState := getTestStateBalances(t)
	testInd := uint64(42)
	beforeBalance, _ := sampleState.ValidatorBalance(int(testInd))

	testCases := []struct {
		description     string
		delta           uint64
		expectedBalance uint64
	}{
		{
			description:     "zero_remaining",
			delta:           beforeBalance,
			expectedBalance: 0,
		},
		{
			description:     "non_zero_remaining",
			delta:           1,
			expectedBalance: beforeBalance - 1,
		},
		{
			description:     "underflow",
			delta:           beforeBalance + 1,
			expectedBalance: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			s := getTestStateBalances(t)
			require.NoError(t, state.DecreaseBalance(s.BeaconState, testInd, tc.delta))
			afterBalance, _ := s.ValidatorBalance(int(testInd))
			require.Equal(t, afterBalance, tc.expectedBalance)
		})
	}
}

func TestInitiatieValidatorExit(t *testing.T) {

	v1 := &cltypes.Validator{}
	v1.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	v1.SetActivationEpoch(0)
	v2 := &cltypes.Validator{}
	v2.SetExitEpoch(testExitEpoch)
	v2.SetWithdrawableEpoch(testExitEpoch + clparams.MainnetBeaconConfig.MinValidatorWithdrawabilityDelay)
	v2.SetActivationEpoch(0)
	testCases := []struct {
		description                string
		numValidators              uint64
		expectedExitEpoch          uint64
		expectedWithdrawlableEpoch uint64
		validator                  *cltypes.Validator
	}{
		{
			description:                "success",
			numValidators:              3,
			expectedExitEpoch:          58,
			expectedWithdrawlableEpoch: 314,
			validator:                  v1,
		},
		{
			description:                "exit_epoch_set",
			numValidators:              3,
			expectedExitEpoch:          testExitEpoch,
			expectedWithdrawlableEpoch: testExitEpoch + clparams.MainnetBeaconConfig.MinValidatorWithdrawabilityDelay,
			validator:                  v2,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			state := getTestStateValidators(t, int(tc.numValidators))
			state.AppendValidator(tc.validator)
			testInd := uint64(state.ValidatorLength() - 1)
			state.InitiateValidatorExit(testInd)
			val, err := state.ValidatorForValidatorIndex(int(testInd))
			require.NoError(t, err)
			if val.ExitEpoch() != tc.expectedExitEpoch {
				t.Errorf("unexpected exit epoch: got %d, want %d", val.ExitEpoch(), tc.expectedExitEpoch)
			}
			if val.WithdrawableEpoch() != tc.expectedWithdrawlableEpoch {
				t.Errorf("unexpected withdrawable epoch: got %d, want %d", val.WithdrawableEpoch(), tc.expectedWithdrawlableEpoch)
			}
		})
	}
}

func TestSlashValidator(t *testing.T) {
	slashedInd := 567
	whistleblowerInd := 678

	successState := getTestState(t)

	successBalances := []uint64{}
	for i := 0; i < successState.ValidatorLength(); i++ {
		successBalances = append(successBalances, uint64(i+1))
	}
	successState.SetBalances(successBalances)

	// Set up slashed balance.
	preSlashBalance := uint64(1 << 20)
	successState.SetValidatorBalance(slashedInd, preSlashBalance)
	vali, err := successState.ValidatorForValidatorIndex(slashedInd)
	require.NoError(t, err)
	successState.SetValidatorAtIndex(slashedInd, vali)
	vali.SetEffectiveBalance(preSlashBalance)

	testCases := []struct {
		description string
		state       *state.BeaconState
		wantErr     bool
	}{
		{
			description: "success",
			state:       successState,
			wantErr:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			w := uint64(whistleblowerInd)
			err := tc.state.SlashValidator(uint64(slashedInd), &w)
			if tc.wantErr {
				if err == nil {
					t.Errorf("unexpected success, wantErr is true")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error, wanted success: %v", err)
			}
			vali, err := tc.state.ValidatorForValidatorIndex(slashedInd)
			require.NoError(t, err)
			// Check that the validator is slashed.
			if !vali.Slashed() {
				t.Errorf("slashed index validator not set as slashed")
			}
		})
	}
}
