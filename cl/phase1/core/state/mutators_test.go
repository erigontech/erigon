package state_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

const (
	testExitEpoch = 53
)

func getTestStateBalances(t *testing.T) *state2.CachingBeaconState {
	numVals := uint64(2048)
	b := state2.New(&clparams.MainnetBeaconConfig)
	for i := uint64(0); i < numVals; i++ {
		v := solid.NewValidator()
		v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
		b.AddValidator(v, i)
	}
	return b
}

func TestIncreaseBalance(t *testing.T) {
	s := getTestStateBalances(t)
	testInd := uint64(42)
	amount := uint64(100)
	beforeBalance, _ := s.ValidatorBalance(int(testInd))
	state2.IncreaseBalance(s, testInd, amount)
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
			require.NoError(t, state2.DecreaseBalance(s, testInd, tc.delta))
			afterBalance, _ := s.ValidatorBalance(int(testInd))
			require.Equal(t, afterBalance, tc.expectedBalance)
		})
	}
}

func TestInitiatieValidatorExit(t *testing.T) {

	v1 := solid.NewValidator()
	v1.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	v1.SetActivationEpoch(0)
	v2 := solid.NewValidator()
	v2.SetExitEpoch(testExitEpoch)
	v2.SetWithdrawableEpoch(testExitEpoch + clparams.MainnetBeaconConfig.MinValidatorWithdrawabilityDelay)
	v2.SetActivationEpoch(0)
	testCases := []struct {
		description                string
		numValidators              uint64
		expectedExitEpoch          uint64
		expectedWithdrawlableEpoch uint64
		validator                  solid.Validator
	}{
		{
			description:                "success",
			numValidators:              3,
			expectedExitEpoch:          5,
			expectedWithdrawlableEpoch: 0,
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
			state := getTestStateBalances(t)
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
